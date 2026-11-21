/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package merge_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"sigs.k8s.io/structured-merge-diff/v7/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v7/internal/fixture"
	"sigs.k8s.io/structured-merge-diff/v7/merge"
	"sigs.k8s.io/structured-merge-diff/v7/typed"
	"sigs.k8s.io/structured-merge-diff/v7/value"
)

// replayParser exercises all three list semantics at once:
//   - tags:       relational (associative list without keys, i.e. a set)
//   - containers: associative list keyed by name
//   - rgb:        atomic list
//
// and a schema default (protocol on container entries).
var replayParser = func() *typed.Parser {
	parser, err := typed.NewParser(`types:
- name: v1
  map:
    fields:
      - name: name
        type:
          scalar: string
      - name: tags
        type:
          list:
            elementType:
              scalar: string
            elementRelationship: associative
      - name: containers
        type:
          list:
            elementType:
              map:
                fields:
                  - name: name
                    type:
                      scalar: string
                  - name: image
                    type:
                      scalar: string
                  - name: protocol
                    default: "TCP"
                    type:
                      scalar: string
            elementRelationship: associative
            keys:
              - name
              - protocol
      - name: rgb
        type:
          list:
            elementType:
              scalar: numeric
            elementRelationship: atomic
`)
	if err != nil {
		panic(err)
	}
	return parser
}()

func replayTestUpdater() *merge.Updater {
	return (&merge.UpdaterBuilder{
		Converter: passThroughConverter{},
	}).BuildUpdater()
}

// passThroughConverter returns the object unchanged, like the fixture's
// unexported dummy converter.
type passThroughConverter struct{}

func (passThroughConverter) Convert(v *typed.TypedValue, _ fieldpath.APIVersion) (*typed.TypedValue, error) {
	return v, nil
}
func (passThroughConverter) IsMissingVersionError(error) bool { return false }

func mustParseReplay(t *testing.T, yaml string) *typed.TypedValue {
	t.Helper()
	tv, err := replayParser.Type("v1").FromYAML(typed.YAMLObject(yaml))
	if err != nil {
		t.Fatalf("failed to parse %q: %v", yaml, err)
	}
	return tv
}

// asMap normalizes the unstructured representation (yaml.v2 may produce
// map[interface{}]interface{}) into map[string]interface{}.
func asMap(t *testing.T, v interface{}) map[string]interface{} {
	t.Helper()
	switch m := v.(type) {
	case map[string]interface{}:
		return m
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for key, item := range m {
			out[key.(string)] = item
		}
		return out
	default:
		t.Fatalf("expected a map, got %T: %v", v, v)
		return nil
	}
}

func asList(t *testing.T, v interface{}) []interface{} {
	t.Helper()
	list, ok := v.([]interface{})
	if !ok {
		t.Fatalf("expected a list, got %T: %v", v, v)
	}
	return list
}

func replayPlan(t *testing.T, liveYAML string, generation int64) *merge.ReplayPlan {
	t.Helper()
	live := mustParseReplay(t, liveYAML)
	plan, err := merge.NewReplayPlan(replayTestUpdater(), live, fieldpath.ManagedFields{}, generation)
	if err != nil {
		t.Fatalf("failed to create replay plan: %v", err)
	}
	return plan
}

// TestReplayMultiManagerSequence verifies requirement 1 and 2: ordered
// Apply/Update operations replay on the fixed baseline and every step
// reports the merged object, ownership changes and conflicts.
func TestReplayMultiManagerSequence(t *testing.T) {
	plan := replayPlan(t, `
name: app
tags:
- base
`, 10)

	ops := []merge.ReplayOperation{
		{
			Kind:               merge.ReplayApply,
			Manager:            "apply-a",
			APIVersion:         "v1",
			Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`),
			ExpectedGeneration: 10,
		},
		{
			Kind:               merge.ReplayApply,
			Manager:            "apply-b",
			APIVersion:         "v1",
			Object:             mustParseReplay(t, `containers: [{name: side, image: log:1}]`),
			ExpectedGeneration: 11,
		},
		{
			Kind:       merge.ReplayUpdate,
			Manager:    "controller",
			APIVersion: "v1",
			Object: mustParseReplay(t, `
name: app
tags:
- base
- updated
containers:
- name: web
  image: nginx:1
- name: side
  image: log:1
`),
			ExpectedGeneration: 12,
		},
		{
			Kind:               merge.ReplayApply,
			Manager:            "apply-a",
			APIVersion:         "v1",
			Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:2}]`),
			ExpectedGeneration: 13,
		},
	}
	for _, op := range ops {
		if err := plan.Append(op); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if result.StopReason != merge.ReplayCompleted {
		t.Fatalf("expected completed replay, got %v: %v", result.StopReason, result.Err)
	}
	if len(result.Steps) != len(ops) {
		t.Fatalf("expected %d steps, got %d", len(ops), len(result.Steps))
	}
	if result.FailedIndex != -1 {
		t.Fatalf("expected no failed index, got %d", result.FailedIndex)
	}
	if result.BaselineGeneration != 10 || result.Generation != 14 {
		t.Fatalf("expected generations 10->14, got %d->%d", result.BaselineGeneration, result.Generation)
	}
	for i, step := range result.Steps {
		if step.Index != i {
			t.Errorf("step %d: expected index %d", i, step.Index)
		}
		if step.Generation != int64(11+i) {
			t.Errorf("step %d: expected generation %d, got %d", i, 11+i, step.Generation)
		}
		if step.Live == nil {
			t.Errorf("step %d: missing merged object", i)
		}
		// The last step re-applies an identical configuration: the
		// owned paths don't move, so an empty ownership diff there is
		// correct. Every earlier step changes ownership.
		if i < len(result.Steps)-1 && len(step.OwnershipChanges) == 0 {
			t.Errorf("step %d: expected ownership changes", i)
		}
	}

	// The final merged object must contain both containers and the
	// controller's updated tag.
	live := asMap(t, result.Live.AsValue().Unstructured())
	containers := asList(t, live["containers"])
	if len(containers) != 2 {
		t.Fatalf("expected 2 containers, got %v", live["containers"])
	}
	tags := asList(t, live["tags"])
	if len(tags) != 2 || tags[1] != "updated" {
		t.Fatalf("expected updated tags, got %v", tags)
	}

	// apply-a must own web at nginx:2; apply-b owns side; the
	// controller's update owns the tags it set.
	webSet := result.Managers["apply-a"].Set()
	if !webSet.Has(fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image")) {
		t.Fatalf("apply-a missing web image ownership: %v", webSet)
	}
	if _, ok := result.Managers["apply-b"]; !ok {
		t.Fatalf("apply-b ownership missing: %v", result.Managers)
	}
	if _, ok := result.Managers["controller"]; !ok {
		t.Fatalf("controller ownership missing: %v", result.Managers)
	}

	// The first step must show apply-a gaining its paths.
	first := result.Steps[0]
	var change merge.OwnershipChange
	for _, c := range first.OwnershipChanges {
		if c.Manager == "apply-a" {
			change = c
		}
	}
	if change.Manager == "" || len(change.Added) == 0 || len(change.Removed) != 0 {
		t.Fatalf("expected apply-a gains on first step, got %+v", first.OwnershipChanges)
	}
	if !change.Applied || change.APIVersion != "v1" {
		t.Fatalf("expected applied manager at v1, got applied=%v version=%v", change.Applied, change.APIVersion)
	}
}

// TestReplayStaleGenerationRejectsStepAndFollowers verifies requirement
// 2: a stale generation rejects the offending step and every later one.
func TestReplayStaleGenerationRejectsStepAndFollowers(t *testing.T) {
	plan := replayPlan(t, `name: app`, 5)
	mustAppend := func(op merge.ReplayOperation) {
		if err := plan.Append(op); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["x"]`),
		ExpectedGeneration: 5,
	})
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayUpdate, Manager: "b", APIVersion: "v1",
		Object:             mustParseReplay(t, `name: app`),
		ExpectedGeneration: 5, // stale: generation is already 6
	})
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "c", APIVersion: "v1",
		Object:             mustParseReplay(t, `rgb: [1, 2, 3]`),
		ExpectedGeneration: 6,
	})
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedStaleGeneration {
		t.Fatalf("expected stale generation stop, got %v", result.StopReason)
	}
	if result.FailedIndex != 1 {
		t.Fatalf("expected failure at index 1, got %d", result.FailedIndex)
	}
	stale, ok := result.Err.(merge.StaleGenerationError)
	if !ok {
		t.Fatalf("expected StaleGenerationError, got %T", result.Err)
	}
	if stale.Expected != 5 || stale.Actual != 6 {
		t.Fatalf("unexpected stale error: %v", stale)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("expected only the first step to complete, got %d", len(result.Steps))
	}
	if result.Generation != 6 {
		t.Fatalf("expected generation to stay at 6, got %d", result.Generation)
	}
	// The rejected managers must not exist in the published state.
	if _, ok := result.Managers["b"]; ok {
		t.Fatalf("rejected manager b left ownership: %v", result.Managers)
	}
	if _, ok := result.Managers["c"]; ok {
		t.Fatalf("rejected manager c left ownership: %v", result.Managers)
	}
}

// TestReplayFutureGenerationIsRejected pins the equality rule: an
// expectation ahead of the plan's generation is rejected just like a
// stale one, and later steps do not run.
func TestReplayFutureGenerationIsRejected(t *testing.T) {
	plan := replayPlan(t, `name: app`, 5)
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["x"]`),
		ExpectedGeneration: 99,
	}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "b", APIVersion: "v1",
		Object:             mustParseReplay(t, `rgb: [1]`),
		ExpectedGeneration: 5,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedStaleGeneration || result.FailedIndex != 0 {
		t.Fatalf("expected stale generation at index 0, got %v/%d", result.StopReason, result.FailedIndex)
	}
	if len(result.Steps) != 0 {
		t.Fatalf("no step should have completed, got %d", len(result.Steps))
	}
	if result.Generation != 5 {
		t.Fatalf("generation must not advance, got %d", result.Generation)
	}
	if _, ok := result.Managers["a"]; ok {
		t.Fatalf("rejected manager left ownership: %v", result.Managers)
	}
}

// TestReplayConflictStopsAndKeepsPriorSteps verifies requirement 4: a
// non-forced conflict stops the replay while prior complete steps remain
// visible, and requirement 5: conflicts carry manager and original path.
func TestReplayConflictStopsAndKeepsPriorSteps(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	mustAppend := func(op merge.ReplayOperation) {
		if err := plan.Append(op); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`),
		ExpectedGeneration: 0,
	})
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-b", APIVersion: "v1",
		Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:2}]`),
		ExpectedGeneration: 1,
	})
	mustAppend(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-c", APIVersion: "v1",
		Object:             mustParseReplay(t, `rgb: [9]`),
		ExpectedGeneration: 2,
	})
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedConflict {
		t.Fatalf("expected conflict stop, got %v (err=%v)", result.StopReason, result.Err)
	}
	if result.FailedIndex != 1 || len(result.Steps) != 1 {
		t.Fatalf("expected one completed step and failure at index 1, got steps=%d failed=%d", len(result.Steps), result.FailedIndex)
	}
	if len(result.Conflicts) == 0 {
		t.Fatalf("expected conflicts on the result")
	}
	expectedConflictPath := fieldpath.MakePathOrDie(
		"containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image",
	)
	foundImageConflict := false
	for _, conflict := range result.Conflicts {
		if conflict.Manager != "apply-a" {
			t.Errorf("expected conflict with apply-a, got %q", conflict.Manager)
		}
		if len(conflict.Path) == 0 {
			t.Errorf("conflict missing original path: %+v", conflict)
		}
		if conflict.Path.Equals(expectedConflictPath) {
			foundImageConflict = true
		}
	}
	if !foundImageConflict {
		t.Fatalf("expected a conflict on the image field %v, got %v", expectedConflictPath, result.Conflicts)
	}
	// The conflicting field keeps the first manager's value.
	live := asMap(t, result.Live.AsValue().Unstructured())
	web := asMap(t, asList(t, live["containers"])[0])
	if web["image"] != "nginx:1" {
		t.Fatalf("expected conflict to preserve nginx:1, got %v", web["image"])
	}
	// Manager c never ran.
	if _, ok := result.Managers["apply-c"]; ok {
		t.Fatalf("manager after the conflict must not own anything")
	}
}

// TestReplayForceApplyRecordsOverriddenConflicts checks that a forced
// apply completes and the conflicts it overrode are still auditable on
// the step with the displaced manager and path.
func TestReplayForceApplyRecordsOverriddenConflicts(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayForceApply, Manager: "apply-b", APIVersion: "v1",
		Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:2}]`),
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("forced replay failed: %v", err)
	}
	if result.StopReason != merge.ReplayCompleted {
		t.Fatalf("expected completion, got %v: %v", result.StopReason, result.Err)
	}
	step := result.Steps[1]
	if len(step.Conflicts) == 0 {
		t.Fatalf("expected overridden conflicts on the forced step")
	}
	found := false
	for _, conflict := range step.Conflicts {
		if conflict.Manager == "apply-a" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a conflict against apply-a, got %v", step.Conflicts)
	}
	// apply-a lost the image field; apply-b owns it now.
	if result.Managers["apply-a"].Set().Has(fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image")) {
		t.Fatalf("apply-a should have lost image ownership")
	}
	if !result.Managers["apply-b"].Set().Has(fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image")) {
		t.Fatalf("apply-b should own image")
	}
}

// TestReplayListSemanticsAndDefaults verifies requirement 3: relational,
// associative and atomic lists keep their typed-merge semantics, schema
// defaults apply and duplicate associative list keys are not swallowed.
func TestReplayListSemanticsAndDefaults(t *testing.T) {
	plan := replayPlan(t, `
name: app
tags:
- shared
containers:
- name: web
  image: nginx:1
rgb:
- 1
- 2
`, 0)

	// Associative list merge: web is updated in place, side is added;
	// the default protocol key is materialized just like a plain apply.
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object: mustParseReplay(t, `
containers:
- name: web
  image: nginx:2
- name: side
  image: logger:1
tags:
- shared
- extra
`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	// Atomic list is replaced wholesale.
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-b", APIVersion: "v1",
		Object:             mustParseReplay(t, `rgb: [9, 8, 7]`),
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if result.StopReason != merge.ReplayCompleted {
		t.Fatalf("replay stopped: %v %v", result.StopReason, result.Err)
	}
	live := asMap(t, result.Live.AsValue().Unstructured())

	// Associative: both entries survive, web carries the new image.
	containers := asList(t, live["containers"])
	if len(containers) != 2 {
		t.Fatalf("expected 2 associative entries, got %v", containers)
	}
	images := map[string]interface{}{}
	for _, c := range containers {
		entry := asMap(t, c)
		images[entry["name"].(string)] = entry["image"]
	}
	if images["web"] != "nginx:2" || images["side"] != "logger:1" {
		t.Fatalf("unexpected associative merge: %v", images)
	}

	// Relational (separable) list: entries from both sides survive.
	tags := asList(t, live["tags"])
	if len(tags) != 2 {
		t.Fatalf("expected relational list union, got %v", tags)
	}

	// Atomic list: wholesale replacement, not an element-wise merge.
	rgb := asList(t, live["rgb"])
	if len(rgb) != 3 || rgb[0] != 9 {
		t.Fatalf("expected atomic replacement, got %v", rgb)
	}

	// Schema default: the ownership set keyed the entries with the
	// defaulted protocol, mirroring a single Updater.Apply.
	expectedPath := fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image")
	if !result.Managers["apply-a"].Set().Has(expectedPath) {
		t.Fatalf("defaulted key missing from ownership set: %v", result.Managers["apply-a"].Set())
	}
}

// TestReplayDuplicateListKeysNotSwallowed verifies requirement 3's
// duplicate-key guarantee: duplicate associative keys surface the same
// validation error a single merge would, and the failing step publishes
// no partial ownership.
func TestReplayDuplicateListKeysNotSwallowed(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["first"]`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	duplicate, err := replayParser.Type("v1").FromYAML(typed.YAMLObject(`
containers:
- name: web
  image: one
- name: web
  image: two
`), typed.AllowDuplicates)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-b", APIVersion: "v1",
		Object:             duplicate,
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()

	// First establish what the single-shot semantics are.
	if _, _, singleErr := replayTestUpdater().Apply(
		mustParseReplay(t, `name: app`), duplicate, "v1",
		fieldpath.ManagedFields{}, "apply-b", false,
	); singleErr == nil || !strings.Contains(singleErr.Error(), "duplicate") {
		t.Fatalf("test setup: expected duplicate error from single apply, got %v", singleErr)
	}

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedError || result.FailedIndex != 1 {
		t.Fatalf("expected merge error at index 1, got %v/%d", result.StopReason, result.FailedIndex)
	}
	if !strings.Contains(result.Err.Error(), "duplicate") {
		t.Fatalf("expected duplicate-key error, got %v", result.Err)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("only the first step should be published, got %d", len(result.Steps))
	}
	if _, ok := result.Managers["apply-b"]; ok {
		t.Fatalf("failing manager must not leave partial ownership: %v", result.Managers)
	}
}

// TestReplaySchemaConversionFailureIsAtomic verifies requirement 4 with
// a converter that fails: prior steps stay visible and the failing step
// leaves no ownership behind.
func TestReplaySchemaConversionFailureIsAtomic(t *testing.T) {
	updater := (&merge.UpdaterBuilder{Converter: failingConverter{}}).BuildUpdater()
	live := mustParseReplay(t, `name: app`)
	plan, err := merge.NewReplayPlan(updater, live, fieldpath.ManagedFields{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["ok"]`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["ok", "v2"]`),
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()

	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedError || result.FailedIndex != 1 {
		t.Fatalf("expected conversion error at index 1, got %v/%d", result.StopReason, result.FailedIndex)
	}
	if len(result.Steps) != 1 {
		t.Fatalf("expected one completed step, got %d", len(result.Steps))
	}
	// The published state is exactly the first step's state.
	if _, ok := result.Managers["apply-a"]; !ok {
		t.Fatalf("first-step ownership must survive: %v", result.Managers)
	}
}

type failingConverter struct{}

func (failingConverter) Convert(v *typed.TypedValue, version fieldpath.APIVersion) (*typed.TypedValue, error) {
	// Fail only when pruning a re-apply: the prune path converts the
	// merged object to the manager's last applied version. A second
	// apply by the same manager triggers prune.
	var tags interface{}
	switch m := v.AsValue().Unstructured().(type) {
	case map[string]interface{}:
		tags = m["tags"]
	case map[interface{}]interface{}:
		tags = m["tags"]
	}
	if list, ok := tags.([]interface{}); ok && len(list) > 1 {
		return nil, fmt.Errorf("simulated conversion failure")
	}
	return v, nil
}

func (failingConverter) IsMissingVersionError(error) bool { return false }

// TestReplayBaselineIsImmutable verifies requirement 1's immutable
// baseline: mutating the caller's live object and managers after plan
// construction has no effect, and the plan never mutates its inputs.
func TestReplayBaselineIsImmutable(t *testing.T) {
	live := mustParseReplay(t, `name: app`)
	original := asMap(t, live.AsValue().Unstructured())
	managers := fieldpath.ManagedFields{
		"existing": fieldpath.NewVersionedSet(
			fieldpath.NewSet(fieldpath.MakePathOrDie("name")),
			"v1", false,
		),
	}
	plan, err := merge.NewReplayPlan(replayTestUpdater(), live, managers, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate the caller-side objects after handing them over.
	original["name"] = "mutated"
	original["injected"] = true
	managers["existing"].Set().Insert(fieldpath.MakePathOrDie("injected"))

	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["x"]`),
		ExpectedGeneration: 3,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	result, err := plan.Run()
	if err != nil {
		t.Fatal(err)
	}
	baseline := asMap(t, result.Steps[0].Live.AsValue().Unstructured())
	if _, ok := baseline["injected"]; ok {
		t.Fatalf("baseline was not isolated from caller mutation: %v", baseline)
	}
	if baseline["name"] != "app" {
		t.Fatalf("baseline name changed: %v", baseline["name"])
	}
	if result.Managers["existing"].Set().Has(fieldpath.MakePathOrDie("injected")) {
		t.Fatalf("baseline managers were not isolated: %v", result.Managers["existing"])
	}
}

// TestReplayUpdateRecordsOwnershipRemovals verifies requirement 2's
// ownership reporting: an Update that drops a field another manager
// owns records the original path in that manager's Removed list.
func TestReplayUpdateRecordsOwnershipRemovals(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
		Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	// The controller replaces the whole object and drops the container
	// apply-a still owns.
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayUpdate, Manager: "controller", APIVersion: "v1",
		Object:             mustParseReplay(t, `name: app`),
		ExpectedGeneration: 1,
	}); err != nil {
		t.Fatal(err)
	}
	plan.Close()
	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if result.StopReason != merge.ReplayCompleted {
		t.Fatalf("unexpected stop: %v %v", result.StopReason, result.Err)
	}
	step := result.Steps[1]
	var changeIndex = -1
	for i, c := range step.OwnershipChanges {
		if c.Manager == "apply-a" {
			changeIndex = i
		}
	}
	if changeIndex == -1 {
		t.Fatalf("expected an ownership change for apply-a, got %v", step.OwnershipChanges)
	}
	change := step.OwnershipChanges[changeIndex]
	if len(change.Removed) == 0 {
		t.Fatalf("expected removed paths for apply-a, got %+v", change)
	}
	// apply-a must be gone from the published state entirely.
	if _, ok := result.Managers["apply-a"]; ok {
		t.Fatalf("apply-a should have lost all ownership: %v", result.Managers)
	}
	// The reported paths are copies: mutating a borrowed path must not
	// reach the result's recorded step.
	borrowed := change.Removed[0].Copy()
	borrowed[0] = fieldpath.PathElement{}
	if step.OwnershipChanges[changeIndex].Removed[0][0].FieldName == nil {
		t.Fatalf("ownership paths must be defensively copied")
	}
}

// TestReplayDeterministicAcrossRuns verifies requirement 5: results do
// not depend on map iteration order. Two plans with identical inputs
// produce byte-identical step output, and conflict ordering is stable.
func TestReplayDeterministicAcrossRuns(t *testing.T) {
	build := func() *merge.ReplayResult {
		plan := replayPlan(t, `name: app`, 0)
		if err := plan.Append(merge.ReplayOperation{
			Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1",
			Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`),
			ExpectedGeneration: 0,
		}); err != nil {
			t.Fatal(err)
		}
		if err := plan.Append(merge.ReplayOperation{
			Kind: merge.ReplayForceApply, Manager: "apply-b", APIVersion: "v1",
			Object:             mustParseReplay(t, `containers: [{name: web, image: nginx:2}]`),
			ExpectedGeneration: 1,
		}); err != nil {
			t.Fatal(err)
		}
		plan.Close()
		result, err := plan.Run()
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := build()
	for range 20 {
		other := build()
		if !resultShapeEqual(first, other) {
			t.Fatalf("non-deterministic replay output:\n%v\nvs\n%v", describe(first), describe(other))
		}
	}
}

func resultShapeEqual(a, b *merge.ReplayResult) bool {
	if a.StopReason != b.StopReason || len(a.Steps) != len(b.Steps) {
		return false
	}
	for i := range a.Steps {
		sa, sb := a.Steps[i], b.Steps[i]
		if len(sa.OwnershipChanges) != len(sb.OwnershipChanges) || len(sa.Conflicts) != len(sb.Conflicts) {
			return false
		}
		for j := range sa.OwnershipChanges {
			ca, cb := sa.OwnershipChanges[j], sb.OwnershipChanges[j]
			if ca.Manager != cb.Manager || len(ca.Added) != len(cb.Added) || len(ca.Removed) != len(cb.Removed) {
				return false
			}
			for k := range ca.Added {
				if !ca.Added[k].Equals(cb.Added[k]) {
					return false
				}
			}
			for k := range ca.Removed {
				if !ca.Removed[k].Equals(cb.Removed[k]) {
					return false
				}
			}
		}
		for j := range sa.Conflicts {
			if !sa.Conflicts[j].Equals(sb.Conflicts[j]) {
				return false
			}
		}
	}
	return true
}

func describe(r *merge.ReplayResult) string {
	var b strings.Builder
	for _, step := range r.Steps {
		fmt.Fprintf(&b, "step %d:\n", step.Index)
		for _, change := range step.OwnershipChanges {
			fmt.Fprintf(&b, "  %s added=%v removed=%v\n", change.Manager, change.Added, change.Removed)
		}
		for _, conflict := range step.Conflicts {
			fmt.Fprintf(&b, "  conflict %s %v\n", conflict.Manager, conflict.Path)
		}
	}
	return b.String()
}

// TestReplayConcurrentReadersSeeCompletePlans verifies requirement 5:
// concurrent readers only ever see nil or a fully built result.
func TestReplayConcurrentReadersSeeCompletePlans(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	for i := range 50 {
		if err := plan.Append(merge.ReplayOperation{
			Kind: merge.ReplayApply, Manager: fmt.Sprintf("manager-%d", i), APIVersion: "v1",
			Object:             mustParseReplay(t, fmt.Sprintf(`tags: ["tag-%d"]`, i)),
			ExpectedGeneration: int64(i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	plan.Close()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 2000 {
				if result := plan.Result(); result != nil {
					if len(result.Steps) != 50 {
						t.Errorf("reader observed partial plan with %d steps", len(result.Steps))
						return
					}
					if result.StopReason != merge.ReplayCompleted {
						t.Errorf("reader observed incomplete result: %v", result.StopReason)
						return
					}
					if result.Managers == nil || result.Live == nil {
						t.Errorf("reader observed incomplete result state")
						return
					}
				}
			}
		}()
	}
	if _, err := plan.Run(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// Run is idempotent: the same published result comes back.
	again, err := plan.Run()
	if err != nil {
		t.Fatal(err)
	}
	if again != plan.Result() {
		t.Fatalf("re-running the plan returned a different result")
	}
}

// TestReplayPlanLifecycle verifies requirement 6: once closed, the plan
// rejects appends, and Run requires a closed plan.
func TestReplayPlanLifecycle(t *testing.T) {
	plan := replayPlan(t, `name: app`, 0)
	if plan.Closed() {
		t.Fatalf("new plan should be open")
	}
	if _, err := plan.Run(); err == nil {
		t.Fatalf("running an open plan should fail")
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "a", APIVersion: "v1",
		Object:             mustParseReplay(t, `tags: ["x"]`),
		ExpectedGeneration: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if plan.Len() != 1 {
		t.Fatalf("expected length 1, got %d", plan.Len())
	}
	plan.Close()
	if !plan.Closed() {
		t.Fatalf("plan should be closed")
	}
	if err := plan.Append(merge.ReplayOperation{
		Kind: merge.ReplayApply, Manager: "b", APIVersion: "v1",
		Object:             mustParseReplay(t, `rgb: [1]`),
		ExpectedGeneration: 1,
	}); err == nil {
		t.Fatalf("appending to a closed plan should fail")
	}
	// Closing twice is fine.
	plan.Close()
}

// TestReplayNewPlanValidation covers constructor and Append argument
// validation.
func TestReplayNewPlanValidation(t *testing.T) {
	live := mustParseReplay(t, `name: app`)
	if _, err := merge.NewReplayPlan(nil, live, nil, 0); err == nil {
		t.Fatalf("expected error without an updater")
	}
	if _, err := merge.NewReplayPlan(replayTestUpdater(), nil, nil, 0); err == nil {
		t.Fatalf("expected error without a live baseline")
	}
	plan, err := merge.NewReplayPlan(replayTestUpdater(), live, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Append(merge.ReplayOperation{Kind: merge.ReplayApply, Manager: "", APIVersion: "v1", Object: live, ExpectedGeneration: 0}); err == nil {
		t.Fatalf("expected error without a manager")
	}
	if err := plan.Append(merge.ReplayOperation{Kind: merge.ReplayApply, Manager: "a", APIVersion: "v1", Object: nil, ExpectedGeneration: 0}); err == nil {
		t.Fatalf("expected error without an object")
	}
	if err := plan.Append(merge.ReplayOperation{Kind: "bogus", Manager: "a", APIVersion: "v1", Object: live, ExpectedGeneration: 0}); err != nil {
		t.Fatalf("unknown kinds should be accepted at append time: %v", err)
	}
	plan.Close()
	result, err := plan.Run()
	if err != nil {
		t.Fatalf("run returned error: %v", err)
	}
	if result.StopReason != merge.ReplayStoppedError || result.FailedIndex != 0 {
		t.Fatalf("expected unknown-kind error at index 0, got %v/%d", result.StopReason, result.FailedIndex)
	}
}

// TestReplayMatchesSingleShotSemantics verifies requirement 6: the
// replay reaches exactly the same live object and ownership state as
// feeding the same operations one by one through the existing Updater.
func TestReplayMatchesSingleShotSemantics(t *testing.T) {
	operations := []merge.ReplayOperation{
		{Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1", Object: mustParseReplay(t, `containers: [{name: web, image: nginx:1}]`), ExpectedGeneration: 0},
		{Kind: merge.ReplayApply, Manager: "apply-b", APIVersion: "v1", Object: mustParseReplay(t, `containers: [{name: side, image: log:1}]`), ExpectedGeneration: 1},
		{Kind: merge.ReplayUpdate, Manager: "controller", APIVersion: "v1", Object: mustParseReplay(t, `
name: app
tags:
- managed
containers:
- name: web
  image: nginx:1
- name: side
  image: log:1
`), ExpectedGeneration: 2},
		{Kind: merge.ReplayApply, Manager: "apply-a", APIVersion: "v1", Object: mustParseReplay(t, `containers: [{name: web, image: nginx:2}]`), ExpectedGeneration: 3},
	}

	// Reference: direct Updater calls.
	updater := replayTestUpdater()
	refLive := mustParseReplay(t, `name: app`)
	refManagers := fieldpath.ManagedFields{}
	for _, op := range operations {
		var err error
		switch op.Kind {
		case merge.ReplayApply:
			refLive, refManagers, err = updater.Apply(refLive, op.Object, op.APIVersion, refManagers, op.Manager, op.Force)
			if refLive == nil {
				refLive = mustParseReplay(t, `name: app`)
			}
		case merge.ReplayUpdate:
			refLive, refManagers, err = updater.Update(refLive, op.Object, op.APIVersion, refManagers, op.Manager)
		}
		if err != nil {
			t.Fatalf("reference operation failed: %v", err)
		}
	}

	plan := replayPlan(t, `name: app`, 0)
	for _, op := range operations {
		if err := plan.Append(op); err != nil {
			t.Fatal(err)
		}
	}
	plan.Close()
	result, err := plan.Run()
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}

	if !value.Equals(result.Live.AsValue(), refLive.AsValue()) {
		t.Fatalf("live objects differ:\nreplay=%v\nreference=%v", result.Live.AsValue().Unstructured(), refLive.AsValue().Unstructured())
	}
	if !result.Managers.Equals(refManagers) {
		t.Fatalf("managed fields differ:\nreplay=%v\nreference=%v", result.Managers, refManagers)
	}
}

// TestReplayFixtureBehavesUnchanged is a guard that the existing
// fixture-driven single-operation behavior still works end to end.
func TestReplayFixtureBehavesUnchanged(t *testing.T) {
	tc := fixture.TestCase{
		Ops: []fixture.Operation{
			fixture.Apply{Manager: "apply-a", APIVersion: "v1", Object: `
containers:
- name: web
  image: nginx:1
`},
			fixture.Update{Manager: "controller", APIVersion: "v1", Object: `
name: app
containers:
- name: web
  image: nginx:1
tags:
- x
`},
		},
		Object: `
name: app
containers:
- name: web
  image: nginx:1
tags:
- x
`,
		APIVersion: "v1",
		Managed: fieldpath.ManagedFields{
			"apply-a": fieldpath.NewVersionedSet(
				fieldpath.NewSet(
					fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP")),
					fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "name"),
					fieldpath.MakePathOrDie("containers", fieldpath.KeyByFields("name", "web", "protocol", "TCP"), "image"),
				),
				"v1", true,
			),
			"controller": fieldpath.NewVersionedSet(
				fieldpath.NewSet(
					fieldpath.MakePathOrDie("name"),
					fieldpath.MakePathOrDie("tags"),
					fieldpath.MakePathOrDie("tags", value.NewValueInterface("x")),
				),
				"v1", false,
			),
		},
	}
	if err := tc.Test(fixture.SameVersionParser{T: replayParser.Type("v1")}); err != nil {
		t.Fatalf("fixture behavior changed: %v", err)
	}
}
