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

package merge

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"sigs.k8s.io/structured-merge-diff/v7/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v7/typed"
	"sigs.k8s.io/structured-merge-diff/v7/value"
)

// ReplayOp identifies the kind of an operation in a ReplayPlan.
type ReplayOp string

const (
	// ReplayApply replays a non-forced server-side apply. Conflicts stop
	// the replay and are reported on the ReplayResult.
	ReplayApply ReplayOp = "Apply"
	// ReplayForceApply replays a forced apply, taking ownership away from
	// conflicting managers.
	ReplayForceApply ReplayOp = "ForceApply"
	// ReplayUpdate replays an Update (full object replacement) operation.
	ReplayUpdate ReplayOp = "Update"
)

// ReplayOperation is a single ordered operation in a multi-step merge
// replay. The Object must already be typed against the same schema as the
// plan's live baseline. ExpectedGeneration is the resource generation the
// operation was issued against; a step whose expectation no longer matches
// the plan's generation is rejected together with every step after it.
type ReplayOperation struct {
	// Kind is the operation to replay.
	Kind ReplayOp
	// Manager is the field manager performing the operation.
	Manager string
	// APIVersion is the version the object is encoded in.
	APIVersion fieldpath.APIVersion
	// Object is the fully parsed configuration (apply) or intended
	// object (update).
	Object *typed.TypedValue
	// ExpectedGeneration is the generation of the resource the caller
	// observed when queuing the operation.
	ExpectedGeneration int64
	// Force only applies to apply operations and mirrors the force flag
	// of Updater.Apply. It is implied for ReplayForceApply.
	Force bool
}

// OwnershipChange describes how one field manager's ownership changed
// during a single replay step.
type OwnershipChange struct {
	// Manager is the field manager whose ownership changed.
	Manager string
	// APIVersion is the version the manager's field set is tracked in
	// after the step.
	APIVersion fieldpath.APIVersion
	// Applied is true if the manager is an applier after the step.
	Applied bool
	// Added are the paths the manager gained ownership of during the
	// step, sorted in fieldpath.Set iteration order.
	Added []fieldpath.Path
	// Removed are the paths the manager lost ownership of during the
	// step, sorted in fieldpath.Set iteration order.
	Removed []fieldpath.Path
}

// ReplayStep is the auditable outcome of a single replayed operation.
type ReplayStep struct {
	// Index is the zero-based position of the operation in the plan.
	Index int
	// Operation is the operation that produced this step.
	Operation ReplayOperation
	// Generation is the plan's generation after the step.
	Generation int64
	// Live is the merged live object as it is after the step.
	Live *typed.TypedValue
	// Managers is the full field-ownership state after the step.
	Managers fieldpath.ManagedFields
	// OwnershipChanges describes, per manager, how ownership moved
	// during the step. Entries are sorted by manager name.
	OwnershipChanges []OwnershipChange
	// Conflicts are the conflicts encountered while replaying the
	// step. For a forced apply these are the conflicts that force
	// overrode; for a non-forced apply they are the reason the replay
	// stopped. Each conflict keeps the manager that owned the field and
	// the original conflicting path.
	Conflicts Conflicts
}

// ReplayStopReason classifies why a replay stopped before exhausting its
// operations.
type ReplayStopReason string

const (
	// ReplayCompleted means every operation was replayed successfully.
	ReplayCompleted ReplayStopReason = "Completed"
	// ReplayStoppedConflict means a non-forced apply hit conflicts.
	ReplayStoppedConflict ReplayStopReason = "Conflict"
	// ReplayStoppedStaleGeneration means an operation expected an
	// outdated resource generation.
	ReplayStoppedStaleGeneration ReplayStopReason = "StaleGeneration"
	// ReplayStoppedError means a schema conversion or merge failed.
	ReplayStoppedError ReplayStopReason = "Error"
)

// ReplayResult is an immutable snapshot of a replay run. Concurrent
// readers either observe the zero value (no replay has run yet) or a
// fully built result; a partially executed replay is never published.
type ReplayResult struct {
	// BaselineGeneration is the generation the plan started from.
	BaselineGeneration int64
	// Generation is the generation reached after Steps.
	Generation int64
	// Live is the live object reached after the last completed step;
	// it is the untouched baseline when no step completed.
	Live *typed.TypedValue
	// Managers is the field-ownership state reached after the last
	// completed step.
	Managers fieldpath.ManagedFields
	// Steps contains every fully completed step, in plan order. A step
	// that failed validation, conversion or merging is never present,
	// so no partial ownership is ever observable.
	Steps []ReplayStep
	// StopReason says why the replay ended.
	StopReason ReplayStopReason
	// FailedIndex is the index of the operation that stopped the
	// replay, or -1 if every operation completed.
	FailedIndex int
	// Conflicts holds the conflicts reported by the stopping operation
	// when StopReason is ReplayStoppedConflict.
	Conflicts Conflicts
	// Err holds the error reported by the stopping operation when
	// StopReason is ReplayStoppedError.
	Err error
}

// StaleGenerationError reports that a replay operation was issued
// against a generation that no longer matches the plan's generation.
// It implements the error interface.
type StaleGenerationError struct {
	// Index is the zero-based position of the rejected operation.
	Index int
	// Expected is the generation the operation was issued against.
	Expected int64
	// Actual is the plan's generation at the time of the rejection.
	Actual int64
}

var _ error = StaleGenerationError{}

// Error formats the stale-generation rejection as an error.
func (e StaleGenerationError) Error() string {
	return fmt.Sprintf("operation %d expected generation %d but the resource is at generation %d", e.Index, e.Expected, e.Actual)
}

// ReplayPlan replays an ordered list of Apply/Update operations on top
// of a fixed, immutable live baseline. Operations are appended while the
// plan is open; Close seals the plan, after which no further operations
// can be added. Run performs the replay on private copies of the state
// and publishes an immutable ReplayResult, so concurrent readers only
// ever observe the previous complete result or the new one.
//
// A ReplayPlan is safe for concurrent use: appends and Close are
// serialized with reads and with Run.
type ReplayPlan struct {
	updater *Updater

	mu         sync.RWMutex
	closed     bool
	ran        bool
	baseline   *typed.TypedValue
	managers   fieldpath.ManagedFields
	generation int64
	operations []ReplayOperation
	result     *ReplayResult
}

// NewReplayPlan creates an open replay plan on top of the given live
// baseline and field-ownership state. The baseline and ownership state
// are defensively copied, so later mutation by the caller cannot affect
// the plan. generation is the resource generation the baseline was read
// at; every successfully replayed operation advances it by one.
func NewReplayPlan(updater *Updater, live *typed.TypedValue, managers fieldpath.ManagedFields, generation int64) (*ReplayPlan, error) {
	if updater == nil {
		return nil, fmt.Errorf("a replay plan requires an Updater")
	}
	if live == nil {
		return nil, fmt.Errorf("a replay plan requires a live baseline object")
	}
	baseline, err := cloneTypedValue(live)
	if err != nil {
		return nil, fmt.Errorf("failed to snapshot live baseline: %v", err)
	}
	return &ReplayPlan{
		updater:    updater,
		baseline:   baseline,
		managers:   cloneManagedFields(managers),
		generation: generation,
	}, nil
}

// Append queues an operation. It returns an error once the plan has been
// closed; queued operations are only executed by Run, so an invalid
// operation never leaves partial state behind.
func (p *ReplayPlan) Append(op ReplayOperation) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("cannot append to a closed replay plan")
	}
	if op.Manager == "" {
		return fmt.Errorf("replay operation %d requires a field manager", len(p.operations))
	}
	if op.Object == nil {
		return fmt.Errorf("replay operation %d requires an object", len(p.operations))
	}
	p.operations = append(p.operations, op)
	return nil
}

// Close seals the plan. Appending to a closed plan fails. Close is
// idempotent.
func (p *ReplayPlan) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
}

// Closed reports whether the plan has been sealed.
func (p *ReplayPlan) Closed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// Len returns the number of queued operations.
func (p *ReplayPlan) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.operations)
}

// Result returns the published replay result, or nil if Run has not
// completed yet. It is safe to call concurrently with Run.
func (p *ReplayPlan) Result() *ReplayResult {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.result
}

// Run replays the queued operations in order on private copies of the
// immutable baseline. The plan must be closed first. The replay stops at
// the first operation whose generation expectation is stale, that hits
// non-forced conflicts, or whose schema conversion/merge fails; the
// returned result contains only the fully completed steps before it and
// no partial ownership. The built result is published atomically and
// returned on every subsequent call.
func (p *ReplayPlan) Run() (*ReplayResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		return nil, fmt.Errorf("cannot run an open replay plan; close it first")
	}
	if p.ran {
		return p.result, nil
	}

	result := &ReplayResult{
		BaselineGeneration: p.generation,
		Generation:         p.generation,
		Live:               p.baseline,
		Managers:           cloneManagedFields(p.managers),
		FailedIndex:        -1,
		StopReason:         ReplayCompleted,
		Steps:              []ReplayStep{},
	}

	live := p.baseline
	managers := cloneManagedFields(p.managers)
	generation := p.generation

	for i, op := range p.operations {
		if op.ExpectedGeneration != generation {
			result.StopReason = ReplayStoppedStaleGeneration
			result.FailedIndex = i
			result.Err = StaleGenerationError{
				Index:    i,
				Expected: op.ExpectedGeneration,
				Actual:   generation,
			}
			break
		}

		step, nextLive, nextManagers, err := p.replayStep(i, op, live, managers)
		if err != nil {
			var conflicts Conflicts
			// Keep the step's conflict detail on the result, but do
			// not publish the step itself: it never completed.
			if errors.As(err, &conflicts) {
				result.StopReason = ReplayStoppedConflict
				result.FailedIndex = i
				result.Conflicts = cloneConflicts(conflicts)
			} else {
				result.StopReason = ReplayStoppedError
				result.FailedIndex = i
				result.Err = err
			}
			break
		}

		generation++
		step.Generation = generation
		result.Steps = append(result.Steps, step)
		live = nextLive
		managers = nextManagers
	}

	result.Generation = generation
	result.Live = live
	result.Managers = managers

	p.result = result
	p.ran = true
	return result, nil
}

// replayStep runs a single operation against the given state. It returns
// the auditable step and the next state; on error the caller must discard
// nextLive/nextManagers so that no partial ownership survives.
func (p *ReplayPlan) replayStep(index int, op ReplayOperation, live *typed.TypedValue, managers fieldpath.ManagedFields) (ReplayStep, *typed.TypedValue, fieldpath.ManagedFields, error) {
	workingManagers := cloneManagedFields(managers)

	var (
		nextLive *typed.TypedValue
		err      error
		force    bool
	)
	switch op.Kind {
	case ReplayApply:
		force = op.Force
		nextLive, workingManagers, err = p.updater.Apply(live, op.Object, op.APIVersion, workingManagers, op.Manager, force)
	case ReplayForceApply:
		nextLive, workingManagers, err = p.updater.Apply(live, op.Object, op.APIVersion, workingManagers, op.Manager, true)
		force = true
	case ReplayUpdate:
		nextLive, workingManagers, err = p.updater.Update(live, op.Object, op.APIVersion, workingManagers, op.Manager)
	default:
		return ReplayStep{}, nil, nil, fmt.Errorf("unknown replay operation kind %q at index %d", op.Kind, index)
	}
	if err != nil {
		return ReplayStep{}, nil, nil, err
	}

	// Apply returns nil when the merge was a no-op and ReturnInputOnNoop
	// is off; the live object is unchanged in that case.
	if nextLive == nil {
		nextLive = live
	}
	nextLive, err = cloneTypedValue(nextLive)
	if err != nil {
		return ReplayStep{}, nil, nil, fmt.Errorf("failed to snapshot merged object: %v", err)
	}
	nextManagers := cloneManagedFields(workingManagers)

	step := ReplayStep{
		Index:            index,
		Operation:        op,
		Live:             nextLive,
		Managers:         nextManagers,
		OwnershipChanges: ownershipChanges(managers, nextManagers),
	}
	if force {
		// A forced apply overrides every conflicting manager: the fields
		// it took away from others are exactly the ownership removals
		// recorded for managers other than the acting one.
		step.Conflicts = conflictsFromOwnership(op.Manager, step.OwnershipChanges)
	}
	return step, nextLive, nextManagers, nil
}

// ownershipChanges diffs the ownership before and after a step and
// returns one entry per manager whose set changed, sorted by manager.
func ownershipChanges(before, after fieldpath.ManagedFields) []OwnershipChange {
	names := map[string]struct{}{}
	for manager := range before {
		names[manager] = struct{}{}
	}
	for manager := range after {
		names[manager] = struct{}{}
	}
	changes := make([]OwnershipChange, 0, len(names))
	for manager := range names {
		var beforeSet, afterSet *fieldpath.Set
		if vs, ok := before[manager]; ok {
			beforeSet = vs.Set()
		} else {
			beforeSet = fieldpath.NewSet()
		}
		afterVS, present := after[manager]
		if present {
			afterSet = afterVS.Set()
		} else {
			afterSet = fieldpath.NewSet()
		}
		added := afterSet.Difference(beforeSet)
		removed := beforeSet.Difference(afterSet)
		if added.Empty() && removed.Empty() {
			continue
		}
		change := OwnershipChange{
			Manager: manager,
			Added:   pathsOf(added),
			Removed: pathsOf(removed),
		}
		if present {
			change.APIVersion = afterVS.APIVersion()
			change.Applied = afterVS.Applied()
		}
		changes = append(changes, change)
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Manager < changes[j].Manager })
	return changes
}

// conflictsFromOwnership builds the conflict list a forced apply
// overrode from the ownership removals suffered by other managers.
func conflictsFromOwnership(actingManager string, changes []OwnershipChange) Conflicts {
	conflicts := Conflicts{}
	for _, change := range changes {
		if change.Manager == actingManager {
			continue
		}
		for _, path := range change.Removed {
			conflicts = append(conflicts, Conflict{
				Manager: change.Manager,
				Path:    path.Copy(),
			})
		}
	}
	sortConflicts(conflicts)
	return conflicts
}

// sortConflicts orders conflicts by manager and then path so replay
// output never depends on map iteration order.
func sortConflicts(conflicts Conflicts) {
	sort.Slice(conflicts, func(i, j int) bool {
		if conflicts[i].Manager != conflicts[j].Manager {
			return conflicts[i].Manager < conflicts[j].Manager
		}
		return conflicts[i].Path.String() < conflicts[j].Path.String()
	})
}

// pathsOf returns the paths contained in a set as a slice in the set's
// deterministic (sorted, preorder DFS) iteration order.
func pathsOf(set *fieldpath.Set) []fieldpath.Path {
	paths := []fieldpath.Path{}
	set.Iterate(func(p fieldpath.Path) {
		paths = append(paths, p.Copy())
	})
	return paths
}

// cloneManagedFields returns a deep copy: the map itself and every
// underlying field set are duplicated, so working-state mutation can
// never reach the caller's ownership state.
func cloneManagedFields(managers fieldpath.ManagedFields) fieldpath.ManagedFields {
	clone := fieldpath.ManagedFields{}
	for manager, vs := range managers {
		clone[manager] = fieldpath.NewVersionedSet(vs.Set().Copy(), vs.APIVersion(), vs.Applied())
	}
	return clone
}

// cloneConflicts copies a conflict list, duplicating each path.
func cloneConflicts(conflicts Conflicts) Conflicts {
	clone := make(Conflicts, len(conflicts))
	for i, conflict := range conflicts {
		clone[i] = Conflict{
			Manager: conflict.Manager,
			Path:    conflict.Path.Copy(),
		}
	}
	sortConflicts(clone)
	return clone
}

// cloneTypedValue snapshots a typed value by round-tripping it through
// a deep copy of its unstructured representation and re-validating it
// against the same schema and type. The result shares no mutable state
// with the input, so later caller-side mutation of the baseline cannot
// affect the plan.
func cloneTypedValue(tv *typed.TypedValue) (*typed.TypedValue, error) {
	return typed.AsTyped(
		value.NewValueInterface(deepCopyUnstructured(tv.AsValue().Unstructured())),
		tv.Schema(),
		tv.TypeRef(),
	)
}

// deepCopyUnstructured duplicates the maps and slices of an unstructured
// value. Leaf values (strings, numbers, booleans, nil) are immutable and
// shared.
func deepCopyUnstructured(in interface{}) interface{} {
	switch v := in.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = deepCopyUnstructured(item)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[fmt.Sprintf("%v", key)] = deepCopyUnstructured(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = deepCopyUnstructured(item)
		}
		return out
	default:
		return in
	}
}
