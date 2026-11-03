/*
Copyright The Kubernetes Authors.

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

package limits

import (
	"cmp"
	"testing"
	"time"

	gocmp "github.com/google/go-cmp/cmp"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
)

func TestPreemptionLimitsSnapshotAllowance(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQ := *utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(3).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		limit  policy.PreemptionLimitReference
		events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		// changeTracker, when set, changes the tracker after the snapshot is taken.
		changeTracker func(*PreemptionLimitTracker)
		want          int32
	}{
		"allowance is the limit minus the preemptions within the window": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-2 * time.Minute), now.Add(-30 * time.Second)}},
			},
			want: 2,
		},
		"allowance returns the current limit for the proper scope value": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-b": {now, now}},
			},
			want: 3,
		},
		"limit missing from the snapshot has 0 allowance": {
			limit: "missing",
			want:  0,
		},
		"allowance doesn't change when the tracker forgets the limit after the snapshot is taken": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now, now}},
			},
			changeTracker: func(tracker *PreemptionLimitTracker) { tracker.Forget("per-cq") },
			want:          1,
		},
		"allowance doesn't change when the tracker prunes the limit after the snapshot is taken": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-50 * time.Second), now}},
			},
			changeTracker: func(tracker *PreemptionLimitTracker) {
				// Drops all the events, as a prune with a shrunk window would.
				tracker.Prune(
					now.Add(time.Hour),
					utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).LimitWindowSeconds(1).Obj(),
				)
			},
			want: 1,
		},
		"allowance doesn't change when the tracker records preemptions after the snapshot is taken": {
			changeTracker: func(tracker *PreemptionLimitTracker) {
				tracker.Record(now, policy.ConsumedLimits{"per-cq": "cq-a"})
			},
			want: 3,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			snapshot := tracker.Snapshot(now, []kueuealpha.PreemptionLimit{perCQ})
			if tc.changeTracker != nil {
				tc.changeTracker(tracker)
			}
			limit := cmp.Or(tc.limit, policy.PreemptionLimitReference("per-cq"))
			if got := snapshot.allowance(limit, "cq-a"); got != tc.want {
				t.Errorf("allowance() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestPreemptionLimitsSnapshotRecord(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	global := *utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		Limit(5).LimitWindowSeconds(60).Obj()
	perCQ := *utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(3).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		// recorded are the preemptions recorded in the snapshot after it was taken.
		recorded      []policy.ConsumedLimits
		wantAllowance map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32
	}{
		"preemptions recorded in the snapshot are counted in allowance along with tracker events": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-30 * time.Second)}},
			},
			recorded: []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"per-cq": {"cq-a": 1},
			},
		},
		"preemptions recorded in the snapshot are counted only for their scope value": {
			recorded: []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"per-cq": {"cq-b": 3},
			},
		},
		"multiple consumed limits are all counted in allowance": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {now.Add(-30 * time.Second)}},
			},
			recorded: []policy.ConsumedLimits{
				{"global": "Global", "per-cq": "cq-a"},
				{"global": "Global", "per-cq": "cq-b"},
			},
			wantAllowance: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32{
				"global": {"Global": 2},
				"per-cq": {"cq-a": 2, "cq-b": 2},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			snapshot := tracker.Snapshot(now, []kueuealpha.PreemptionLimit{global, perCQ})
			for _, consumed := range tc.recorded {
				snapshot.Record(now, consumed)
			}
			for limit, scopes := range tc.wantAllowance {
				for scope, want := range scopes {
					if got := snapshot.allowance(limit, scope); got != want {
						t.Errorf("allowance(%q, %q) = %d, want %d", limit, scope, got, want)
					}
				}
			}
		})
	}
}

func TestPreemptionLimitsSnapshotRecordEvents(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	global := *utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		Limit(5).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		seeded events
		ts     time.Time
		want   events
	}{
		"later event is appended": {
			seeded: events{now.Add(-20 * time.Second), now},
			ts:     now.Add(5 * time.Second),
			want:   events{now.Add(-20 * time.Second), now, now.Add(5 * time.Second)},
		},
		"earlier event is inserted in order": {
			seeded: events{now.Add(-20 * time.Second), now},
			ts:     now.Add(-10 * time.Second),
			want:   events{now.Add(-20 * time.Second), now.Add(-10 * time.Second), now},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Spare capacity ensures an uncloned slice would overwrite tracker's backing array.
			seeded := make(events, len(tc.seeded), len(tc.seeded)+2)
			copy(seeded, tc.seeded)
			tracker := NewPreemptionLimitTracker()
			tracker.events = map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": seeded},
			}

			snapshot := tracker.Snapshot(now, []kueuealpha.PreemptionLimit{global})
			snapshot.Record(tc.ts, policy.ConsumedLimits{"global": "Global"})

			wantSnapshot := map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": tc.want},
			}
			if diff := gocmp.Diff(wantSnapshot, snapshot.events); diff != "" {
				t.Errorf("Unexpected snapshot events (-want,+got):\n%s", diff)
			}
			wantTracker := map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": tc.seeded},
			}
			if diff := gocmp.Diff(wantTracker, tracker.events); diff != "" {
				t.Errorf("Unexpected tracker events after snapshot.Record (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitsSnapshotNextSlotTime(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	global := utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		LimitWindowSeconds(60)

	cases := map[string]struct {
		limits []kueuealpha.PreemptionLimit
		events events
		limit  policy.PreemptionLimitReference
		count  int32
		want   *time.Time
	}{
		"single active event expires after window": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(1).Obj()},
			events: events{now.Add(-20 * time.Second)},
			limit:  "global",
			count:  1,
			want:   new(now.Add(40 * time.Second)),
		},
		"multiple needed slots wait for enough events to expire": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(2).Obj()},
			events: events{now.Add(-40 * time.Second), now.Add(-10 * time.Second)},
			limit:  "global",
			count:  2,
			want:   new(now.Add(50 * time.Second)),
		},
		"limit lowered below recorded events waits until excess events expire": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(1).Obj()},
			events: events{now.Add(-50 * time.Second), now.Add(-30 * time.Second), now.Add(-10 * time.Second)},
			limit:  "global",
			count:  1,
			want:   new(now.Add(50 * time.Second)),
		},
		"event after snapshot time is counted in slot time": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(1).Obj()},
			events: events{now.Add(5 * time.Second)},
			limit:  "global",
			count:  1,
			want:   new(now.Add(65 * time.Second)),
		},
		"limit of 0 never frees a slot": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(0).Obj()},
			limit:  "global",
			count:  1,
		},
		"count exceeding limit never frees a slot": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(1).Obj()},
			events: events{now.Add(-20 * time.Second)},
			limit:  "global",
			count:  2,
		},
		"unknown limit returns nil": {
			limits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(1).Obj()},
			limit:  "missing",
			count:  1,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if len(tc.events) > 0 {
				tracker.events = map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
					"global": {"Global": tc.events},
				}
			}
			snapshot := tracker.Snapshot(now, tc.limits)
			if diff := gocmp.Diff(tc.want, snapshot.nextSlotTime(tc.limit, "Global", tc.count)); diff != "" {
				t.Errorf("nextSlotTime() (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitsSnapshotWithinLimits(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQ := *utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(2).LimitWindowSeconds(60).Obj()
	global := *utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		Limit(2).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		// recorded are the preemptions recorded in the snapshot after it was taken.
		recorded         []policy.ConsumedLimits
		nilSnapshot      bool
		preemptions      []policy.ConsumedLimits
		wantWithinLimits bool
	}{
		"preemptions within the remaining limit are allowed": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-30 * time.Second)}},
			},
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			wantWithinLimits: true,
		},
		"preemptions together exceeding the remaining limit are blocked": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-30 * time.Second)}},
			},
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-a"}, {"per-cq": "cq-a"}},
			wantWithinLimits: false,
		},
		"preemptions recorded in the snapshot are counted against the limit": {
			recorded:         []policy.ConsumedLimits{{"per-cq": "cq-a"}, {"per-cq": "cq-a"}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			wantWithinLimits: false,
		},
		"different scope values are counted independently": {
			recorded:         []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-b"}, {"per-cq": "cq-b"}, {"per-cq": "cq-a"}},
			wantWithinLimits: true,
		},
		"preemption is blocked when any of its consumed limits is exhausted": {
			recorded:         []policy.ConsumedLimits{{"global": "Global"}, {"global": "Global"}},
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-a", "global": "Global"}},
			wantWithinLimits: false,
		},
		"preemption consuming a limit missing from the snapshot is blocked": {
			preemptions:      []policy.ConsumedLimits{{"missing": "Global"}},
			wantWithinLimits: false,
		},
		"preemption is blocked when the snapshot is nil": {
			nilSnapshot:      true,
			preemptions:      []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			wantWithinLimits: false,
		},
		"preemption consuming no limits is blocked when the snapshot is nil": {
			nilSnapshot:      true,
			preemptions:      []policy.ConsumedLimits{{}},
			wantWithinLimits: false,
		},
		// True when there are no preemptions to block.
		"zero preemptions are allowed when the snapshot is nil": {
			nilSnapshot:      true,
			wantWithinLimits: true,
		},
		"zero preemptions are allowed": {
			wantWithinLimits: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			snapshot := tracker.Snapshot(now, []kueuealpha.PreemptionLimit{perCQ, global})
			for _, consumed := range tc.recorded {
				snapshot.Record(now, consumed)
			}
			if tc.nilSnapshot {
				snapshot = nil
			}
			if got := snapshot.WithinLimits(tc.preemptions); got != tc.wantWithinLimits {
				t.Errorf("WithinLimits(%v) = %v, want %v", tc.preemptions, got, tc.wantWithinLimits)
			}
		})
	}
}
