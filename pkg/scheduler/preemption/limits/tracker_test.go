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
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestConsumedLimits(t *testing.T) {
	preemptor := &workload.Info{
		Obj:          utiltestingapi.MakeWorkload("preemptor", "ns").Obj(),
		ClusterQueue: "preempting-cq",
	}
	target := &workload.Info{
		Obj:          utiltestingapi.MakeWorkload("target", "ns").Obj(),
		ClusterQueue: "preempted-cq",
	}

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		want             map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue
	}{
		"no limits": {},
		"each scope resolves its scope value": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				*utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempting-cq", kueuealpha.PreemptingCQLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempted-cq", kueuealpha.PreemptedCQLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("preempted-workload", kueuealpha.PreemptedWorkloadLimitScope).Obj(),
			},
			want: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{
				"global":             "Global",
				"preempting-cq":      "preempting-cq",
				"preempted-cq":       "preempted-cq",
				"preempted-workload": "ns/target",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := ConsumedLimits(tc.preemptionLimits, preemptor, target)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected consumed limits (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerRecord(t *testing.T) {
	t1 := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	type record struct {
		ts       time.Time
		consumed map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue
	}
	cases := map[string]struct {
		records []record
		want    map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
	}{
		"nil consumed limits is a no-op": {
			records: []record{{ts: t1}},
		},
		"multiple limits and scope values": {
			records: []record{
				{ts: t1, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{
					"global":       "Global",
					"per-cq":       "cq-a",
					"per-workload": "ns/wl-1",
				}},
				{ts: t2, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{
					"global":       "Global",
					"per-cq":       "cq-b",
					"per-workload": "ns/wl-2",
				}},
				{ts: t3, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{
					"per-cq": "cq-a",
				}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global":       {"Global": {t1, t2}},
				"per-cq":       {"cq-a": {t1, t3}, "cq-b": {t2}},
				"per-workload": {"ns/wl-1": {t1}, "ns/wl-2": {t2}},
			},
		},
		"out-of-order inserts are kept sorted": {
			records: []record{
				{ts: t3, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{"global": "Global"}},
				{ts: t1, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{"global": "Global"}},
				{ts: t2, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{"global": "Global"}},
				{ts: t1, consumed: map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{"global": "Global"}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {t1, t1, t2, t3}},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			for _, r := range tc.records {
				tracker.Record(r.ts, r.consumed)
			}
			if diff := cmp.Diff(tc.want, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected events (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerConcurrent(t *testing.T) {
	const records = 100
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	limit := utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).LimitWindowSeconds(60).Obj()
	consumed := map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{"global": "Global"}
	cutoff := start.Add(records / 2 * time.Second)
	now := cutoff.Add(2 * time.Minute) // window (1m) + grace (1m)

	tracker := NewPreemptionLimitTracker()
	var wg sync.WaitGroup
	for i := range records {
		wg.Go(func() {
			tracker.Record(start.Add(time.Duration(i)*time.Second), consumed)
		})
		wg.Go(func() {
			tracker.Prune(now, limit)
		})
	}
	wg.Wait()

	// The concurrent prunes may run before some expired events are recorded,
	// so prune once more to get a deterministic result.
	tracker.Prune(now, limit)

	var want events
	for i := records / 2; i < records; i++ {
		want = append(want, start.Add(time.Duration(i)*time.Second))
	}
	if diff := cmp.Diff(want, tracker.events["global"]["Global"]); diff != "" {
		t.Errorf("Unexpected events (-want,+got):\n%s", diff)
	}
}

func TestPreemptionLimitTrackerPrune(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	cutoff := now.Add(-2 * time.Minute) // window (1m) + grace (1m)
	expired := cutoff.Add(-time.Nanosecond)
	limit := utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptingCQLimitScope).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		recorded map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		want     map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
	}{
		"drops the events before the cutoff, keeps the one at it": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {cutoff.Add(-time.Hour), expired, cutoff, now}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {cutoff, now}},
			},
		},
		"keeps the events outside the window but within the grace period": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-90 * time.Second)}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-90 * time.Second)}},
			},
		},
		"forgets the scope values left without events": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {expired}, "cq-b": {now}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-b": {now}},
			},
		},
		"forgets the limit left without events": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {expired}, "cq-b": {expired}},
			},
		},
		"leaves the other limits untouched": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {expired, now}},
				"global": {"Global": {expired}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now}},
				"global": {"Global": {expired}},
			},
		},
		"unknown limit is a no-op": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {expired}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {expired}},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := newTrackerWithEvents(tc.recorded)
			tracker.Prune(now, limit)
			if diff := cmp.Diff(tc.want, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected events (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitTrackerForget(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

	cases := map[string]struct {
		recorded map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		want     map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
	}{
		"drops only the given limit": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now}, "cq-b": {now}},
				"global": {"Global": {now}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {now}},
			},
		},
		"unknown limit is a no-op": {
			recorded: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {now}},
			},
			want: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"global": {"Global": {now}},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := newTrackerWithEvents(tc.recorded)
			tracker.Forget("per-cq")
			if diff := cmp.Diff(tc.want, tracker.events, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected events (-want,+got):\n%s", diff)
			}
		})
	}
}

// newTrackerWithEvents returns a tracker that recorded the given events.
func newTrackerWithEvents(recorded map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events) *PreemptionLimitTracker {
	tracker := NewPreemptionLimitTracker()
	for limit, scopes := range recorded {
		for scopeValue, e := range scopes {
			for _, ts := range e {
				tracker.Record(ts, map[policy.PreemptionLimitReference]policy.PreemptionLimitScopeValue{limit: scopeValue})
			}
		}
	}
	return tracker
}
