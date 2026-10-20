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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestPreemptionLimitBudgetTryConsume(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	globalLimit := utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		Limit(1).LimitWindowSeconds(60).Obj()
	preemptorCQ := &schdcache.ClusterQueueSnapshot{Name: "preempting-cq"}
	targetCQ := &schdcache.ClusterQueueSnapshot{Name: "preempted-cq"}
	preemptor := &workload.Info{Obj: utiltestingapi.MakeWorkload("preemptor", "ns").Obj(), ClusterQueue: "preempting-cq"}
	targetWl := utiltestingapi.MakeWorkload("target", "ns").UID("target")

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		taken            preemptionCounts
		target           *workload.Info
		wantConsumed     policy.ConsumedLimits
		wantOK           bool
		wantTaken        preemptionCounts
	}{
		"applicable limits are consumed and taken from the budget": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*globalLimit},
			target:           &workload.Info{Obj: targetWl.Clone().Obj(), ClusterQueue: "preempted-cq"},
			wantConsumed:     policy.ConsumedLimits{"global": "Global"},
			wantOK:           true,
			wantTaken:        preemptionCounts{"global": {"Global": 1}},
		},
		"target exceeding a limit is blocked": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*globalLimit},
			taken:            preemptionCounts{"global": {"Global": 1}},
			target:           &workload.Info{Obj: targetWl.Clone().Obj(), ClusterQueue: "preempted-cq"},
			wantTaken:        preemptionCounts{"global": {"Global": 1}},
		},
		"target whose preemption is in progress is allowed with an exhausted budget": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*globalLimit},
			taken:            preemptionCounts{"global": {"Global": 1}},
			target:           &workload.Info{Obj: targetWl.Clone().EvictedAt(now).Obj(), ClusterQueue: "preempted-cq"},
			wantOK:           true,
			wantTaken:        preemptionCounts{"global": {"Global": 1}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			limiter, err := NewPreemptionLimiter(logr.Discard(), tc.preemptionLimits, utiltestingalpha.MakePreemptionConfig("config").Obj(),
				NewPreemptionLimitTracker(), preemptexpectations.New(), now)
			if err != nil {
				t.Fatalf("NewPreemptionLimiter() unexpected error: %v", err)
			}
			budget := limiter.NewBudget()
			if tc.taken != nil {
				budget.taken = tc.taken
			}

			gotConsumed, ok := budget.TryConsume(preemptor, tc.target, preemptorCQ, targetCQ)

			if ok != tc.wantOK {
				t.Errorf("TryConsume() ok = %v, want %v", ok, tc.wantOK)
			}
			if diff := cmp.Diff(tc.wantConsumed, gotConsumed); diff != "" {
				t.Errorf("Unexpected consumed limits (-want,+got):\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantTaken, budget.taken, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected taken preemptions (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimitBudgetTake(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	global := utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).
		Limit(3).LimitWindowSeconds(60)
	perCQ := utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(1).LimitWindowSeconds(60)
	twoRecordedGlobalEvents := map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
		"global": {"Global": {now.Add(-2 * time.Minute), now.Add(-30 * time.Second), now}},
	}

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		events           map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		taken            preemptionCounts
		consumedLimits   policy.ConsumedLimits
		wantBlockedBy    policy.PreemptionLimitReference
		wantTaken        preemptionCounts
	}{
		"budget is the limit minus the recorded preemptions": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Obj()},
			events:           twoRecordedGlobalEvents,
			consumedLimits:   policy.ConsumedLimits{"global": "Global"},
			wantTaken:        preemptionCounts{"global": {"Global": 1}},
		},
		"preemptions taken by this search count against the budget": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Obj()},
			events:           twoRecordedGlobalEvents,
			taken:            preemptionCounts{"global": {"Global": 1}},
			consumedLimits:   policy.ConsumedLimits{"global": "Global"},
			wantBlockedBy:    "global",
			wantTaken:        preemptionCounts{"global": {"Global": 1}},
		},
		"each preemption taken adds to what this search took": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Obj()},
			taken:            preemptionCounts{"global": {"Global": 2}},
			consumedLimits:   policy.ConsumedLimits{"global": "Global"},
			wantTaken:        preemptionCounts{"global": {"Global": 3}},
		},
		"limit of 0 has no budget": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Clone().Limit(0).Obj()},
			consumedLimits:   policy.ConsumedLimits{"global": "Global"},
			wantBlockedBy:    "global",
		},
		"every consumed limit is taken from": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Obj(), *perCQ.Obj()},
			consumedLimits:   policy.ConsumedLimits{"global": "Global", "per-cq": "cq-a"},
			wantTaken: preemptionCounts{
				"global": {"Global": 1},
				"per-cq": {"cq-a": 1},
			},
		},
		"limit without budget left blocks, and nothing is taken from the others": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*global.Obj(), *perCQ.Obj()},
			taken:            preemptionCounts{"per-cq": {"cq-a": 1}},
			consumedLimits:   policy.ConsumedLimits{"global": "Global", "per-cq": "cq-a"},
			wantBlockedBy:    "per-cq",
			wantTaken:        preemptionCounts{"per-cq": {"cq-a": 1}},
		},
		"scope values of a limit have independent budgets": {
			preemptionLimits: []kueuealpha.PreemptionLimit{*perCQ.Obj()},
			taken:            preemptionCounts{"per-cq": {"cq-a": 1}},
			consumedLimits:   policy.ConsumedLimits{"per-cq": "cq-b"},
			wantTaken:        preemptionCounts{"per-cq": {"cq-a": 1, "cq-b": 1}},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			limiter, err := NewPreemptionLimiter(logr.Discard(), tc.preemptionLimits, utiltestingalpha.MakePreemptionConfig("config").Obj(), tracker, nil, now)
			if err != nil {
				t.Fatalf("NewPreemptionLimiter() unexpected error: %v", err)
			}
			budget := limiter.NewBudget()
			if tc.taken != nil {
				budget.taken = tc.taken
			}

			blockedBy, ok := budget.take(tc.consumedLimits)

			if blockedBy != tc.wantBlockedBy || ok != (tc.wantBlockedBy == "") {
				t.Errorf("take() = (%q, %v), want (%q, %v)", blockedBy, ok, tc.wantBlockedBy, tc.wantBlockedBy == "")
			}
			if diff := cmp.Diff(tc.wantTaken, budget.taken, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("Unexpected taken preemptions (-want,+got):\n%s", diff)
			}
		})
	}
}
