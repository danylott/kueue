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

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
)

func TestPreemptionLimitsSnapshotRecord(t *testing.T) {
	now := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	perCQ := *utiltestingalpha.MakePreemptionLimit("per-cq", kueuealpha.PreemptedCQLimitScope).
		Limit(3).LimitWindowSeconds(60).Obj()

	cases := map[string]struct {
		events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
		// issued are the preemptions issued in this cycle, after the snapshot was taken.
		issued []policy.ConsumedLimits
		scope  policy.PreemptionLimitScopeValue
		want   int32
	}{
		"preemptions issued in this cycle count against the limit along with the recorded ones": {
			events: map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events{
				"per-cq": {"cq-a": {now.Add(-30 * time.Second)}},
			},
			issued: []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			scope:  "cq-a",
			want:   1,
		},
		"preemptions issued in this cycle count only for their scope value": {
			issued: []policy.ConsumedLimits{{"per-cq": "cq-a"}},
			scope:  "cq-b",
			want:   3,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := NewPreemptionLimitTracker()
			if tc.events != nil {
				tracker.events = tc.events
			}
			snapshot := tracker.Snapshot(now, []kueuealpha.PreemptionLimit{perCQ})
			for _, consumed := range tc.issued {
				snapshot.Record(consumed)
			}
			limiter, err := NewPreemptionLimiter(logr.Discard(), snapshot, utiltestingalpha.MakePreemptionConfig("config").Obj(), nil)
			if err != nil {
				t.Fatalf("NewPreemptionLimiter() unexpected error: %v", err)
			}
			if got := limiter.allowance("per-cq", tc.scope); got != tc.want {
				t.Errorf("allowance(%q) = %d, want %d", tc.scope, got, tc.want)
			}
		})
	}
}
