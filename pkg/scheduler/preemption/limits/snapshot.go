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
	"time"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
)

// PreemptionLimitsSnapshot captures PreemptionLimits and the preemption events
// recorded against them at a single point in time. It allows to reason about
// the current limits state in one scheduling cycle without danger of concurrent
// modification of PreemptionLimits or currently tracked preemptions, caused by
// user actions (e.g. delete of a limit resource).
//
// Moreover, snapshot allows to update the underlying captured state via Record
// method, to take into account additional preemptions that were issued after it
// was captured, for example to track preemptions already issued in the same
// scheduling cycle.
type PreemptionLimitsSnapshot struct {
	// limits are kept in the given order, so that errors are deterministic.
	limits []*kueuealpha.PreemptionLimit
	byName map[policy.PreemptionLimitReference]*kueuealpha.PreemptionLimit
	// events holds the preemption timestamps within the window of each limit when
	// the snapshot was taken, plus ones added via Record.
	events map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]events
}

// Record records a preemption at ts in the snapshot for every PreemptionLimit
// and scope value in consumedLimits.
func (s *PreemptionLimitsSnapshot) Record(ts time.Time, consumedLimits policy.ConsumedLimits) {
	recordEvents(s.events, ts, consumedLimits)
}

// WithinLimits reports whether all preemptions in consumedLimits fit together
// within the remaining limits in the snapshot. A nil PreemptionLimitsSnapshot
// allows no preemptions.
func (s *PreemptionLimitsSnapshot) WithinLimits(consumedLimits []policy.ConsumedLimits) bool {
	if s == nil {
		return len(consumedLimits) == 0
	}
	taken := preemptionCounts{}
	for _, consumed := range consumedLimits {
		for limit, scopeValue := range consumed {
			taken.add(limit, scopeValue, 1)
			if taken[limit][scopeValue] > s.allowance(limit, scopeValue) {
				return false
			}
		}
	}
	return true
}

// allowance returns the limit minus the preemptions recorded for it in the
// snapshot, or 0 if the limit is not in the snapshot.
func (s *PreemptionLimitsSnapshot) allowance(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue) int32 {
	preemptionLimit, found := s.byName[limit]
	if !found {
		return 0
	}
	return preemptionLimit.Spec.Limit - int32(len(s.events[limit][scopeValue]))
}

// nextSlotTime returns when count more preemptions fit within limit for scopeValue,
// or nil if waiting alone never makes room. count must exceed the current allowance.
func (s *PreemptionLimitsSnapshot) nextSlotTime(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue, count int32) *time.Time {
	preemptionLimit, found := s.byName[limit]
	if !found || count > preemptionLimit.Spec.Limit {
		return nil
	}
	events := s.events[limit][scopeValue]
	index := int(int32(len(events))+count-preemptionLimit.Spec.Limit) - 1
	return new(events[index].Add(time.Duration(preemptionLimit.Spec.LimitWindowSeconds) * time.Second))
}
