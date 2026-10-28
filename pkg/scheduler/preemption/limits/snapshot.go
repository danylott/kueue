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

// PreemptionLimitsSnapshot holds the PreemptionLimits and the preemptions counted
// against them during one scheduling cycle. Changes to the PreemptionLimits or to
// the tracker apply from the next cycle, like changes to ClusterQueues.
type PreemptionLimitsSnapshot struct {
	// limits are kept in the listed order, so that errors are deterministic.
	limits []*kueuealpha.PreemptionLimit
	// recorded holds the preemptions recorded within the window of each limit when
	// the snapshot was taken, plus the ones issued since in this cycle.
	recorded preemptionCounts
}

// Snapshot returns a snapshot of preemptionLimits with the preemptions recorded
// within their windows ending at now. The caller must not modify preemptionLimits.
func (t *PreemptionLimitTracker) Snapshot(now time.Time, preemptionLimits []kueuealpha.PreemptionLimit) *PreemptionLimitsSnapshot {
	limits := make([]*kueuealpha.PreemptionLimit, len(preemptionLimits))
	for i := range preemptionLimits {
		limits[i] = &preemptionLimits[i]
	}
	return &PreemptionLimitsSnapshot{limits: limits, recorded: t.counts(now, limits)}
}

// Record counts a preemption issued in this cycle against every PreemptionLimit
// and scope value in consumedLimits.
func (s *PreemptionLimitsSnapshot) Record(consumedLimits policy.ConsumedLimits) {
	for limit, scopeValue := range consumedLimits {
		s.recorded.add(limit, scopeValue, 1)
	}
}
