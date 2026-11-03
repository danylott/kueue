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

	"k8s.io/klog/v2"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

// PreemptionLimitBudget counts the preemptions taken from the PreemptionLimits
// during one pass of candidate selection.
type PreemptionLimitBudget struct {
	limiter *PreemptionLimiter
	// taken holds the preemptions taken from this budget.
	taken preemptionCounts
}

// NewBudget returns an empty budget for one pass of candidate selection.
// A budget from a nil PreemptionLimiter imposes no limits.
func (l *PreemptionLimiter) NewBudget() *PreemptionLimitBudget {
	return &PreemptionLimitBudget{
		limiter: l,
		taken:   preemptionCounts{},
	}
}

// TryConsume takes one preemption from every limit that preempting target
// consumes, and returns those limits.
//
// If any limit has no budget left, it takes nothing and returns false along with
// when every limit has budget left again, or nil if waiting alone is not enough.
func (b *PreemptionLimitBudget) TryConsume(
	preemptor, target *workload.Info,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) (policy.ConsumedLimits, *time.Time, bool) {
	consumedLimits := b.limiter.consumedLimits(preemptor, target, preemptorCQ, targetCQ)
	if len(consumedLimits) == 0 {
		return nil, nil, true
	}
	var (
		blockingLimit    policy.PreemptionLimitReference
		candidateRetryAt *time.Time
	)
	for limit, scopeValue := range consumedLimits {
		if b.remaining(limit, scopeValue) > 0 {
			continue
		}
		slotTime := b.limiter.snapshot.nextSlotTime(limit, scopeValue, b.taken[limit][scopeValue]+1)
		if slotTime == nil {
			b.limiter.log.V(5).Info("Skipping preemption candidate exceeding a PreemptionLimit",
				"candidate", klog.KObj(target.Obj), "preemptionLimit", limit, "scopeValue", scopeValue)
			return nil, nil, false
		}
		if candidateRetryAt == nil || slotTime.After(*candidateRetryAt) {
			blockingLimit = limit
			candidateRetryAt = slotTime
		}
	}
	// Any exhausted limit either returned early above or set candidateRetryAt.
	if candidateRetryAt != nil {
		b.limiter.log.V(5).Info("Skipping preemption candidate exceeding a PreemptionLimit",
			"candidate", klog.KObj(target.Obj), "preemptionLimit", blockingLimit, "scopeValue", consumedLimits[blockingLimit], "nextSlotTime", *candidateRetryAt)
		return nil, candidateRetryAt, false
	}
	for limit, scopeValue := range consumedLimits {
		b.taken.add(limit, scopeValue, 1)
	}
	return consumedLimits, nil, true
}

// remaining returns the limiter's allowance minus the preemptions taken from this budget.
func (b *PreemptionLimitBudget) remaining(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue) int32 {
	return b.limiter.allowance(limit, scopeValue) - b.taken[limit][scopeValue]
}
