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
	"k8s.io/klog/v2"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

// PreemptionLimitBudget tracks how many more preemptions each PreemptionLimit allows
// during one search for preemption targets.
//
// A budget is read from the limiter the first time it is needed, then reduced locally
// for every preemption taken. That stays accurate because no preemption is recorded
// while a scheduling attempt runs.
type PreemptionLimitBudget struct {
	limiter   *PreemptionLimiter
	remaining map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32
}

// NewBudget returns a budget for one search of preemption targets, with nothing taken yet.
// It may be called on a nil PreemptionLimiter; the budget then allows every preemption.
func (l *PreemptionLimiter) NewBudget() *PreemptionLimitBudget {
	return &PreemptionLimitBudget{
		limiter:   l,
		remaining: make(map[policy.PreemptionLimitReference]map[policy.PreemptionLimitScopeValue]int32),
	}
}

// TryConsume takes one preemption from every PreemptionLimit that preempting target
// consumes, and returns those limits.
//
// If any of them has no budget left, it takes nothing and returns false.
// If no limit applies, or the preemption of target is already in progress, it takes
// nothing and returns no limits and true.
func (b *PreemptionLimitBudget) TryConsume(
	preemptor, target *workload.Info,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) (policy.ConsumedLimits, bool) {
	consumedLimits := b.limiter.consumedLimits(preemptor, target, preemptorCQ, targetCQ)
	if len(consumedLimits) == 0 {
		return nil, true
	}
	if limit, ok := b.take(consumedLimits); !ok {
		b.limiter.log.V(5).Info("Skipping preemption candidate exceeding a PreemptionLimit",
			"candidate", klog.KObj(target.Obj), "preemptionLimit", limit, "scopeValue", consumedLimits[limit])
		return nil, false
	}
	return consumedLimits, true
}

// take takes one preemption from the budget of every PreemptionLimit and scope value in
// consumedLimits, if all of them have budget left. Otherwise, it takes nothing and
// returns a PreemptionLimit without budget left.
func (b *PreemptionLimitBudget) take(consumedLimits policy.ConsumedLimits) (policy.PreemptionLimitReference, bool) {
	for limit, scopeValue := range consumedLimits {
		if b.remainingFor(limit, scopeValue) <= 0 {
			return limit, false
		}
	}
	for limit, scopeValue := range consumedLimits {
		b.remaining[limit][scopeValue]--
	}
	return "", true
}

// remainingFor returns the budget left for limit and scopeValue, asking the limiter on
// first use.
func (b *PreemptionLimitBudget) remainingFor(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue) int32 {
	scopes, found := b.remaining[limit]
	if !found {
		scopes = make(map[policy.PreemptionLimitScopeValue]int32)
		b.remaining[limit] = scopes
	}
	remaining, found := scopes[scopeValue]
	if !found {
		remaining = b.limiter.allowance(limit, scopeValue)
		scopes[scopeValue] = remaining
	}
	return remaining
}
