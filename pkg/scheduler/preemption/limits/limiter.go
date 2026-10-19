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

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/expectations"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

// PreemptionLimiter enforces the PreemptionLimits that apply to one PreemptionConfig
// during one scheduling attempt.
//
// It decides which PreemptionLimits a preemption consumes, and how many more
// preemptions each of them allows. The budgets it creates do the counting.
// A nil PreemptionLimiter enforces no PreemptionLimit.
type PreemptionLimiter struct {
	log          logr.Logger
	matchers     map[policy.PreemptionLimitReference]preemptionLimitMatcher
	tracker      *PreemptionLimitTracker
	expectations *expectations.Store
	// now is the time of the scheduling attempt, where the windows of the
	// PreemptionLimits end.
	now time.Time
}

// NewPreemptionLimiter returns the PreemptionLimiter of the PreemptionLimits whose
// configSelector selects config.
//
// It returns an error if the configSelector of any PreemptionLimit is invalid, or if the
// clusterQueueSelector of a PreemptionLimit applying to config is invalid.
func NewPreemptionLimiter(
	log logr.Logger,
	preemptionLimits []kueuealpha.PreemptionLimit,
	config *kueuealpha.PreemptionConfig,
	tracker *PreemptionLimitTracker,
	preemptionExpectations *expectations.Store,
	now time.Time,
) (*PreemptionLimiter, error) {
	matchers := make(map[policy.PreemptionLimitReference]preemptionLimitMatcher)
	for i := range preemptionLimits {
		limit := &preemptionLimits[i]
		matcher, applies, err := newPreemptionLimitMatcher(limit, config)
		if err != nil {
			return nil, err
		}
		if applies {
			matchers[policy.PreemptionLimitReference(limit.Name)] = matcher
		}
	}
	return &PreemptionLimiter{
		log:          log,
		matchers:     matchers,
		tracker:      tracker,
		expectations: preemptionExpectations,
		now:          now,
	}, nil
}

// consumedLimits returns the PreemptionLimits that preempting target to admit preemptor
// consumes, each with its scope value. preemptorCQ and targetCQ are the ClusterQueues of
// preemptor and target.
//
// It returns nil if no PreemptionLimit applies, or if the preemption of target is already
// in progress.
func (l *PreemptionLimiter) consumedLimits(
	preemptor, target *workload.Info,
	preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot,
) policy.ConsumedLimits {
	if l == nil {
		return nil
	}
	var consumedLimits policy.ConsumedLimits
	for ref, matcher := range l.matchers {
		if !matcher.matchesClusterQueues(preemptorCQ, targetCQ) {
			continue
		}
		if consumedLimits == nil {
			consumedLimits = make(policy.ConsumedLimits)
		}
		consumedLimits[ref] = matcher.scopeValue(preemptor, target)
	}
	if len(consumedLimits) == 0 || l.preemptionInProgress(target) {
		return nil
	}
	return consumedLimits
}

// preemptionInProgress returns whether target is evicted, or its eviction was issued but
// is not observed yet.
//
// A preemptor waiting for such a target selects it again every cycle until its quota is
// released. IssuePreemptions neither evicts it again nor records it, so it must not
// consume any budget either.
func (l *PreemptionLimiter) preemptionInProgress(target *workload.Info) bool {
	return workloadevict.IsEvicted(target.Obj) ||
		!l.expectations.Satisfied(l.log, client.ObjectKeyFromObject(target.Obj))
}

// allowance returns how many more preemptions limit allows for scopeValue: its limit
// minus the preemptions recorded within its window.
func (l *PreemptionLimiter) allowance(limit policy.PreemptionLimitReference, scopeValue policy.PreemptionLimitScopeValue) int32 {
	preemptionLimit := l.matchers[limit].limit
	return preemptionLimit.Spec.Limit - l.tracker.Count(l.now, preemptionLimit, scopeValue)
}
