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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

// preemptionLimitMatcher decides whether one PreemptionLimit applies to a preemption,
// and which of its buckets the preemption counts against.
type preemptionLimitMatcher struct {
	limit                *kueuealpha.PreemptionLimit
	clusterQueueSelector labels.Selector
}

// newPreemptionLimitMatcher returns the matcher of limit, or false if limit doesn't
// apply to config. It returns an error if a selector it needs is invalid.
func newPreemptionLimitMatcher(limit *kueuealpha.PreemptionLimit, config *kueuealpha.PreemptionConfig) (preemptionLimitMatcher, bool, error) {
	configSelector, err := selectorOrEverything(limit.Spec.ConfigSelector)
	if err != nil {
		return preemptionLimitMatcher{}, false, fmt.Errorf("preemptionLimit %q configSelector: %w", limit.Name, err)
	}
	if !configSelector.Matches(labels.Set(config.Labels)) {
		return preemptionLimitMatcher{}, false, nil
	}
	clusterQueueSelector, err := selectorOrEverything(limit.Spec.ClusterQueueSelector)
	if err != nil {
		return preemptionLimitMatcher{}, false, fmt.Errorf("preemptionLimit %q clusterQueueSelector: %w", limit.Name, err)
	}
	return preemptionLimitMatcher{limit: limit, clusterQueueSelector: clusterQueueSelector}, true, nil
}

// selectorOrEverything returns the selector of ls. A nil ls selects everything,
// unlike metav1.LabelSelectorAsSelector(nil), which selects nothing.
func selectorOrEverything(ls *metav1.LabelSelector) (labels.Selector, error) {
	if ls == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(ls)
}

// matchesClusterQueues returns whether the clusterQueueSelector of the limit selects
// the ClusterQueue of its scope: the preemptor's one for PreemptingClusterQueue, the
// target's one for PreemptedClusterQueue and PreemptedWorkload, and either of them
// for Global.
func (m preemptionLimitMatcher) matchesClusterQueues(preemptorCQ, targetCQ *schdcache.ClusterQueueSnapshot) bool {
	switch m.limit.Spec.Scope {
	case kueuealpha.PreemptingCQLimitScope:
		return m.clusterQueueSelector.Matches(labels.Set(preemptorCQ.Labels))
	case kueuealpha.PreemptedCQLimitScope, kueuealpha.PreemptedWorkloadLimitScope:
		return m.clusterQueueSelector.Matches(labels.Set(targetCQ.Labels))
	default:
		// GlobalPreemptionLimitScope, as the API admits no other scope.
		return m.clusterQueueSelector.Matches(labels.Set(preemptorCQ.Labels)) ||
			m.clusterQueueSelector.Matches(labels.Set(targetCQ.Labels))
	}
}

// scopeValue returns the scope value of the limit that preempting target to admit
// preemptor counts against.
func (m preemptionLimitMatcher) scopeValue(preemptor, target *workload.Info) policy.PreemptionLimitScopeValue {
	switch m.limit.Spec.Scope {
	case kueuealpha.PreemptingCQLimitScope:
		return policy.PreemptionLimitScopeValue(preemptor.ClusterQueue)
	case kueuealpha.PreemptedCQLimitScope:
		return policy.PreemptionLimitScopeValue(target.ClusterQueue)
	case kueuealpha.PreemptedWorkloadLimitScope:
		return policy.PreemptionLimitScopeValue(workload.Key(target.Obj))
	default:
		// GlobalPreemptionLimitScope, as the API admits no other scope.
		return policy.PreemptionLimitScopeValue(kueuealpha.GlobalPreemptionLimitScope)
	}
}
