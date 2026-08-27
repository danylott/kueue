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

package filters

import (
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

// ClusterQueueFilter evaluates whether a ClusterQueue is eligible to yield preemption candidates.
// ClusterQueue filtering evaluates entire ClusterQueues to prune ineligible queues,
// making unnecessary iteration over all individual workloads within those queues avoidable.
type ClusterQueueFilter interface {
	Matches(cq *schdcache.ClusterQueueSnapshot) bool
}

// WorkloadFilter evaluates whether a specific candidate workload is eligible for preemption.
type WorkloadFilter interface {
	Matches(wl *workload.Info) bool
}

// CandidateFilters contains the complete filter set compiled for a candidate selector.
type CandidateFilters struct {
	CQFilters []ClusterQueueFilter
	WLFilters []WorkloadFilter
}

// MatchesCQ returns true if the given ClusterQueue passes all configured CQFilters.
func (cf *CandidateFilters) MatchesCQ(cq *schdcache.ClusterQueueSnapshot) bool {
	for _, f := range cf.CQFilters {
		if !f.Matches(cq) {
			return false
		}
	}
	return true
}

// FilterClusterQueues applies all CQFilters to a list of ClusterQueues.
// If no CQFilters are configured, it returns the input slice directly with zero allocations.
func (cf *CandidateFilters) FilterClusterQueues(cqs []*schdcache.ClusterQueueSnapshot) []*schdcache.ClusterQueueSnapshot {
	if len(cf.CQFilters) == 0 {
		return cqs
	}
	var matchingCQs []*schdcache.ClusterQueueSnapshot
	for _, cq := range cqs {
		if cf.MatchesCQ(cq) {
			matchingCQs = append(matchingCQs, cq)
		}
	}
	return matchingCQs
}

// MatchesWorkload returns true if the candidate workload passes all configured WLFilters.
func (cf *CandidateFilters) MatchesWorkload(wl *workload.Info) bool {
	for _, f := range cf.WLFilters {
		if !f.Matches(wl) {
			return false
		}
	}
	return true
}

// FilterWorkloads applies all WLFilters to candidate workloads.
// If no WLFilters are configured, it returns the input slice directly with zero allocations.
func (cf *CandidateFilters) FilterWorkloads(candidates []*workload.Info) []*workload.Info {
	if len(cf.WLFilters) == 0 {
		return candidates
	}
	var matchingWorkloads []*workload.Info
	for _, wl := range candidates {
		if cf.MatchesWorkload(wl) {
			matchingWorkloads = append(matchingWorkloads, wl)
		}
	}
	return matchingWorkloads
}
