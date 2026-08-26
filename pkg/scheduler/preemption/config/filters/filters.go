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
// Level 1 filtering evaluates entire ClusterQueues to prune ineligible queues in O(1) operations,
// preventing unnecessary iteration over all individual workloads within those queues.
type ClusterQueueFilter interface {
	Matches(cq *schdcache.ClusterQueueSnapshot) bool
}

// WorkloadFilter evaluates whether a specific candidate workload is eligible for preemption.
// Level 2 filtering executes only on candidate workloads from ClusterQueues that passed Level 1.
type WorkloadFilter interface {
	Matches(wl *workload.Info) bool
}

// CandidateFilters contains the complete 2-level filter set compiled for a candidate selector.
type CandidateFilters struct {
	CQFilters []ClusterQueueFilter
	WLFilters []WorkloadFilter
}

// FilterClusterQueues applies all Level 1 CQFilters to a list of ClusterQueues.
// If no CQFilters are configured, it returns the input slice directly with zero allocations.
func (cf *CandidateFilters) FilterClusterQueues(cqs []*schdcache.ClusterQueueSnapshot) []*schdcache.ClusterQueueSnapshot {
	if len(cf.CQFilters) == 0 {
		return cqs
	}
	var matchingCQs []*schdcache.ClusterQueueSnapshot
	for _, cq := range cqs {
		if cf.matchesAllCQ(cq) {
			matchingCQs = append(matchingCQs, cq)
		}
	}
	return matchingCQs
}

// FilterWorkloads applies all Level 2 WLFilters to candidate workloads.
// If no WLFilters are configured, it returns the input slice directly with zero allocations.
func (cf *CandidateFilters) FilterWorkloads(candidates []*workload.Info) []*workload.Info {
	if len(cf.WLFilters) == 0 {
		return candidates
	}
	var matchingWorkloads []*workload.Info
	for _, wl := range candidates {
		if cf.matchesAllWL(wl) {
			matchingWorkloads = append(matchingWorkloads, wl)
		}
	}
	return matchingWorkloads
}

func (cf *CandidateFilters) matchesAllCQ(cq *schdcache.ClusterQueueSnapshot) bool {
	for _, f := range cf.CQFilters {
		if !f.Matches(cq) {
			return false
		}
	}
	return true
}

func (cf *CandidateFilters) matchesAllWL(wl *workload.Info) bool {
	for _, f := range cf.WLFilters {
		if !f.Matches(wl) {
			return false
		}
	}
	return true
}
