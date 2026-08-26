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
	"github.com/go-logr/logr"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

// CandidateFiltersFactory compiles PreemptionCandidateSelector rules into 2-level CandidateFilters.
type CandidateFiltersFactory struct {
	snapshot *schdcache.Snapshot
}

// NewCandidateFiltersFactory creates a factory configured with the cluster state snapshot.
func NewCandidateFiltersFactory(snapshot *schdcache.Snapshot) *CandidateFiltersFactory {
	return &CandidateFiltersFactory{snapshot: snapshot}
}

// Build compiles a candidate selector into Level 1 (CQ) and Level 2 (Workload) filters.
func (f *CandidateFiltersFactory) Build(
	log logr.Logger,
	selector *kueuev1beta2.PreemptionCandidateSelector,
	preemptor *workload.Info,
) CandidateFilters {
	if selector == nil {
		return CandidateFilters{}
	}

	cqRelationFilters, wlRelationFilters := f.buildRelationFilters(log, selector.RelationRequirement, preemptor)
	wlNumericFilters := f.buildNumericLabelFilters(log, selector.NumericLabels, preemptor)

	return CandidateFilters{
		CQFilters: cqRelationFilters,
		WLFilters: append(wlRelationFilters, wlNumericFilters...),
	}
}

func (f *CandidateFiltersFactory) buildRelationFilters(
	log logr.Logger,
	relation kueuev1beta2.PreemptionRelationConstraint,
	preemptor *workload.Info,
) ([]ClusterQueueFilter, []WorkloadFilter) {
	switch relation {
	case kueuev1beta2.SameLocalQueue:
		// Level 1: Prune all other ClusterQueues
		// Level 2: Match exact LocalQueue within the remaining ClusterQueue
		return []ClusterQueueFilter{NewSameClusterQueueFilter(preemptor.ClusterQueue)},
			[]WorkloadFilter{NewSameLocalQueueFilter(preemptor.Obj.Namespace, preemptor.Obj.Spec.QueueName)}

	case kueuev1beta2.SameClusterQueue:
		return []ClusterQueueFilter{NewSameClusterQueueFilter(preemptor.ClusterQueue)}, nil

	case kueuev1beta2.SameCohort:
		return []ClusterQueueFilter{NewSameCohortFilter(preemptor.ClusterQueue, f.snapshot)}, nil

	case kueuev1beta2.SameCohortTree:
		return []ClusterQueueFilter{NewSameCohortTreeFilter(preemptor.ClusterQueue, f.snapshot)}, nil

	case kueuev1beta2.AnyClusterQueue:
		return nil, nil

	default:
		log.V(3).Info("Unsupported or unhandled relation constraint evaluated; 0 candidates permitted", "relation", relation)
		return []ClusterQueueFilter{NewRejectAllCQFilter()}, nil
	}
}

func (f *CandidateFiltersFactory) buildNumericLabelFilters(
	log logr.Logger,
	labels []kueuev1beta2.NumericLabelConstraint,
	preemptor *workload.Info,
) []WorkloadFilter {
	if len(labels) == 0 {
		return nil
	}
	filters := make([]WorkloadFilter, 0, len(labels))
	for _, numConstraint := range labels {
		filters = append(filters, NewNumericLabelFilter(log, numConstraint, preemptor))
	}
	return filters
}
