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
	"k8s.io/klog/v2"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/priority"
	"sigs.k8s.io/kueue/pkg/workload"
)

type workloadPriorityFilter struct {
	log               logr.Logger
	relation          kueuev1beta2.RelativeConstraint
	preemptorPriority int64
}

// NewWorkloadPriorityFilter creates a WorkloadFilter to evaluate candidate workloads
// based on relative workload priority compared against the preemptor workload.
// The effective priority (accounting for priority boost if configured) is used for comparison.
func NewWorkloadPriorityFilter(log logr.Logger, relation kueuev1beta2.RelativeConstraint, preemptor *workload.Info) WorkloadFilter {
	filterLog := log.WithValues("relation", relation)
	preemptorLog := filterLog.WithValues("preemptor", klog.KObj(preemptor.Obj))
	preemptorPriority := priority.EffectivePriority(preemptorLog, preemptor.Obj)
	preemptorLog.V(3).Info("Configured workload priority filter with preemptor effective priority", "preemptorPriority", preemptorPriority)

	return &workloadPriorityFilter{
		log:               filterLog,
		relation:          relation,
		preemptorPriority: preemptorPriority,
	}
}

// Matches evaluates a candidate workload's effective priority against the preemptor's priority.
func (f *workloadPriorityFilter) Matches(wl *workload.Info) bool {
	candLog := f.log.WithValues("candidate", klog.KObj(wl.Obj))
	candPriority := priority.EffectivePriority(candLog, wl.Obj)
	return matchesRelation(candLog, &f.relation, candPriority, f.preemptorPriority)
}
