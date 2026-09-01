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
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type snapshotBuilder struct {
	mgr hierarchy.Manager[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot]
}

func newSnapshotBuilder() *snapshotBuilder {
	return &snapshotBuilder{
		mgr: hierarchy.NewManager(func(name kueuev1beta2.CohortReference) *schdcache.CohortSnapshot {
			return &schdcache.CohortSnapshot{
				Name:   name,
				Cohort: hierarchy.NewCohort[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot](),
			}
		}),
	}
}

func (b *snapshotBuilder) Cohort(name, parent kueuev1beta2.CohortReference) *snapshotBuilder {
	b.mgr.AddCohort(name)
	if parent != "" {
		b.mgr.UpdateCohortEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) ClusterQueue(name kueuev1beta2.ClusterQueueReference, parent kueuev1beta2.CohortReference) *snapshotBuilder {
	b.mgr.AddClusterQueue(&schdcache.ClusterQueueSnapshot{Name: name})
	if parent != "" {
		b.mgr.UpdateClusterQueueEdge(name, parent)
	}
	return b
}

func (b *snapshotBuilder) Build() *schdcache.Snapshot {
	return &schdcache.Snapshot{
		Manager: b.mgr,
	}
}

type WorkloadInfoWrapper struct {
	*utiltestingapi.WorkloadWrapper
	clusterQueue kueuev1beta2.ClusterQueueReference
}

func MakeWorkloadInfo(name, namespace string) *WorkloadInfoWrapper {
	return &WorkloadInfoWrapper{
		WorkloadWrapper: utiltestingapi.MakeWorkload(name, namespace),
	}
}

func (w *WorkloadInfoWrapper) ClusterQueue(cq kueuev1beta2.ClusterQueueReference) *WorkloadInfoWrapper {
	w.clusterQueue = cq
	return w
}

func (w *WorkloadInfoWrapper) Queue(q kueuev1beta2.LocalQueueName) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Queue(q)
	return w
}

func (w *WorkloadInfoWrapper) Label(k, v string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Label(k, v)
	return w
}

func (w *WorkloadInfoWrapper) Labels(l map[string]string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Labels(l)
	return w
}

func (w *WorkloadInfoWrapper) Annotation(k, v string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Annotation(k, v)
	return w
}

func (w *WorkloadInfoWrapper) Annotations(a map[string]string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Annotations(a)
	return w
}

func (w *WorkloadInfoWrapper) Priority(p int32) *WorkloadInfoWrapper {
	w.WorkloadWrapper.Priority(p)
	return w
}

func (w *WorkloadInfoWrapper) PriorityClassRef(ref *kueuev1beta2.PriorityClassRef) *WorkloadInfoWrapper {
	w.WorkloadWrapper.PriorityClassRef(ref)
	return w
}

func (w *WorkloadInfoWrapper) WorkloadPriorityClassRef(name string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.WorkloadPriorityClassRef(name)
	return w
}

func (w *WorkloadInfoWrapper) PodPriorityClassRef(name string) *WorkloadInfoWrapper {
	w.WorkloadWrapper.PodPriorityClassRef(name)
	return w
}

func (w *WorkloadInfoWrapper) Obj() *workload.Info {
	info := workload.NewInfo(w.WorkloadWrapper.Obj())
	if w.clusterQueue != "" {
		info.ClusterQueue = w.clusterQueue
	}
	return info
}
