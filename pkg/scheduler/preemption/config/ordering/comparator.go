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

package ordering

import (
	"cmp"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/util/priority"
	"sigs.k8s.io/kueue/pkg/workload"
)

var defaultOrdering = []kueue.Order{
	{OrderingField: kueue.Priority, Direction: kueue.Ascending},
	{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending},
}

// CompareCandidates compares two candidate workloads according to the configured ordering rules,
// using Workload UID comparison as a deterministic tie-breaker.
// If ordering is empty, it falls back to the default ordering:
// 1. Priority (Ascending: lowest priority first)
// 2. AdmissionTimestamp (Descending: most recently admitted first, protecting long-running workloads)
// 3. UID (Ascending: deterministic tie-breaker)
//
// Natural field comparisons:
// - Priority: Natural integer comparison (cmp.Compare(prioA, prioB)).
// - AdmissionTimestamp: Natural time comparison (timestampA.Compare(timestampB)).
// - IsOtherCQ: Natural boolean comparison (compareBool(isOtherA, isOtherB) where false < true).
// - IsOtherCohort: Natural boolean comparison (compareBool(isOtherCohortA, isOtherCohortB) where false < true).
//
// Direction handling:
// - Ascending (default): Preserves natural comparison result.
// - Descending: Negates comparison result (res = -res).
//
// Deterministic tie-breaker:
// - Workload UID comparison (cmp.Compare(a.Obj.UID, b.Obj.UID)).
func CompareCandidates(
	log logr.Logger,
	ordering []kueue.Order,
	a, b *workload.Info,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
	now time.Time,
) int {
	return NewComparator(log, ordering, preemptor, snapshot, now)(a, b)
}

// NewComparator returns a comparator function that compares two candidate workloads
// according to the configured ordering rules and UID tie-breaking. If ordering is empty,
// it defaults to:
// 1. Priority (Ascending: lowest priority first)
// 2. AdmissionTimestamp (Descending: most recently admitted first, protecting long-running workloads)
// 3. UID (Ascending: deterministic tie-breaker)
//
// Preemptor CQ and Cohort lookups are cached within the closure for efficiency.
func NewComparator(
	log logr.Logger,
	ordering []kueue.Order,
	preemptor *workload.Info,
	snapshot *schdcache.Snapshot,
	now time.Time,
) func(a, b *workload.Info) int {
	if len(ordering) == 0 {
		ordering = defaultOrdering
	}

	preemptorCQName := preemptor.ClusterQueue
	var preemptorCohort kueue.CohortReference
	var hasPreemptorCohort bool
	if cq := snapshot.ClusterQueue(preemptorCQName); cq != nil && cq.HasParent() {
		preemptorCohort = cq.Parent().GetName()
		hasPreemptorCohort = true
	}

	return func(a, b *workload.Info) int {
		if a == b {
			return 0
		}

		for _, order := range ordering {
			var res int
			switch order.OrderingField {
			case kueue.Priority:
				res = comparePriority(log, a, b)
			case kueue.AdmissionTimestamp:
				res = compareAdmissionTimestamp(a, b, now)
			case kueue.IsOtherCQ:
				res = compareIsOtherCQ(a, b, preemptorCQName)
			case kueue.IsOtherCohort:
				res = compareIsOtherCohort(a, b, preemptorCQName, preemptorCohort, hasPreemptorCohort, snapshot)
			}
			if res != 0 {
				if order.Direction == kueue.Descending {
					return -res
				}
				return res
			}
		}

		return compareUID(a, b)
	}
}

func comparePriority(log logr.Logger, a, b *workload.Info) int {
	prioA := priority.EffectivePriority(log, a.Obj)
	prioB := priority.EffectivePriority(log, b.Obj)
	return cmp.Compare(prioA, prioB)
}

func compareAdmissionTimestamp(a, b *workload.Info, now time.Time) int {
	timestampA := quotaReservationTime(a.Obj, now)
	timestampB := quotaReservationTime(b.Obj, now)
	return timestampA.Compare(timestampB)
}

func quotaReservationTime(wl *kueue.Workload, now time.Time) time.Time {
	cond := meta.FindStatusCondition(wl.Status.Conditions, kueue.WorkloadQuotaReserved)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return now
	}
	return cond.LastTransitionTime.Time
}

func compareIsOtherCQ(a, b *workload.Info, preemptorCQName kueue.ClusterQueueReference) int {
	isOtherA := a.ClusterQueue != preemptorCQName
	isOtherB := b.ClusterQueue != preemptorCQName
	return compareBool(isOtherA, isOtherB)
}

func compareIsOtherCohort(a, b *workload.Info, preemptorCQName kueue.ClusterQueueReference, preemptorCohort kueue.CohortReference, hasPreemptorCohort bool, snapshot *schdcache.Snapshot) int {
	isOtherCohortA := !isSameCohort(a, preemptorCQName, preemptorCohort, hasPreemptorCohort, snapshot)
	isOtherCohortB := !isSameCohort(b, preemptorCQName, preemptorCohort, hasPreemptorCohort, snapshot)
	return compareBool(isOtherCohortA, isOtherCohortB)
}

// compareBool performs standard mathematical comparison of two boolean values
// where false (0) is strictly less than true (1).
func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func isSameCohort(wl *workload.Info, preemptorCQName kueue.ClusterQueueReference, preemptorCohort kueue.CohortReference, hasPreemptorCohort bool, snapshot *schdcache.Snapshot) bool {
	if wl.ClusterQueue == preemptorCQName {
		return true
	}
	if !hasPreemptorCohort {
		return false
	}
	candCQ := snapshot.ClusterQueue(wl.ClusterQueue)
	if candCQ == nil || !candCQ.HasParent() {
		return false
	}
	return candCQ.Parent().GetName() == preemptorCohort
}

func compareUID(a, b *workload.Info) int {
	return cmp.Compare(a.Obj.UID, b.Obj.UID)
}
