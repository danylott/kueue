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
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestCompareCandidates(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.PriorityBoost, true)

	now := time.Now()
	t1 := now.Add(-10 * time.Minute)
	t2 := now.Add(-5 * time.Minute)
	t3 := now.Add(-1 * time.Minute)

	baseWorkload := func(name, uid, cq string) *workload.Info {
		wl := utiltestingapi.MakeWorkload(name, "ns").
			UID(types.UID(uid)).
			Obj()
		info := workload.NewInfo(wl)
		info.ClusterQueue = kueue.ClusterQueueReference(cq)
		return info
	}

	withPriority := func(info *workload.Info, p int32) *workload.Info {
		info.Obj.Spec.Priority = &p
		return info
	}

	withPriorityBoost := func(info *workload.Info, boost string) *workload.Info {
		if info.Obj.Annotations == nil {
			info.Obj.Annotations = make(map[string]string)
		}
		info.Obj.Annotations[controllerconstants.PriorityBoostAnnotationKey] = boost
		return info
	}

	withReservationTime := func(info *workload.Info, tm time.Time) *workload.Info {
		info.Obj.Status.Conditions = append(info.Obj.Status.Conditions, metav1.Condition{
			Type:               kueue.WorkloadQuotaReserved,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: tm},
		})
		return info
	}

	ctx, log := utiltesting.ContextWithLog(t)

	setupSnapshot := func(t *testing.T) *schdcache.Snapshot {
		cl := utiltesting.NewClientBuilder().Build()
		cqCache := schdcache.New(cl)
		cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())

		// Cohort structure:
		// rootA
		//  ├── cq-a1
		//  ├── cq-a3 (sibling in rootA)
		//  └── subA
		//       └── cq-a2
		// rootB
		//  └── cq-b1
		// standalone-cq (no cohort)

		rootA := utiltestingapi.MakeCohort("rootA").Obj()
		subA := utiltestingapi.MakeCohort("subA").Parent("rootA").Obj()
		rootB := utiltestingapi.MakeCohort("rootB").Obj()

		_ = cqCache.AddOrUpdateCohort(rootA)
		_ = cqCache.AddOrUpdateCohort(subA)
		_ = cqCache.AddOrUpdateCohort(rootB)

		cqA1 := utiltestingapi.MakeClusterQueue("cq-a1").Cohort("rootA").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
		cqA3 := utiltestingapi.MakeClusterQueue("cq-a3").Cohort("rootA").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
		cqA2 := utiltestingapi.MakeClusterQueue("cq-a2").Cohort("subA").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
		cqB1 := utiltestingapi.MakeClusterQueue("cq-b1").Cohort("rootB").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
		cqStandalone := utiltestingapi.MakeClusterQueue("standalone-cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()

		_ = cqCache.AddClusterQueue(ctx, cqA1)
		_ = cqCache.AddClusterQueue(ctx, cqA3)
		_ = cqCache.AddClusterQueue(ctx, cqA2)
		_ = cqCache.AddClusterQueue(ctx, cqB1)
		_ = cqCache.AddClusterQueue(ctx, cqStandalone)

		snap, err := cqCache.Snapshot(ctx)
		if err != nil {
			t.Fatalf("Failed to create snapshot: %v", err)
		}
		return snap
	}

	snap := setupSnapshot(t)
	preemptor := baseWorkload("preemptor", "p-uid", "cq-a1")

	tests := []struct {
		name      string
		ordering  []kueue.Order
		a         *workload.Info
		b         *workload.Info
		preemptor *workload.Info
		snapshot  *schdcache.Snapshot
		want      int // negative: a < b, positive: a > b, zero: a == b
	}{
		{
			name:      "same workload identity comparison returns 0",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority}},
			a:         preemptor,
			b:         preemptor,
			preemptor: preemptor,
			snapshot:  snap,
			want:      0,
		},
		{
			name:      "Priority Ascending (default): lower priority comes first (a < b)",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority}},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 20),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "Priority Ascending (default): lower priority comes first (a > b)",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority}},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 100),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 50),
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "Priority Ascending (explicit): lower priority comes first",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority, Direction: kueue.Ascending}},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 20),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "Priority Descending: higher priority comes first (a < b)",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority, Direction: kueue.Descending}},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 20),
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "Priority Descending: higher priority comes first (a > b)",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority, Direction: kueue.Descending}},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 100),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 50),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "Priority Ascending: priority boost is respected",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority}},
			a:         withPriorityBoost(withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10), "100"), // effective 110
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 50),                           // effective 50
			preemptor: preemptor,
			snapshot:  snap,
			want:      1, // b has lower priority, so b comes first
		},
		{
			name:      "Priority Descending: priority boost is respected",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority, Direction: kueue.Descending}},
			a:         withPriorityBoost(withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10), "100"), // effective 110
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 50),                           // effective 50
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // a has higher priority (110 vs 50), so a comes first
		},
		{
			name:      "AdmissionTimestamp Ascending (default): older admission comes first (t1 < t3)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp}},
			a:         withReservationTime(baseWorkload("a", "same-uid", "cq-a1"), t1), // older
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t3), // more recent
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "AdmissionTimestamp Ascending (explicit): older admission comes first (t3 > t1)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Ascending}},
			a:         withReservationTime(baseWorkload("a", "same-uid", "cq-a1"), t3), // more recent
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t1), // older
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "AdmissionTimestamp Descending: more recently admitted comes first (t3 > t1)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending}},
			a:         withReservationTime(baseWorkload("a", "same-uid", "cq-a1"), t3), // more recent
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t1), // older
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "AdmissionTimestamp Descending: older comes after (t1 < t2)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending}},
			a:         withReservationTime(baseWorkload("a", "same-uid", "cq-a1"), t1), // older
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t2), // more recent
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "AdmissionTimestamp Ascending: missing condition falls back to now (older t1 < now)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp}},
			a:         baseWorkload("a", "same-uid", "cq-a1"),                          // now
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t1), // older
			preemptor: preemptor,
			snapshot:  snap,
			want:      1, // b is older (t1 < now) -> b comes first (a > b)
		},
		{
			name:      "AdmissionTimestamp Descending: missing condition falls back to now (now > older t1)",
			ordering:  []kueue.Order{{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending}},
			a:         baseWorkload("a", "same-uid", "cq-a1"),                          // now
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t1), // older
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // a was just admitted (now > t1) -> a comes first
		},
		{
			name:     "AdmissionTimestamp: ConditionFalse treated as unreserved (now)",
			ordering: []kueue.Order{{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending}},
			a: func() *workload.Info {
				wl := baseWorkload("a", "same-uid", "cq-a1")
				wl.Obj.Status.Conditions = append(wl.Obj.Status.Conditions, metav1.Condition{
					Type:               kueue.WorkloadQuotaReserved,
					Status:             metav1.ConditionFalse,
					LastTransitionTime: metav1.Time{Time: t1},
				})
				return wl
			}(),
			b:         withReservationTime(baseWorkload("b", "same-uid", "cq-a1"), t1), // True condition at t1
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // a has False condition -> treated as now (now > t1) -> a comes first in Descending
		},
		{
			name:      "IsOtherCQ Ascending (default): other CQ before same CQ",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCQ}},
			a:         baseWorkload("a", "same-uid", "cq-b1"), // other CQ
			b:         baseWorkload("b", "same-uid", "cq-a1"), // same CQ as preemptor
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "IsOtherCQ Ascending (default): same CQ after other CQ",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCQ}},
			a:         baseWorkload("a", "same-uid", "cq-a1"), // same CQ as preemptor
			b:         baseWorkload("b", "same-uid", "cq-b1"), // other CQ
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "IsOtherCQ Descending: same CQ before other CQ",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCQ, Direction: kueue.Descending}},
			a:         baseWorkload("a", "same-uid", "cq-a1"), // same CQ as preemptor
			b:         baseWorkload("b", "same-uid", "cq-b1"), // other CQ
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "IsOtherCQ Descending: other CQ after same CQ",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCQ, Direction: kueue.Descending}},
			a:         baseWorkload("a", "same-uid", "cq-b1"), // other CQ
			b:         baseWorkload("b", "same-uid", "cq-a1"), // same CQ as preemptor
			preemptor: preemptor,
			snapshot:  snap,
			want:      1,
		},
		{
			name:      "IsOtherCQ: both other CQs tie-break to UID",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCQ}},
			a:         baseWorkload("a", "uid-1", "cq-b1"), // other CQ
			b:         baseWorkload("b", "uid-2", "cq-a2"), // other CQ
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // "uid-1" < "uid-2"
		},
		{
			name:      "IsOtherCohort Ascending (default): other cohort before same cohort (rootB vs rootA)",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCohort}},
			a:         baseWorkload("a", "same-uid", "cq-b1"), // rootB (other cohort)
			b:         baseWorkload("b", "same-uid", "cq-a1"), // rootA (same cohort as preemptor)
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "IsOtherCohort Ascending (default): same cohort (cq-a3 under rootA) after other cohort (standalone)",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCohort}},
			a:         baseWorkload("a", "same-uid", "cq-a3"),         // sibling CQ under rootA -> same cohort rootA
			b:         baseWorkload("b", "same-uid", "standalone-cq"), // no cohort -> other cohort
			preemptor: preemptor,
			snapshot:  snap,
			want:      1, // b is other cohort, so b comes first (a > b)
		},
		{
			name:      "IsOtherCohort Descending: same cohort before other cohort (rootA vs rootB)",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCohort, Direction: kueue.Descending}},
			a:         baseWorkload("a", "same-uid", "cq-a1"), // rootA (same cohort)
			b:         baseWorkload("b", "same-uid", "cq-b1"), // rootB (other cohort)
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name:      "IsOtherCohort Descending: other cohort after same cohort (standalone vs cq-a3)",
			ordering:  []kueue.Order{{OrderingField: kueue.IsOtherCohort, Direction: kueue.Descending}},
			a:         baseWorkload("a", "same-uid", "standalone-cq"), // no cohort -> other cohort
			b:         baseWorkload("b", "same-uid", "cq-a3"),         // sibling CQ under rootA -> same cohort rootA
			preemptor: preemptor,
			snapshot:  snap,
			want:      1, // b is same cohort, so b comes first (a > b)
		},
		{
			name:      "Deterministic Tie-breaker: UID comparison when ordering fields are equal",
			ordering:  []kueue.Order{{OrderingField: kueue.Priority}},
			a:         withPriority(baseWorkload("a", "uid-aaa", "cq-a1"), 100),
			b:         withPriority(baseWorkload("b", "uid-zzz", "cq-a1"), 100),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // "uid-aaa" < "uid-zzz"
		},
		{
			name:      "Deterministic Tie-breaker: UID comparison when ordering is empty",
			ordering:  []kueue.Order{},
			a:         baseWorkload("a", "uid-2", "cq-a1"),
			b:         baseWorkload("b", "uid-1", "cq-a1"),
			preemptor: preemptor,
			snapshot:  snap,
			want:      1, // "uid-2" > "uid-1"
		},
		{
			name: "Multi-key chain: Priority (Ascending) -> AdmissionTimestamp (Descending) -> IsOtherCQ (Ascending)",
			ordering: []kueue.Order{
				{OrderingField: kueue.Priority, Direction: kueue.Ascending},
				{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending},
				{OrderingField: kueue.IsOtherCQ, Direction: kueue.Ascending},
			},
			// Equal priority, different admission timestamp -> more recent admission timestamp wins in Descending
			a:         withReservationTime(withPriority(baseWorkload("a", "uid-9", "cq-a1"), 100), t3),
			b:         withReservationTime(withPriority(baseWorkload("b", "uid-1", "cq-b1"), 100), t1),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // a is more recently admitted (t3 > t1)
		},
		{
			name: "Multi-key chain: equal priority and admission time falls back to IsOtherCQ",
			ordering: []kueue.Order{
				{OrderingField: kueue.Priority},
				{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending},
				{OrderingField: kueue.IsOtherCQ},
			},
			a:         withReservationTime(withPriority(baseWorkload("a", "uid-9", "cq-b1"), 100), t2), // other CQ
			b:         withReservationTime(withPriority(baseWorkload("b", "uid-1", "cq-a1"), 100), t2), // same CQ
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // a is other CQ
		},
		{
			name: "Multi-key chain: Priority Descending -> IsOtherCQ Ascending",
			ordering: []kueue.Order{
				{OrderingField: kueue.Priority, Direction: kueue.Descending},
				{OrderingField: kueue.IsOtherCQ, Direction: kueue.Ascending},
			},
			// a has priority 100 (same CQ), b has priority 50 (other CQ) -> a comes first because priority is Descending
			a:         withPriority(baseWorkload("a", "uid-1", "cq-a1"), 100),
			b:         withPriority(baseWorkload("b", "uid-2", "cq-b1"), 50),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name: "Unknown ordering field is ignored",
			ordering: []kueue.Order{
				{OrderingField: "UnknownField"},
				{OrderingField: kueue.Priority},
			},
			a:         withPriority(baseWorkload("a", "same-uid", "cq-a1"), 10),
			b:         withPriority(baseWorkload("b", "same-uid", "cq-a1"), 20),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1,
		},
		{
			name: "4-key ordering chain: IsOtherCohort -> IsOtherCQ -> Priority -> AdmissionTimestamp",
			ordering: []kueue.Order{
				{OrderingField: kueue.IsOtherCohort},
				{OrderingField: kueue.IsOtherCQ},
				{OrderingField: kueue.Priority},
				{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending},
			},
			// a is in same cohort but different CQ, lower priority (10)
			// b is in same cohort, same CQ, higher priority (100)
			a:         withReservationTime(withPriority(baseWorkload("a", "same-uid", "cq-a3"), 10), t1),
			b:         withReservationTime(withPriority(baseWorkload("b", "same-uid", "cq-a1"), 100), t3),
			preemptor: preemptor,
			snapshot:  snap,
			want:      -1, // both same cohort (rootA), but a is other CQ (cq-a3 vs cq-a1) -> a comes first
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CompareCandidates(log, tc.ordering, tc.a, tc.b, tc.preemptor, tc.snapshot, now)
			if (tc.want < 0 && got >= 0) || (tc.want > 0 && got <= 0) || (tc.want == 0 && got != 0) {
				t.Errorf("CompareCandidates() = %d, want sign matching %d", got, tc.want)
			}
		})
	}
}

func TestCandidateSortingWithMultiKeyComparator(t *testing.T) {
	now := time.Now()
	tOld := now.Add(-10 * time.Minute)
	tNew := now.Add(-1 * time.Minute)

	wl := func(name string, priority int32, tm time.Time, cq string, uid string) *workload.Info {
		obj := utiltestingapi.MakeWorkload(name, "ns").
			UID(types.UID(uid)).
			Priority(priority).
			Obj()
		obj.Status.Conditions = append(obj.Status.Conditions, metav1.Condition{
			Type:               kueue.WorkloadQuotaReserved,
			Status:             metav1.ConditionTrue,
			LastTransitionTime: metav1.Time{Time: tm},
		})
		info := workload.NewInfo(obj)
		info.ClusterQueue = kueue.ClusterQueueReference(cq)
		return info
	}

	w1 := wl("w1-prio10-old", 10, tOld, "cq-a", "uid-1")
	w2 := wl("w2-prio10-new", 10, tNew, "cq-a", "uid-2")
	w3 := wl("w3-prio20-new", 20, tNew, "cq-a", "uid-3")
	w4 := wl("w4-prio20-other-cq", 20, tNew, "cq-b", "uid-4")

	ctx, log := utiltesting.ContextWithLog(t)
	cl := utiltesting.NewClientBuilder().Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	cqA := utiltestingapi.MakeClusterQueue("cq-a").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	cqB := utiltestingapi.MakeClusterQueue("cq-b").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	_ = cqCache.AddClusterQueue(ctx, cqA)
	_ = cqCache.AddClusterQueue(ctx, cqB)
	snap, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	preemptor := wl("preemptor", 100, now, "cq-a", "uid-p")

	// Ordering: Priority (Ascending) -> AdmissionTimestamp (Descending) -> IsOtherCQ (Ascending)
	cmpFunc := NewComparator(
		log,
		[]kueue.Order{
			{OrderingField: kueue.Priority, Direction: kueue.Ascending},
			{OrderingField: kueue.AdmissionTimestamp, Direction: kueue.Descending},
			{OrderingField: kueue.IsOtherCQ, Direction: kueue.Ascending},
		},
		preemptor,
		snap,
		now,
	)

	list := []*workload.Info{w4, w3, w1, w2}
	slices.SortFunc(list, cmpFunc)

	// Expected order:
	// Priority 10 first:
	//   Between w1 and w2: w2 is more recent (tNew > tOld) in Descending timestamp -> w2, then w1
	// Priority 20 next:
	//   Between w3 (same CQ) and w4 (other CQ): both tNew, w4 is other CQ -> w4, then w3
	wantNames := []string{"w2-prio10-new", "w1-prio10-old", "w4-prio20-other-cq", "w3-prio20-new"}
	var gotNames []string
	for _, item := range list {
		gotNames = append(gotNames, item.Obj.Name)
	}

	if diff := cmp.Diff(wantNames, gotNames); diff != "" {
		t.Errorf("Sorted order mismatch (-want +got):\n%s", diff)
	}
}

func TestDeepHierarchicalCohortComparator(t *testing.T) {
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now()

	cl := utiltesting.NewClientBuilder().Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())

	// Cohort Tree: 4 levels deep
	// Level 0: root
	//   └── Level 1: level1
	//         └── Level 2: level2
	//               └── Level 3: level3
	//                     ├── cq-deep-1 (cohort: level3)
	//                     └── cq-deep-2 (cohort: level3, sibling in level3)
	//   ├── cq-root (cohort: root)
	//   └── Level 1: level1-sibling (cohort: root)
	//         └── cq-sibling-branch (cohort: level1-sibling)
	// standalone-cq (no cohort)

	root := utiltestingapi.MakeCohort("root").Obj()
	lvl1 := utiltestingapi.MakeCohort("level1").Parent("root").Obj()
	lvl2 := utiltestingapi.MakeCohort("level2").Parent("level1").Obj()
	lvl3 := utiltestingapi.MakeCohort("level3").Parent("level2").Obj()
	lvl1Sib := utiltestingapi.MakeCohort("level1-sibling").Parent("root").Obj()

	_ = cqCache.AddOrUpdateCohort(root)
	_ = cqCache.AddOrUpdateCohort(lvl1)
	_ = cqCache.AddOrUpdateCohort(lvl2)
	_ = cqCache.AddOrUpdateCohort(lvl3)
	_ = cqCache.AddOrUpdateCohort(lvl1Sib)

	cqDeep1 := utiltestingapi.MakeClusterQueue("cq-deep-1").Cohort("level3").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	cqDeep2 := utiltestingapi.MakeClusterQueue("cq-deep-2").Cohort("level3").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	cqRoot := utiltestingapi.MakeClusterQueue("cq-root").Cohort("root").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	cqSibBranch := utiltestingapi.MakeClusterQueue("cq-sibling-branch").Cohort("level1-sibling").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()
	cqStandalone := utiltestingapi.MakeClusterQueue("standalone-cq").
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj()).Obj()

	_ = cqCache.AddClusterQueue(ctx, cqDeep1)
	_ = cqCache.AddClusterQueue(ctx, cqDeep2)
	_ = cqCache.AddClusterQueue(ctx, cqRoot)
	_ = cqCache.AddClusterQueue(ctx, cqSibBranch)
	_ = cqCache.AddClusterQueue(ctx, cqStandalone)

	snap, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	createWl := func(name, cq, uid string) *workload.Info {
		wl := utiltestingapi.MakeWorkload(name, "ns").
			UID(types.UID(uid)).
			Obj()
		info := workload.NewInfo(wl)
		info.ClusterQueue = kueue.ClusterQueueReference(cq)
		return info
	}

	preemptor := createWl("preemptor", "cq-deep-1", "p-uid")
	wlSameCQ := createWl("wl-same-cq", "cq-deep-1", "uid-1")
	wlSameCohortSibling := createWl("wl-same-cohort-sib", "cq-deep-2", "uid-2")
	wlRootCQ := createWl("wl-root-cq", "cq-root", "uid-3")
	wlSibBranchCQ := createWl("wl-sib-branch-cq", "cq-sibling-branch", "uid-4")
	wlStandalone := createWl("wl-standalone", "standalone-cq", "uid-5")

	t.Run("IsOtherCohort Ascending", func(t *testing.T) {
		cmpFunc := NewComparator(log, []kueue.Order{{OrderingField: kueue.IsOtherCohort, Direction: kueue.Ascending}}, preemptor, snap, now)

		candidates := []*workload.Info{wlSameCQ, wlRootCQ, wlSameCohortSibling, wlStandalone, wlSibBranchCQ}
		slices.SortFunc(candidates, cmpFunc)

		// In IsOtherCohort Ascending: other cohort comes before same cohort.
		// Other cohort: wlRootCQ, wlStandalone, wlSibBranchCQ (tied on IsOtherCohort=true -> ordered by UID)
		// Same cohort: wlSameCQ, wlSameCohortSibling (tied on IsOtherCohort=false -> ordered by UID)
		// UID order for other cohort: uid-3 (wlRootCQ), uid-4 (wlSibBranchCQ), uid-5 (wlStandalone)
		// UID order for same cohort: uid-1 (wlSameCQ), uid-2 (wlSameCohortSibling)
		wantNames := []string{"wl-root-cq", "wl-sib-branch-cq", "wl-standalone", "wl-same-cq", "wl-same-cohort-sib"}
		var gotNames []string
		for _, c := range candidates {
			gotNames = append(gotNames, c.Obj.Name)
		}

		if diff := cmp.Diff(wantNames, gotNames); diff != "" {
			t.Errorf("Deep cohort hierarchy sorting mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("IsOtherCohort Descending", func(t *testing.T) {
		cmpFunc := NewComparator(log, []kueue.Order{{OrderingField: kueue.IsOtherCohort, Direction: kueue.Descending}}, preemptor, snap, now)

		candidates := []*workload.Info{wlRootCQ, wlSameCQ, wlStandalone, wlSameCohortSibling, wlSibBranchCQ}
		slices.SortFunc(candidates, cmpFunc)

		// In IsOtherCohort Descending: same cohort comes before other cohort.
		// Same cohort: wlSameCQ (uid-1), wlSameCohortSibling (uid-2)
		// Other cohort: wlRootCQ (uid-3), wlSibBranchCQ (uid-4), wlStandalone (uid-5)
		wantNames := []string{"wl-same-cq", "wl-same-cohort-sib", "wl-root-cq", "wl-sib-branch-cq", "wl-standalone"}
		var gotNames []string
		for _, c := range candidates {
			gotNames = append(gotNames, c.Obj.Name)
		}

		if diff := cmp.Diff(wantNames, gotNames); diff != "" {
			t.Errorf("Deep cohort hierarchy Descending mismatch (-want +got):\n%s", diff)
		}
	})
}
