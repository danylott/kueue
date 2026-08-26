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
	"testing"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/hierarchy"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func newTestCohortSnapshot(name kueuev1beta2.CohortReference) *schdcache.CohortSnapshot {
	return &schdcache.CohortSnapshot{
		Name:   name,
		Cohort: hierarchy.NewCohort[*schdcache.ClusterQueueSnapshot, *schdcache.CohortSnapshot](),
	}
}

func buildTestSnapshot(
	cqs map[kueuev1beta2.ClusterQueueReference]kueuev1beta2.CohortReference,
	cohorts map[kueuev1beta2.CohortReference]kueuev1beta2.CohortReference,
) *schdcache.Snapshot {
	mgr := hierarchy.NewManager(newTestCohortSnapshot)
	for cohortName := range cohorts {
		mgr.AddCohort(cohortName)
	}
	for cqName := range cqs {
		mgr.AddClusterQueue(&schdcache.ClusterQueueSnapshot{
			Name: cqName,
		})
	}
	for cohortName, parentCohort := range cohorts {
		if parentCohort != "" {
			mgr.UpdateCohortEdge(cohortName, parentCohort)
		}
	}
	for cqName, parentCohort := range cqs {
		if parentCohort != "" {
			mgr.UpdateClusterQueueEdge(cqName, parentCohort)
		}
	}
	return &schdcache.Snapshot{
		Manager: mgr,
	}
}

func makeWorkloadInfo(name, namespace string, localQueue kueuev1beta2.LocalQueueName, clusterQueue kueuev1beta2.ClusterQueueReference) *workload.Info {
	wl := utiltestingapi.MakeWorkload(name, namespace).Queue(localQueue).Obj()
	info := workload.NewInfo(wl)
	info.ClusterQueue = clusterQueue
	return info
}

func TestClusterQueueRelationFilters(t *testing.T) {
	// Hierarchy topology:
	//           rootA (Root Cohort)                         rootB (Root Cohort)
	//         /   |         \                                      |
	// cqDirectRoot subA1      subA2                               subB
	//            /  |   \      |                                   |
	//         cq1  cq2  subSubA cq3                               cq4
	//                     |
	//                   cqDeep
	//
	// Standalone CQs (no cohort):
	// - cqStandalone1
	// - cqStandalone2
	snapshot := buildTestSnapshot(
		map[kueuev1beta2.ClusterQueueReference]kueuev1beta2.CohortReference{
			"cq1":           "subA1",
			"cq2":           "subA1",
			"cqDeep":        "subSubA",
			"cqDirectRoot":  "rootA",
			"cq3":           "subA2",
			"cq4":           "subB",
			"cqStandalone1": "",
			"cqStandalone2": "",
		},
		map[kueuev1beta2.CohortReference]kueuev1beta2.CohortReference{
			"rootA":   "",
			"subA1":   "rootA",
			"subSubA": "subA1",
			"subA2":   "rootA",
			"rootB":   "",
			"subB":    "rootB",
		},
	)

	cq1 := snapshot.ClusterQueue("cq1")
	cq2 := snapshot.ClusterQueue("cq2")
	cqDeep := snapshot.ClusterQueue("cqDeep")
	cqDirectRoot := snapshot.ClusterQueue("cqDirectRoot")
	cq3 := snapshot.ClusterQueue("cq3")
	cq4 := snapshot.ClusterQueue("cq4")
	cqStandalone1 := snapshot.ClusterQueue("cqStandalone1")
	cqStandalone2 := snapshot.ClusterQueue("cqStandalone2")

	cases := map[string]struct {
		filter      ClusterQueueFilter
		candidateCQ *schdcache.ClusterQueueSnapshot
		wantMatch   bool
	}{
		// 1. SameClusterQueue Filter Tests
		"SameClusterQueue: matching target CQ": {
			filter:      NewSameClusterQueueFilter("cq1"),
			candidateCQ: cq1,
			wantMatch:   true,
		},
		"SameClusterQueue: different sibling CQ in same cohort rejected": {
			filter:      NewSameClusterQueueFilter("cq1"),
			candidateCQ: cq2,
			wantMatch:   false,
		},
		"SameClusterQueue: different CQ in disjoint tree rejected": {
			filter:      NewSameClusterQueueFilter("cq1"),
			candidateCQ: cq4,
			wantMatch:   false,
		},
		"SameClusterQueue: standalone CQ matches itself": {
			filter:      NewSameClusterQueueFilter("cqStandalone1"),
			candidateCQ: cqStandalone1,
			wantMatch:   true,
		},
		"SameClusterQueue: standalone CQ rejects another standalone CQ": {
			filter:      NewSameClusterQueueFilter("cqStandalone1"),
			candidateCQ: cqStandalone2,
			wantMatch:   false,
		},

		// 2. SameCohort Filter Tests
		"SameCohort: sibling CQ in same immediate parent cohort matches": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cq2,
			wantMatch:   true,
		},
		"SameCohort: candidate in exact same CQ as preemptor matches": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cq1,
			wantMatch:   true,
		},
		"SameCohort: sub-cohort child under same immediate parent rejected": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cqDeep,
			wantMatch:   false,
		},
		"SameCohort: sibling sub-cohort under same root rejected": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cq3,
			wantMatch:   false,
		},
		"SameCohort: direct child of root cohort rejected when preemptor is in sub-cohort": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cqDirectRoot,
			wantMatch:   false,
		},
		"SameCohort: candidate in disjoint cohort tree rejected": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cq4,
			wantMatch:   false,
		},
		"SameCohort: standalone candidate rejected for preemptor with cohort": {
			filter:      NewSameCohortFilter("cq1", snapshot),
			candidateCQ: cqStandalone1,
			wantMatch:   false,
		},
		"SameCohort: preemptor directly under root matches itself": {
			filter:      NewSameCohortFilter("cqDirectRoot", snapshot),
			candidateCQ: cqDirectRoot,
			wantMatch:   true,
		},
		"SameCohort: preemptor directly under root rejects sub-cohort child": {
			filter:      NewSameCohortFilter("cqDirectRoot", snapshot),
			candidateCQ: cq1,
			wantMatch:   false,
		},
		"SameCohort: standalone preemptor matches candidate in its own CQ": {
			filter:      NewSameCohortFilter("cqStandalone1", snapshot),
			candidateCQ: cqStandalone1,
			wantMatch:   true,
		},
		"SameCohort: standalone preemptor rejects candidate in another standalone CQ": {
			filter:      NewSameCohortFilter("cqStandalone1", snapshot),
			candidateCQ: cqStandalone2,
			wantMatch:   false,
		},
		"SameCohort: standalone preemptor rejects candidate in a cohort CQ": {
			filter:      NewSameCohortFilter("cqStandalone1", snapshot),
			candidateCQ: cq1,
			wantMatch:   false,
		},

		// 3. SameCohortTree Filter Tests
		"SameCohortTree: sibling CQ in same immediate cohort matches": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cq2,
			wantMatch:   true,
		},
		"SameCohortTree: candidate in sibling sub-cohort sharing root matches": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cq3,
			wantMatch:   true,
		},
		"SameCohortTree: candidate in 3-level deep sub-cohort sharing root matches": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cqDeep,
			wantMatch:   true,
		},
		"SameCohortTree: candidate directly under root cohort matches": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cqDirectRoot,
			wantMatch:   true,
		},
		"SameCohortTree: candidate in exact same CQ as preemptor matches": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cq1,
			wantMatch:   true,
		},
		"SameCohortTree: candidate in disjoint cohort tree rejected": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cq4,
			wantMatch:   false,
		},
		"SameCohortTree: preemptor in rootB matches itself": {
			filter:      NewSameCohortTreeFilter("cq4", snapshot),
			candidateCQ: cq4,
			wantMatch:   true,
		},
		"SameCohortTree: preemptor in rootB rejects candidate in rootA": {
			filter:      NewSameCohortTreeFilter("cq4", snapshot),
			candidateCQ: cq1,
			wantMatch:   false,
		},
		"SameCohortTree: standalone candidate rejected for preemptor with cohort tree": {
			filter:      NewSameCohortTreeFilter("cq1", snapshot),
			candidateCQ: cqStandalone1,
			wantMatch:   false,
		},
		"SameCohortTree: standalone preemptor matches candidate in its own CQ": {
			filter:      NewSameCohortTreeFilter("cqStandalone1", snapshot),
			candidateCQ: cqStandalone1,
			wantMatch:   true,
		},
		"SameCohortTree: standalone preemptor rejects candidate in another standalone CQ": {
			filter:      NewSameCohortTreeFilter("cqStandalone1", snapshot),
			candidateCQ: cqStandalone2,
			wantMatch:   false,
		},
		"SameCohortTree: standalone preemptor rejects candidate in cohort tree": {
			filter:      NewSameCohortTreeFilter("cqStandalone1", snapshot),
			candidateCQ: cq1,
			wantMatch:   false,
		},

		// 4. RejectAllCQFilter Tests
		"RejectAllCQFilter: rejects candidate in cohort": {
			filter:      NewRejectAllCQFilter(),
			candidateCQ: cq1,
			wantMatch:   false,
		},
		"RejectAllCQFilter: rejects candidate in disjoint cohort": {
			filter:      NewRejectAllCQFilter(),
			candidateCQ: cq4,
			wantMatch:   false,
		},
		"RejectAllCQFilter: rejects candidate in deep sub-cohort": {
			filter:      NewRejectAllCQFilter(),
			candidateCQ: cqDeep,
			wantMatch:   false,
		},
		"RejectAllCQFilter: rejects direct root candidate": {
			filter:      NewRejectAllCQFilter(),
			candidateCQ: cqDirectRoot,
			wantMatch:   false,
		},
		"RejectAllCQFilter: rejects standalone candidate": {
			filter:      NewRejectAllCQFilter(),
			candidateCQ: cqStandalone1,
			wantMatch:   false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotMatch := tc.filter.Matches(tc.candidateCQ)
			if gotMatch != tc.wantMatch {
				t.Errorf("Matches() = %v, want %v", gotMatch, tc.wantMatch)
			}
		})
	}
}

func TestSameLocalQueueFilter(t *testing.T) {
	filter := NewSameLocalQueueFilter("ns1", "lq1")

	cases := map[string]struct {
		candidate *workload.Info
		wantMatch bool
	}{
		"matching namespace and queue name": {
			candidate: makeWorkloadInfo("c-exact", "ns1", "lq1", "cq1"),
			wantMatch: true,
		},
		"different local queue name rejected": {
			candidate: makeWorkloadInfo("c-diff-lq", "ns1", "lq2", "cq1"),
			wantMatch: false,
		},
		"different namespace rejected": {
			candidate: makeWorkloadInfo("c-diff-ns", "ns2", "lq1", "cq1"),
			wantMatch: false,
		},
		"different namespace and queue name rejected": {
			candidate: makeWorkloadInfo("c-diff-both", "ns2", "lq2", "cq1"),
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gotMatch := filter.Matches(tc.candidate)
			if gotMatch != tc.wantMatch {
				t.Errorf("Matches() = %v, want %v", gotMatch, tc.wantMatch)
			}
		})
	}
}
