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
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

type mockCQFilter struct {
	allowedNames map[string]bool
}

func (m *mockCQFilter) Matches(cq *schdcache.ClusterQueueSnapshot) bool {
	return cq != nil && m.allowedNames[string(cq.Name)]
}

type mockWLFilter struct {
	allowedNames map[string]bool
}

func (m *mockWLFilter) Matches(wl *workload.Info) bool {
	return wl != nil && wl.Obj != nil && m.allowedNames[wl.Obj.Name]
}

func TestCandidateFilters_MatchesCQ(t *testing.T) {
	cq1 := &schdcache.ClusterQueueSnapshot{Name: "cq1"}
	cq2 := &schdcache.ClusterQueueSnapshot{Name: "cq2"}

	cases := map[string]struct {
		filters   []ClusterQueueFilter
		targetCQ  *schdcache.ClusterQueueSnapshot
		wantMatch bool
	}{
		"empty CQFilters matches any ClusterQueue": {
			filters:   nil,
			targetCQ:  cq1,
			wantMatch: true,
		},
		"single filter matching target CQ": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true}},
			},
			targetCQ:  cq1,
			wantMatch: true,
		},
		"single filter rejecting target CQ": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true}},
			},
			targetCQ:  cq2,
			wantMatch: false,
		},
		"multiple filters with all matching target CQ": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true, "cq2": true}},
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true}},
			},
			targetCQ:  cq1,
			wantMatch: true,
		},
		"multiple filters with one rejecting target CQ": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true, "cq2": true}},
				&mockCQFilter{allowedNames: map[string]bool{"cq2": true}},
			},
			targetCQ:  cq1,
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cf := &CandidateFilters{CQFilters: tc.filters}
			if got := cf.MatchesCQ(tc.targetCQ); got != tc.wantMatch {
				t.Errorf("MatchesCQ() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestCandidateFilters_FilterClusterQueues(t *testing.T) {
	cq1 := &schdcache.ClusterQueueSnapshot{Name: "cq1"}
	cq2 := &schdcache.ClusterQueueSnapshot{Name: "cq2"}
	cq3 := &schdcache.ClusterQueueSnapshot{Name: "cq3"}
	allCQs := []*schdcache.ClusterQueueSnapshot{cq1, cq2, cq3}

	cases := map[string]struct {
		filters   []ClusterQueueFilter
		inputCQs  []*schdcache.ClusterQueueSnapshot
		wantNames []string
		wantSame  bool
	}{
		"empty CQFilters returns input slice directly with zero allocations": {
			filters:   nil,
			inputCQs:  allCQs,
			wantNames: []string{"cq1", "cq2", "cq3"},
			wantSame:  true,
		},
		"single matching CQFilter": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true, "cq3": true}},
			},
			inputCQs:  allCQs,
			wantNames: []string{"cq1", "cq3"},
		},
		"multiple CQFilters evaluate logical AND": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true, "cq2": true}},
				&mockCQFilter{allowedNames: map[string]bool{"cq2": true, "cq3": true}},
			},
			inputCQs:  allCQs,
			wantNames: []string{"cq2"},
		},
		"disjoint CQFilters result in empty slice": {
			filters: []ClusterQueueFilter{
				&mockCQFilter{allowedNames: map[string]bool{"cq1": true}},
				&mockCQFilter{allowedNames: map[string]bool{"cq3": true}},
			},
			inputCQs:  allCQs,
			wantNames: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cf := &CandidateFilters{CQFilters: tc.filters}
			gotCQs := cf.FilterClusterQueues(tc.inputCQs)

			if tc.wantSame && !slices.Equal(gotCQs, tc.inputCQs) {
				t.Errorf("FilterClusterQueues did not return identical input slice instance")
			}

			var gotNames []string
			for _, cq := range gotCQs {
				gotNames = append(gotNames, string(cq.Name))
			}
			if diff := cmp.Diff(tc.wantNames, gotNames, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("FilterClusterQueues mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCandidateFilters_MatchesWorkload(t *testing.T) {
	wl1 := workload.NewInfo(utiltestingapi.MakeWorkload("wl1", "ns1").Obj())
	wl2 := workload.NewInfo(utiltestingapi.MakeWorkload("wl2", "ns1").Obj())

	cases := map[string]struct {
		filters   []WorkloadFilter
		targetWL  *workload.Info
		wantMatch bool
	}{
		"empty WLFilters matches any workload": {
			filters:   nil,
			targetWL:  wl1,
			wantMatch: true,
		},
		"single filter matching target workload": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true}},
			},
			targetWL:  wl1,
			wantMatch: true,
		},
		"single filter rejecting target workload": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true}},
			},
			targetWL:  wl2,
			wantMatch: false,
		},
		"multiple filters with all matching target workload": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true, "wl2": true}},
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true}},
			},
			targetWL:  wl1,
			wantMatch: true,
		},
		"multiple filters with one rejecting target workload": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true, "wl2": true}},
				&mockWLFilter{allowedNames: map[string]bool{"wl2": true}},
			},
			targetWL:  wl1,
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cf := &CandidateFilters{WLFilters: tc.filters}
			if got := cf.MatchesWorkload(tc.targetWL); got != tc.wantMatch {
				t.Errorf("MatchesWorkload() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestCandidateFilters_FilterWorkloads(t *testing.T) {
	wl1 := workload.NewInfo(utiltestingapi.MakeWorkload("wl1", "ns1").Obj())
	wl2 := workload.NewInfo(utiltestingapi.MakeWorkload("wl2", "ns1").Obj())
	wl3 := workload.NewInfo(utiltestingapi.MakeWorkload("wl3", "ns1").Obj())
	allWLs := []*workload.Info{wl1, wl2, wl3}

	cases := map[string]struct {
		filters   []WorkloadFilter
		inputWLs  []*workload.Info
		wantNames []string
		wantSame  bool
	}{
		"empty WLFilters returns input slice directly with zero allocations": {
			filters:   nil,
			inputWLs:  allWLs,
			wantNames: []string{"wl1", "wl2", "wl3"},
			wantSame:  true,
		},
		"single matching WLFilter": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true, "wl3": true}},
			},
			inputWLs:  allWLs,
			wantNames: []string{"wl1", "wl3"},
		},
		"multiple WLFilters evaluate logical AND": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true, "wl2": true}},
				&mockWLFilter{allowedNames: map[string]bool{"wl2": true, "wl3": true}},
			},
			inputWLs:  allWLs,
			wantNames: []string{"wl2"},
		},
		"disjoint WLFilters result in empty slice": {
			filters: []WorkloadFilter{
				&mockWLFilter{allowedNames: map[string]bool{"wl1": true}},
				&mockWLFilter{allowedNames: map[string]bool{"wl3": true}},
			},
			inputWLs:  allWLs,
			wantNames: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cf := &CandidateFilters{WLFilters: tc.filters}
			gotWLs := cf.FilterWorkloads(tc.inputWLs)

			if tc.wantSame && !slices.Equal(gotWLs, tc.inputWLs) {
				t.Errorf("FilterWorkloads did not return identical input slice instance")
			}

			var gotNames []string
			for _, wl := range gotWLs {
				gotNames = append(gotNames, wl.Obj.Name)
			}
			if diff := cmp.Diff(tc.wantNames, gotNames, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("FilterWorkloads mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
