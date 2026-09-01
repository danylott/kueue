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

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestRejectAllCandidateFilters(t *testing.T) {
	cases := map[string]struct {
		want CandidateFilters
	}{
		"RejectAllCandidateFilters returns RejectAll=true with empty filter slices": {
			want: CandidateFilters{
				RejectAll: true,
				CQFilters: nil,
				WLFilters: nil,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := RejectAllCandidateFilters()
			if got.RejectAll != tc.want.RejectAll {
				t.Errorf("RejectAllCandidateFilters() RejectAll = %v, want %v", got.RejectAll, tc.want.RejectAll)
			}
			if len(got.CQFilters) != 0 {
				t.Errorf("RejectAllCandidateFilters() CQFilters length = %d, want 0", len(got.CQFilters))
			}
			if len(got.WLFilters) != 0 {
				t.Errorf("RejectAllCandidateFilters() WLFilters length = %d, want 0", len(got.WLFilters))
			}
		})
	}
}

func TestRejectAllCQFilter_Matches(t *testing.T) {
	snapshot := newSnapshotBuilder().
		Cohort("rootA", "").
		Cohort("subA", "rootA").
		ClusterQueue("cqInRoot", "rootA").
		ClusterQueue("cqInSub", "subA").
		ClusterQueue("cqStandalone", "").
		Build()

	filter := NewRejectAllCQFilter()

	cases := map[string]struct {
		cq        *schdcache.ClusterQueueSnapshot
		wantMatch bool
	}{
		"standalone ClusterQueue rejected": {
			cq:        snapshot.ClusterQueue("cqStandalone"),
			wantMatch: false,
		},
		"ClusterQueue in root cohort rejected": {
			cq:        snapshot.ClusterQueue("cqInRoot"),
			wantMatch: false,
		},
		"ClusterQueue in sub-cohort rejected": {
			cq:        snapshot.ClusterQueue("cqInSub"),
			wantMatch: false,
		},
		"arbitrary ClusterQueue snapshot rejected": {
			cq:        &schdcache.ClusterQueueSnapshot{Name: "custom-cq"},
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := filter.Matches(tc.cq); got != tc.wantMatch {
				t.Errorf("RejectAllCQFilter.Matches() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestRejectAllWLFilter_Matches(t *testing.T) {
	filter := NewRejectAllWLFilter()

	cases := map[string]struct {
		candidate *workload.Info
		wantMatch bool
	}{
		"standard workload without labels rejected": {
			candidate: MakeWorkloadInfo("wl-plain", "ns").Obj(),
			wantMatch: false,
		},
		"workload with numeric labels and annotations rejected": {
			candidate: MakeWorkloadInfo("wl-labeled", "ns").
				Label("tpu-size", "8").
				Annotation("priority-boost", "10").
				Obj(),
			wantMatch: false,
		},
		"workload referencing WorkloadPriorityClass rejected": {
			candidate: MakeWorkloadInfo("wl-wpc", "ns").
				WorkloadPriorityClassRef("wpc-critical").
				Priority(100).
				Obj(),
			wantMatch: false,
		},
		"workload referencing standard PriorityClass rejected": {
			candidate: MakeWorkloadInfo("wl-pc", "ns").
				PodPriorityClassRef("high-priority").
				Obj(),
			wantMatch: false,
		},
		"workload assigned to local queue and cluster queue rejected": {
			candidate: MakeWorkloadInfo("wl-assigned", "ns-custom").Queue("lq1").ClusterQueue("cq1").Obj(),
			wantMatch: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := filter.Matches(tc.candidate); got != tc.wantMatch {
				t.Errorf("RejectAllWLFilter.Matches() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}
