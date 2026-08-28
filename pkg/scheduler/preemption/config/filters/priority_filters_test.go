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

	"github.com/go-logr/logr"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"

	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestWorkloadPriorityFilter_Matches(t *testing.T) {
	cases := map[string]struct {
		relation          kueuev1beta2.RelativeConstraint
		preemptorPriority *int32
		candidatePriority *int32
		wantMatch         bool
	}{
		"Lower: candidate priority strictly lower matches": {
			relation:          kueuev1beta2.Lower,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"Lower: candidate priority equal rejected": {
			relation:          kueuev1beta2.Lower,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"Lower: candidate priority strictly greater rejected": {
			relation:          kueuev1beta2.Lower,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         false,
		},
		"LowerOrEqual: candidate priority strictly lower matches": {
			relation:          kueuev1beta2.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         true,
		},
		"LowerOrEqual: candidate priority equal matches": {
			relation:          kueuev1beta2.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"LowerOrEqual: candidate priority strictly greater rejected": {
			relation:          kueuev1beta2.LowerOrEqual,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         false,
		},
		"Greater: candidate priority strictly greater matches": {
			relation:          kueuev1beta2.Greater,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"Greater: candidate priority equal rejected": {
			relation:          kueuev1beta2.Greater,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         false,
		},
		"Greater: candidate priority strictly lower rejected": {
			relation:          kueuev1beta2.Greater,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
		"GreaterOrEqual: candidate priority strictly greater matches": {
			relation:          kueuev1beta2.GreaterOrEquals,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](150),
			wantMatch:         true,
		},
		"GreaterOrEqual: candidate priority equal matches": {
			relation:          kueuev1beta2.GreaterOrEquals,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](100),
			wantMatch:         true,
		},
		"GreaterOrEqual: candidate priority strictly lower rejected": {
			relation:          kueuev1beta2.GreaterOrEquals,
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
		"Default priority handling: nil preemptor priority defaults to 0 and matches strictly lower candidate": {
			relation:          kueuev1beta2.Lower,
			preemptorPriority: nil,
			candidatePriority: ptr.To[int32](-10),
			wantMatch:         true,
		},
		"Default priority handling: nil candidate priority defaults to 0 and matches when equal": {
			relation:          kueuev1beta2.LowerOrEqual,
			preemptorPriority: ptr.To[int32](0),
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Default priority handling: both nil priorities compare as equal (0 vs 0)": {
			relation:          kueuev1beta2.LowerOrEqual,
			preemptorPriority: nil,
			candidatePriority: nil,
			wantMatch:         true,
		},
		"Negative priorities: candidate -100 is Lower than preemptor -50": {
			relation:          kueuev1beta2.Lower,
			preemptorPriority: ptr.To[int32](-50),
			candidatePriority: ptr.To[int32](-100),
			wantMatch:         true,
		},
		"Negative priorities: candidate -50 is Greater than preemptor -100": {
			relation:          kueuev1beta2.Greater,
			preemptorPriority: ptr.To[int32](-100),
			candidatePriority: ptr.To[int32](-50),
			wantMatch:         true,
		},
		"Unknown/unsupported relation constraint rejects all candidates": {
			relation:          kueuev1beta2.RelativeConstraint("InvalidRelation"),
			preemptorPriority: ptr.To[int32](100),
			candidatePriority: ptr.To[int32](50),
			wantMatch:         false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			preemptorBuilder := utiltestingapi.MakeWorkload("preemptor", "ns")
			if tc.preemptorPriority != nil {
				preemptorBuilder = preemptorBuilder.Priority(*tc.preemptorPriority)
			}
			preemptor := workload.NewInfo(preemptorBuilder.Obj())

			candBuilder := utiltestingapi.MakeWorkload("candidate", "ns")
			if tc.candidatePriority != nil {
				candBuilder = candBuilder.Priority(*tc.candidatePriority)
			}
			candidate := workload.NewInfo(candBuilder.Obj())

			filter := NewWorkloadPriorityFilter(logr.Discard(), tc.relation, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}

func TestWorkloadPriorityFilter_PriorityBoost(t *testing.T) {
	cases := map[string]struct {
		featureGates      map[featuregate.Feature]bool
		relation          kueuev1beta2.RelativeConstraint
		preemptorPriority int32
		preemptorBoost    string
		candidatePriority int32
		candidateBoost    string
		wantMatch         bool
	}{
		"PriorityBoost enabled: candidate boost raises effective priority above preemptor": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueuev1beta2.Greater,
			preemptorPriority: 50,
			candidatePriority: 10,
			candidateBoost:    "100", // effective priority: 10 + 100 = 110 > 50
			wantMatch:         true,
		},
		"PriorityBoost enabled: preemptor boost raises effective priority above candidate": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueuev1beta2.Lower,
			preemptorPriority: 50,
			preemptorBoost:    "100", // effective priority: 50 + 100 = 150
			candidatePriority: 120,   // effective priority: 120 < 150
			wantMatch:         true,
		},
		"PriorityBoost enabled: candidate negative boost lowers effective priority below preemptor": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueuev1beta2.Lower,
			preemptorPriority: 100,
			candidatePriority: 120,
			candidateBoost:    "-50", // effective priority: 120 - 50 = 70 < 100
			wantMatch:         true,
		},
		"PriorityBoost disabled: boost annotation is ignored and base priority is used": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: false},
			relation:          kueuev1beta2.Greater,
			preemptorPriority: 50,
			candidatePriority: 10,
			candidateBoost:    "100", // ignored -> effective priority is base 10 (not > 50)
			wantMatch:         false,
		},
		"PriorityBoost enabled: invalid boost annotation on candidate safely defaults to base priority": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueuev1beta2.Lower,
			preemptorPriority: 100,
			candidatePriority: 50,
			candidateBoost:    "not-a-number", // parse fails -> defaults to base 50 < 100
			wantMatch:         true,
		},
		"PriorityBoost enabled: invalid boost annotation on preemptor safely defaults to base priority": {
			featureGates:      map[featuregate.Feature]bool{features.PriorityBoost: true},
			relation:          kueuev1beta2.Lower,
			preemptorPriority: 100,
			preemptorBoost:    "invalid", // parse fails -> defaults to base 100
			candidatePriority: 50,        // 50 < 100
			wantMatch:         true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)

			preemptorBuilder := utiltestingapi.MakeWorkload("preemptor", "ns").Priority(tc.preemptorPriority)
			if tc.preemptorBoost != "" {
				preemptorBuilder = preemptorBuilder.Annotation(controllerconstants.PriorityBoostAnnotationKey, tc.preemptorBoost)
			}
			preemptor := workload.NewInfo(preemptorBuilder.Obj())

			candBuilder := utiltestingapi.MakeWorkload("candidate", "ns").Priority(tc.candidatePriority)
			if tc.candidateBoost != "" {
				candBuilder = candBuilder.Annotation(controllerconstants.PriorityBoostAnnotationKey, tc.candidateBoost)
			}
			candidate := workload.NewInfo(candBuilder.Obj())

			filter := NewWorkloadPriorityFilter(logr.Discard(), tc.relation, preemptor)
			if got := filter.Matches(candidate); got != tc.wantMatch {
				t.Errorf("Matches() = %v, want %v", got, tc.wantMatch)
			}
		})
	}
}
