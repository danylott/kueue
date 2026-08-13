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

package selectors

import (
	"context"
	"slices"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestNumericLabelFilter(t *testing.T) {
	cases := map[string]struct {
		config     kueuev1beta1.NumericLabelConstraint
		preemptor  *workload.Info
		candidates []*workload.Info
		wantNames  []string
	}{
		"candidate less than or equal to preemptor": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "tpu-size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"tpu-size": "4"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c3", "").Labels(map[string]string{"tpu-size": "16"}).Obj()),
			},
			wantNames: []string{"c1", "c2"},
		},
		"candidate less than preemptor": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "tpu-size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.Lower),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"tpu-size": "4"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c3", "").Labels(map[string]string{"tpu-size": "16"}).Obj()),
			},
			wantNames: []string{"c1"},
		},
		"candidate greater than preemptor": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "tpu-size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"tpu-size": "4"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c3", "").Labels(map[string]string{"tpu-size": "16"}).Obj()),
			},
			wantNames: []string{"c3"},
		},
		"candidate greater than or equal to preemptor": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "tpu-size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.GreaterOrEquals),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"tpu-size": "4"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"tpu-size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c3", "").Labels(map[string]string{"tpu-size": "16"}).Obj()),
			},
			wantNames: []string{"c2", "c3"},
		},
		"candidate uses default value when label key is missing": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 4,
				Relation:     ptr.To(kueuev1beta1.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "2"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"other-key": "123"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "1"}).Obj()),
			},
			wantNames: []string{"c1"},
		},
		"candidate uses default value when label value is empty string": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 4,
				Relation:     ptr.To(kueuev1beta1.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "2"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": ""}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "1"}).Obj()),
			},
			wantNames: []string{"c1"},
		},
		"malformed labels fallback to default": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 2,
				Relation:     ptr.To(kueuev1beta1.LowerOrEqual),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "invalid"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "invalid-candidate"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "4"}).Obj()),
			},
			wantNames: []string{"c1"},
		},
		"reject candidates below min value": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.LowerOrEqual),
				MinValue:     ptr.To[int32](4),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "16"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "2"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "8"}).Obj()),
			},
			wantNames: []string{"c2"},
		},
		"reject candidates above max value": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.LowerOrEqual),
				MaxValue:     ptr.To[int32](10),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "32"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "16"}).Obj()),
			},
			wantNames: []string{"c1"},
		},
		"unsupported relation constraint permits all falling back": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 1,
				Relation:     ptr.To[kueuev1beta1.RelativeConstraint]("Unsupported"),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "2"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "4"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "8"}).Obj()),
			},
			wantNames: []string{"c1", "c2"},
		},
		"full rejection resulting in empty slice": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 1,
				Relation:     ptr.To(kueuev1beta1.Greater),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "32"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "8"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "16"}).Obj()),
			},
			wantNames: []string{},
		},
		"no relation combined with min value filters candidates": {
			config: kueuev1beta1.NumericLabelConstraint{
				Key:          "size",
				DefaultValue: 1,
				MinValue:     ptr.To[int32](4),
			},
			preemptor: workload.NewInfo(utiltesting.MakeWorkload("p", "").Labels(map[string]string{"size": "2"}).Obj()),
			candidates: []*workload.Info{
				workload.NewInfo(utiltesting.MakeWorkload("c1", "").Labels(map[string]string{"size": "2"}).Obj()),
				workload.NewInfo(utiltesting.MakeWorkload("c2", "").Labels(map[string]string{"size": "8"}).Obj()),
			},
			wantNames: []string{"c2"},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			filter := NewNumericLabelFilter(tc.config)
			got := filter.Filter(context.Background(), testr.New(t), tc.preemptor, tc.candidates)

			var wantCandidates []*workload.Info
			for _, c := range tc.candidates {
				if slices.Contains(tc.wantNames, c.Obj.Name) {
					wantCandidates = append(wantCandidates, c)
				}
			}

			if diff := cmp.Diff(wantCandidates, got); diff != "" {
				t.Errorf("Unexpected filtered candidates (-want +got):\n%s", diff)
			}
		})
	}
}
