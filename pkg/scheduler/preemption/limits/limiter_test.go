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

package limits

import (
	"cmp"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	gocmp "github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestNewPreemptionLimiter(t *testing.T) {
	config := utiltestingalpha.MakePreemptionConfig("config").Label("tier", "gold").Obj()

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		want             []policy.PreemptionLimitReference
		// wantErr, when set, is the expected prefix of the error message.
		wantErr string
	}{
		"limits applying to the config are kept by name, the others are dropped": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				*utiltestingalpha.MakePreemptionLimit("any-config", kueuealpha.GlobalPreemptionLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("silver", kueuealpha.GlobalPreemptionLimitScope).
					ConfigSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"tier": "silver"}}).Obj(),
			},
			want: []policy.PreemptionLimitReference{"any-config"},
		},
		"invalid limit is an error": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				*utiltestingalpha.MakePreemptionLimit("any-config", kueuealpha.GlobalPreemptionLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("invalid", kueuealpha.GlobalPreemptionLimitScope).
					ConfigSelector(&metav1.LabelSelector{
						MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tier", Operator: "invalid"}},
					}).Obj(),
			},
			wantErr: `preemptionLimit "invalid" configSelector: `,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			limiter, err := NewPreemptionLimiter(logr.Discard(), tc.preemptionLimits, config, nil, nil, time.Time{})
			if tc.wantErr != "" {
				if err == nil || !strings.HasPrefix(err.Error(), tc.wantErr) {
					t.Fatalf("NewPreemptionLimiter() error = %v, want error starting with %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewPreemptionLimiter() unexpected error: %v", err)
			}
			got := slices.Sorted(maps.Keys(limiter.matchers))
			if diff := gocmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected limits (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestPreemptionLimiterConsumedLimits(t *testing.T) {
	preemptorCQ := &schdcache.ClusterQueueSnapshot{Name: "preempting-cq", Labels: map[string]string{"side": "preempting"}}
	targetCQ := &schdcache.ClusterQueueSnapshot{Name: "preempted-cq", Labels: map[string]string{"side": "preempted"}}
	preemptor := &workload.Info{Obj: utiltestingapi.MakeWorkload("preemptor", "ns").Obj(), ClusterQueue: "preempting-cq"}
	target := &workload.Info{Obj: utiltestingapi.MakeWorkload("target", "ns").UID("target").Obj(), ClusterQueue: "preempted-cq"}
	globalLimit := *utiltestingalpha.MakePreemptionLimit("global", kueuealpha.GlobalPreemptionLimitScope).Obj()

	cases := map[string]struct {
		preemptionLimits []kueuealpha.PreemptionLimit
		// target, when set, replaces the default target.
		target *workload.Info
		// unobservedEviction marks the eviction of the target as issued but not observed yet.
		unobservedEviction bool
		nilLimiter         bool
		want               policy.ConsumedLimits
	}{
		"no limits consume nothing": {},
		"limits matching the ClusterQueues are consumed together, the others are not": {
			preemptionLimits: []kueuealpha.PreemptionLimit{
				globalLimit,
				*utiltestingalpha.MakePreemptionLimit("preempted-workload", kueuealpha.PreemptedWorkloadLimitScope).Obj(),
				*utiltestingalpha.MakePreemptionLimit("other-cqs", kueuealpha.PreemptedCQLimitScope).
					ClusterQueueSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"side": "other"}}).Obj(),
			},
			want: policy.ConsumedLimits{"global": "Global", "preempted-workload": "ns/target"},
		},
		"evicted target consumes nothing": {
			preemptionLimits: []kueuealpha.PreemptionLimit{globalLimit},
			target: &workload.Info{
				Obj:          utiltestingapi.MakeWorkload("target", "ns").UID("target").EvictedAt(time.Now()).Obj(),
				ClusterQueue: "preempted-cq",
			},
		},
		"target with an unobserved eviction consumes nothing": {
			preemptionLimits:   []kueuealpha.PreemptionLimit{globalLimit},
			unobservedEviction: true,
		},
		"nil limiter consumes nothing": {
			preemptionLimits: []kueuealpha.PreemptionLimit{globalLimit},
			nilLimiter:       true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			log := logr.Discard()
			preemptionExpectations := preemptexpectations.New()
			if tc.unobservedEviction {
				preemptionExpectations.ExpectUIDs(log, client.ObjectKeyFromObject(target.Obj), []types.UID{target.Obj.UID})
			}
			limiter, err := NewPreemptionLimiter(log, tc.preemptionLimits, utiltestingalpha.MakePreemptionConfig("config").Obj(), nil, preemptionExpectations, time.Time{})
			if err != nil {
				t.Fatalf("NewPreemptionLimiter() unexpected error: %v", err)
			}
			if tc.nilLimiter {
				limiter = nil
			}
			got := limiter.consumedLimits(preemptor, cmp.Or(tc.target, target), preemptorCQ, targetCQ)
			if diff := gocmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected consumed limits (-want,+got):\n%s", diff)
			}
		})
	}
}
