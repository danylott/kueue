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
	"strconv"

	"github.com/go-logr/logr"

	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/kueue/pkg/workload"
)

type numericLabelFilter struct {
	config kueuev1beta1.NumericLabelConstraint
}

// NewNumericLabelFilter creates a highly reusable CandidateSelector to evaluate candidate workloads
// based on customized integer labels.
func NewNumericLabelFilter(cfg kueuev1beta1.NumericLabelConstraint) CandidateSelector {
	return &numericLabelFilter{
		config: cfg,
	}
}

// Filter evaluates candidates against absolute bounds and relationship boundaries with the preemptor workload.
func (f *numericLabelFilter) Filter(ctx context.Context, log logr.Logger, preemptor *workload.Info, candidates []*workload.Info) []*workload.Info {
	var filtered []*workload.Info

	var preemptorVal int32
	if f.config.Relation != nil {
		preemptorVal = getLabelValueOrDefault(log.WithValues("workload", preemptor.Obj.Name, "role", "preemptor"), preemptor, f.config.Key, f.config.DefaultValue)
	}

	for _, candidate := range candidates {
		candidateVal := getLabelValueOrDefault(log.WithValues("workload", candidate.Obj.Name, "role", "candidate"), candidate, f.config.Key, f.config.DefaultValue)

		// 1. Check absolute bounds (MinValue, MaxValue)
		if f.config.MinValue != nil && candidateVal < *f.config.MinValue {
			continue
		}
		if f.config.MaxValue != nil && candidateVal > *f.config.MaxValue {
			continue
		}

		// 2. Check relation constraint compared to preemptor
		if f.config.Relation == nil {
			filtered = append(filtered, candidate)
			continue
		}

		switch *f.config.Relation {
		case kueuev1beta1.LowerOrEqual:
			if candidateVal <= preemptorVal {
				filtered = append(filtered, candidate)
			}
		case kueuev1beta1.Greater:
			if candidateVal > preemptorVal {
				filtered = append(filtered, candidate)
			}
		case kueuev1beta1.Lower:
			if candidateVal < preemptorVal {
				filtered = append(filtered, candidate)
			}
		case kueuev1beta1.GreaterOrEquals:
			if candidateVal >= preemptorVal {
				filtered = append(filtered, candidate)
			}
		default:
			// Fallback logic for unsupported or missing relations
			log.V(3).Info("Unsupported or unhandled relation constraint evaluated", "relation", *f.config.Relation)
			filtered = append(filtered, candidate)
		}
	}

	return filtered
}

// getLabelValueOrDefault safely extracts a numeric int32 label from a workload.
func getLabelValueOrDefault(log logr.Logger, wl *workload.Info, key string, def int32) int32 {
	if wl == nil || wl.Obj.Labels == nil {
		return def
	}
	valStr, exists := wl.Obj.Labels[key]
	if !exists {
		return def
	}
	val, err := strconv.ParseInt(valStr, 10, 32)
	if err != nil {
		log.V(3).Info("Failed to parse label into integer as expected; falling back to default", "key", key, "value", valStr, "default", def, "error", err)
		return def
	}
	return int32(val)
}
