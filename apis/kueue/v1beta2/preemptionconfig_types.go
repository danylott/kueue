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

package v1beta2

// RelativeConstraint defines how the preemptor compares to the candidate.
// Possible values are:
// - "Lower": permits preemption if candidate < preemptor
// - "Greater": permits preemption if candidate > preemptor
// - "LowerOrEqual": permits preemption if candidate <= preemptor
// - "GreaterOrEqual": permits preemption if candidate >= preemptor
// +kubebuilder:validation:Enum=Lower;Greater;LowerOrEqual;GreaterOrEqual
type RelativeConstraint string

const (
	// Lower permits preemption if candidate < preemptor
	Lower RelativeConstraint = "Lower"
	// Greater permits preemption if candidate > preemptor
	Greater RelativeConstraint = "Greater"
	// LowerOrEqual permits preemption if candidate <= preemptor
	LowerOrEqual RelativeConstraint = "LowerOrEqual"
	// GreaterOrEquals permits preemption if candidate >= preemptor
	GreaterOrEquals RelativeConstraint = "GreaterOrEqual"
)

// NumericLabelConstraint describes the configurations for filtering a numerical label.
// For example, this can be used to filter candidates based on topology domains, such as the
// "number of TPUs". If a preemptor requires a large topology, you can set Key="tpu-size"
// and Relation="LowerOrEqual", allowing it to preempt smaller workloads rather than disrupting
// other large topology workloads.
type NumericLabelConstraint struct {
	// Key is the label key that stores the integer value.
	Key string `json:"key"`
	// DefaultValue is used when a workload does not possess the label key.
	DefaultValue int32 `json:"defaultValue"`
	// Relation defines how the preemptor compares to the candidate.
	// +optional
	Relation *RelativeConstraint `json:"relation,omitempty"`
	// MinValue prevents preempting candidates with a label value strictly smaller than this.
	// +optional
	MinValue *int32 `json:"minValue,omitempty"`
	// MaxValue prevents preempting candidates with a label value strictly greater than this.
	// +optional
	MaxValue *int32 `json:"maxValue,omitempty"`
}
