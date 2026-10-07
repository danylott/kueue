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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName={preemplim}

// PreemptionLimit is the Schema for the preemptionlimits API
type PreemptionLimit struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of the PreemptionLimit.
	// +optional
	Spec PreemptionLimitSpec `json:"spec"`

	// status defines the observed state of the PreemptionLimit.
	// +optional
	Status PreemptionLimitStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PreemptionLimitList contains a list of PreemptionLimit
type PreemptionLimitList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PreemptionLimit `json:"items"`
}

// PreemptionLimitScope specifies the entity boundary for a preemption limit.
// Possible values are:
// - "Global": restricts the total number of preemption events across the entire cluster within a sliding time window.
// - "PreemptingClusterQueue": throttles preemptions triggered by workloads originating from a specific ClusterQueue.
// - "PreemptedClusterQueue": limits or blocks preemptions targeting workloads belonging to a specific ClusterQueue.
// - "PreemptedWorkload": restricts how many times an individual workload can be preempted within a given time window.
//
// +kubebuilder:validation:Enum=Global;PreemptingClusterQueue;PreemptedClusterQueue;PreemptedWorkload
type PreemptionLimitScope string

const (
	// GlobalPreemptionLimitScope restricts the total number of preemption events
	// across the entire cluster within a sliding time window.
	GlobalPreemptionLimitScope PreemptionLimitScope = "Global"

	// PreemptingCQLimitScope throttles preemptions triggered by workloads
	// originating from a specific ClusterQueue.
	PreemptingCQLimitScope PreemptionLimitScope = "PreemptingClusterQueue"

	// PreemptedCQLimitScope limits or blocks preemptions targeting workloads
	// belonging to a specific ClusterQueue.
	PreemptedCQLimitScope PreemptionLimitScope = "PreemptedClusterQueue"

	// PreemptedWorkloadLimitScope restricts how many times an individual workload
	// can be preempted within a given time window.
	PreemptedWorkloadLimitScope PreemptionLimitScope = "PreemptedWorkload"
)

// PreemptionLimitSpec defines the desired state of PreemptionLimit
type PreemptionLimitSpec struct {
	// scope specifies the entity boundary for this preemption limit.
	//
	// +required
	Scope PreemptionLimitScope `json:"scope,omitempty"`

	// configSelector selects PreemptionConfigs to which this limit applies.
	// If not set, it applies to all PreemptionConfigs.
	//
	// +optional
	ConfigSelector *metav1.LabelSelector `json:"configSelector,omitempty"`

	// clusterQueueSelector selects ClusterQueues to which this limit applies.
	// If not set, it applies to all ClusterQueues under the configured scope.
	//
	// +optional
	ClusterQueueSelector *metav1.LabelSelector `json:"clusterQueueSelector,omitempty"`

	// ruleNames restricts the limit to specific rule names within matching PreemptionConfigs.
	// If not set, it applies to all rules.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern="^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"
	RuleNames []string `json:"ruleNames,omitempty"`

	// limit defines how many preemption events can occur within the given time window.
	// An event is defined as a confirmed (preemptor, preemptee) eviction pair.
	// Setting limit to 0 blocks all preemptions under this limit's scope.
	//
	// +required
	// +kubebuilder:validation:Minimum=0
	Limit int32 `json:"limit"`

	// limitWindowSeconds specifies the sliding time window duration, in seconds.
	// Must be greater than or equal to 1 to prevent sub-second thrashing.
	//
	// +required
	// +kubebuilder:validation:Minimum=1
	LimitWindowSeconds int32 `json:"limitWindowSeconds,omitempty"`
}

// PreemptionLimitStatus defines the observed state of PreemptionLimit
type PreemptionLimitStatus struct {
	// counts is periodically updated, for reference only.
	// Restricted to the top 1000 counts to fit within CRD size limits.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=1000
	Counts []PreemptionLimitCount `json:"counts,omitempty"`
}

// PreemptionLimitCount tracks the number of preemptions for a single scoped entity.
type PreemptionLimitCount struct {
	// name identifies the scoped entity.
	// For Global scope it is "Global".
	// For PreemptingClusterQueue and PreemptedClusterQueue scopes it is the ClusterQueue name.
	// For PreemptedWorkload scope it is "<namespace>/<workload-name>".
	//
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Name string `json:"name"`

	// count is the number of preemptions recorded within the sliding time window.
	//
	// +required
	// +kubebuilder:validation:Minimum=0
	Count int32 `json:"count"`
}
