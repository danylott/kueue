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
)

func TestMakeWorkloadInfo(t *testing.T) {
	info := MakeWorkloadInfo("wl1", "ns1").
		Queue("lq1").
		ClusterQueue("cq1").
		Label("tpu-size", "8").
		Annotation("priority-boost", "10").
		Priority(100).
		WorkloadPriorityClassRef("wpc-critical").
		Obj()

	if info == nil {
		t.Fatal("MakeWorkloadInfo().Obj() returned nil")
	}
	if info.Obj == nil {
		t.Fatal("MakeWorkloadInfo().Obj.Obj returned nil")
	}
	if info.Obj.Name != "wl1" || info.Obj.Namespace != "ns1" {
		t.Errorf("Unexpected workload name/ns: %s/%s, want wl1/ns1", info.Obj.Name, info.Obj.Namespace)
	}
	if info.ClusterQueue != "cq1" {
		t.Errorf("info.ClusterQueue = %s, want cq1", info.ClusterQueue)
	}
	if info.Obj.Spec.QueueName != "lq1" {
		t.Errorf("info.Obj.Spec.QueueName = %s, want lq1", info.Obj.Spec.QueueName)
	}
	if info.Obj.Labels["tpu-size"] != "8" {
		t.Errorf("info.Obj.Labels[tpu-size] = %s, want 8", info.Obj.Labels["tpu-size"])
	}
	if info.Obj.Annotations["priority-boost"] != "10" {
		t.Errorf("info.Obj.Annotations[priority-boost] = %s, want 10", info.Obj.Annotations["priority-boost"])
	}
	if info.Obj.Spec.Priority == nil || *info.Obj.Spec.Priority != 100 {
		t.Errorf("info.Obj.Spec.Priority = %v, want 100", info.Obj.Spec.Priority)
	}
	if info.Obj.Spec.PriorityClassRef == nil || info.Obj.Spec.PriorityClassRef.Name != "wpc-critical" {
		t.Errorf("info.Obj.Spec.PriorityClassRef = %v, want wpc-critical", info.Obj.Spec.PriorityClassRef)
	}

	infoPC := MakeWorkloadInfo("wl2", "ns1").
		PodPriorityClassRef("high-priority").
		Obj()
	if infoPC.Obj.Spec.PriorityClassRef == nil || infoPC.Obj.Spec.PriorityClassRef.Name != "high-priority" {
		t.Errorf("infoPC.Obj.Spec.PriorityClassRef = %v, want high-priority", infoPC.Obj.Spec.PriorityClassRef)
	}
}
