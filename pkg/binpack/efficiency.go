// Copyright (c) 2019 Palantir Technologies. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binpack

import (
	"math"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
)

// PackingEfficiency represents result packing efficiency per resource type. Computed as the total
// resources used divided by total capacity.
type PackingEfficiency struct {
	CPU    float64
	Memory float64
	GPU    float64
}

// LessThan compares two packing efficiencies. For a single packing we take the highest of the
// resources' efficiency. For example, when CPU is at 0.81 and Memory is at 0.54 the avg efficiency
// is 0.81. One packing efficiency is deemed less efficient when its avg efficiency is lower than
// the other's packing efficiency.
func (p *PackingEfficiency) LessThan(o PackingEfficiency) bool {
	// TODO: GPU is explicitly excluded for now but worthwhile to reconsider in future
	pMaxEfficiency := math.Max(p.CPU, p.Memory)
	oMaxEfficiency := math.Max(o.CPU, o.Memory)
	return pMaxEfficiency < oMaxEfficiency
}

// ComputePackingEfficiencies calculates utilization for all provided nodes, given the new reservation.
func ComputePackingEfficiencies(
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) (PackingEfficiency, []*PackingEfficiency) {

	var cpuSum, gpuSum, memorySum float64
	nodesWithGPU := 0
	nodeEfficiencies := make([]*PackingEfficiency, 0)

	for nodeName, nodeSchedulingMetadata := range nodeGroupSchedulingMetadata {
		nodeEfficiency := computePackingEfficiency(nodeName, *nodeSchedulingMetadata, reservedResources)

		cpuSum += nodeEfficiency.CPU
		memorySum += nodeEfficiency.Memory

		if nodeSchedulingMetadata.SchedulableResources.NvidiaGPU.Value() != 0 {
			gpuSum += nodeEfficiency.GPU
			nodesWithGPU++
		}
	}

	length := math.Max(float64(len(nodeGroupSchedulingMetadata)), 1)
	var gpuEfficiency float64
	if nodesWithGPU == 0 {
		gpuEfficiency = 1
	} else {
		gpuEfficiency = gpuSum / float64(nodesWithGPU)
	}

	avgEfficiency := PackingEfficiency{
		CPU:    cpuSum / length,
		Memory: memorySum / length,
		GPU:    gpuEfficiency,
	}

	return avgEfficiency, nodeEfficiencies
}

func computePackingEfficiency(
	nodeName string,
	nodeSchedulingMetadata resources.NodeSchedulingMetadata,
	reservedResources resources.NodeGroupResources) PackingEfficiency {

	nodeReservedResources := nodeSchedulingMetadata.SchedulableResources.Copy()
	nodeReservedResources.Sub(nodeSchedulingMetadata.AvailableResources)
	if reserved, ok := reservedResources[nodeName]; ok {
		nodeReservedResources.Add(reserved)
	}
	nodeSchedulableResources := nodeSchedulingMetadata.SchedulableResources

	// GPU treated differently because not every node has GPU
	gpuEfficiency := 0.0
	if nodeSchedulableResources.NvidiaGPU.Value() != 0 {
		gpuEfficiency = float64(nodeReservedResources.NvidiaGPU.Value()) / float64(normalizeResource(nodeSchedulableResources.NvidiaGPU.Value()))
	}

	return PackingEfficiency{
		CPU:    float64(nodeReservedResources.CPU.Value()) / float64(normalizeResource(nodeSchedulableResources.CPU.Value())),
		Memory: float64(nodeReservedResources.Memory.Value()) / float64(normalizeResource(nodeSchedulableResources.Memory.Value())),
		GPU:    gpuEfficiency,
	}
}

func normalizeResource(resourceValue int64) int64 {
	if resourceValue == 0 {
		return 1
	}
	return resourceValue
}
