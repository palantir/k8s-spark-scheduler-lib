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

// ComputePackingEfficiency calculates average utilization across provided nodes, given the new reservation.
func ComputePackingEfficiency(
	nodesSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources) PackingEfficiency {

	var cpuSum, gpuSum, memorySum float64
	nodesWithGPU := 0

	for nodeName, nodesSchedulingMetadata := range nodesSchedulingMetadata {
		nodeReservedResources := nodesSchedulingMetadata.AvailableResources.Copy()
		if reserved, ok := reservedResources[nodeName]; ok {
			nodeReservedResources.Add(reserved)
		}
		nodeSchedulableResources := nodesSchedulingMetadata.SchedulableResources

		cpuSum += float64(nodeReservedResources.CPU.Value()) / float64(normalizeResource(nodeSchedulableResources.CPU.Value()))
		memorySum += float64(nodeReservedResources.Memory.Value()) / float64(normalizeResource(nodeSchedulableResources.Memory.Value()))
		// GPU treated differently because not every bin has GPU
		if nodeSchedulableResources.NvidiaGPU.Value() != 0 {
			gpuSum += float64(nodeSchedulableResources.NvidiaGPU.Value()) / float64(normalizeResource(nodeSchedulableResources.NvidiaGPU.Value()))
			nodesWithGPU++
		}
	}

	length := math.Max(float64(len(nodesSchedulingMetadata)), 1)
	var gpuEfficiency float64
	if nodesWithGPU == 0 {
		gpuEfficiency = 1
	} else {
		gpuEfficiency = gpuSum / float64(nodesWithGPU)
	}

	return PackingEfficiency{
		CPU:    cpuSum / length,
		Memory: memorySum / length,
		GPU:    gpuEfficiency,
	}
}

func normalizeResource(resourceValue int64) int64 {
	if resourceValue == 0 {
		return 1
	}
	return resourceValue
}
