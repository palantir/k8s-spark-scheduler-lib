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
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"k8s.io/apimachinery/pkg/api/resource"
)

const CmpTolerance = 0.0001

func TestSinglePackingEfficiency(t *testing.T) {
	tests := []struct {
		name                     string
		nodeName                 string
		nodesSchedulingMetadata  resources.NodeSchedulingMetadata
		reservedResources        resources.NodeGroupResources
		expectedCPUEfficiency    float64
		expectedMemoryEfficiency float64
		expectedGPUEfficiency    float64
	}{{
		name:                     "packing efficiency calculated correctly for one node",
		nodeName:                 "n1",
		nodesSchedulingMetadata:  *createSchedulingMetadataWithTotals(6, 10, 8, 10, 1, 1, "zone1"),
		reservedResources:        createNodeReservedResources("n1", "1", "1", "1"),
		expectedCPUEfficiency:    0.5,
		expectedMemoryEfficiency: 0.3,
		expectedGPUEfficiency:    1.0,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := computePackingEfficiency(
				test.nodeName,
				test.nodesSchedulingMetadata,
				test.reservedResources)

			if math.Abs(test.expectedCPUEfficiency-p.CPU) > CmpTolerance {
				t.Fatalf("mismatch in expectedCPUEfficiency, expected: %v, got: %v", test.expectedCPUEfficiency, p.CPU)
			}

			if math.Abs(test.expectedMemoryEfficiency-p.Memory) > CmpTolerance {
				t.Fatalf("mismatch in expectedMemoryEfficiency, expected: %v, got: %v", test.expectedMemoryEfficiency, p.Memory)
			}

			if math.Abs(test.expectedGPUEfficiency-p.GPU) > CmpTolerance {
				t.Fatalf("mismatch in expectedGPUEfficiency, expected: %v, got: %v", test.expectedGPUEfficiency, p.GPU)
			}
		})
	}
}

func createNodeReservedResources(nodeName, cpu, memory, gpu string) resources.NodeGroupResources {
	reserved := make(resources.NodeGroupResources)
	reserved[nodeName] = createNodeResources(cpu, memory, gpu)
	return reserved
}

func createNodeResources(cpu, memory, gpu string) *resources.Resources {
	return &resources.Resources{
		CPU:       resource.MustParse(cpu),
		Memory:    resource.MustParse(memory),
		NvidiaGPU: resource.MustParse(gpu),
	}
}

func TestMultiPackingEfficiency(t *testing.T) {
	tests := []struct {
		name                         string
		nodesGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata
		reservedResources            resources.NodeGroupResources
		expectedCPUEfficiency        float64
		expectedMemoryEfficiency     float64
		expectedGPUEfficiency        float64
	}{{
		name: "packing efficiency calculated correctly for multiple nodes",
		nodesGroupSchedulingMetadata: resources.NodeGroupSchedulingMetadata(map[string]*resources.NodeSchedulingMetadata{
			"n1": createSchedulingMetadataWithTotals(10, 10, 10, 10, 2, 2, "zone1"),
			"n2": createSchedulingMetadataWithTotals(10, 10, 10, 10, 0, 0, "zone1"),
			"n3": createSchedulingMetadataWithTotals(10, 10, 10, 10, 2, 2, "zone1"),
		}),
		reservedResources: createReservedResources(
			[]string{"n1", "n2", "n3"},
			[]*resources.Resources{
				createNodeResources("5", "5", "2"),
				createNodeResources("2", "7", "0"),
				createNodeResources("9", "2", "1"),
			}),
		/*
			cpu: 0.5 0.2 0.9 -> 0.53
			mem: 0.5 0.7 0.2 -> 0.46
			gpu: 1.0 0.0 0.5 -> 0.75
		*/
		expectedCPUEfficiency:    0.533333,
		expectedMemoryEfficiency: 0.466666,
		expectedGPUEfficiency:    0.75,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			efficiencies := ComputePackingEfficiencies(
				test.nodesGroupSchedulingMetadata,
				test.reservedResources)

			avgEfficiency := ComputeAvgPackingEfficiency(test.nodesGroupSchedulingMetadata, efficiencies)

			if math.Abs(test.expectedCPUEfficiency-avgEfficiency.CPU) > CmpTolerance {
				t.Fatalf("mismatch in expectedCPUEfficiency, expected: %v, got: %v", test.expectedCPUEfficiency, avgEfficiency.CPU)
			}

			if math.Abs(test.expectedMemoryEfficiency-avgEfficiency.Memory) > CmpTolerance {
				t.Fatalf("mismatch in expectedMemoryEfficiency, expected: %v, got: %v", test.expectedMemoryEfficiency, avgEfficiency.Memory)
			}

			if math.Abs(test.expectedGPUEfficiency-avgEfficiency.GPU) > CmpTolerance {
				t.Fatalf("mismatch in expectedGPUEfficiency, expected: %v, got: %v", test.expectedGPUEfficiency, avgEfficiency.GPU)
			}
		})
	}
}

func createReservedResources(nodeNames []string, nodeResources []*resources.Resources) resources.NodeGroupResources {
	reserved := make(resources.NodeGroupResources)

	for i, nodeName := range nodeNames {
		reserved[nodeName] = nodeResources[i]
	}

	return reserved
}
