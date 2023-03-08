package capacity

import (
	"math"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"gopkg.in/inf.v0"
	"k8s.io/apimachinery/pkg/api/resource"
)

type NodeAndExecutorCapacity struct {
	NodeName string
	Capacity int
}

// getCapacityAgainstSingleDimension computes how many times we can fit the required quantity within (available-reserved)
// e.g. if required = 4, available = 14, reserved = 1, we can fit 3 executors (3 * required <= available - reserved)
//
// This function is only useful to compare one dimension at a time, e.g. CPU or Memory, use GetNodeCapacity to account
// for all dimensions
func getCapacityAgainstSingleDimension(available, reserved, required resource.Quantity) int {
	if reserved.Cmp(available) == 1 {
		// ideally this shouldn't happen (reserved > available), but should this happen, let's be resilient
		return 0
	}

	if required.IsZero() {
		// if we don't require any resources for this dimension, then we can fit an infinite number of executors
		return math.MaxInt
	}

	// this basically computes: floor((available - reserved) / required)
	return int(new(inf.Dec).QuoRound(
		new(inf.Dec).Sub(available.AsDec(), reserved.AsDec()),
		required.AsDec(),
		0,
		inf.RoundFloor,
	).UnscaledBig().Int64())
}

func GetNodeCapacity(available, reserved, singleExecutor *resources.Resources) int {
	capacityConsideringCPUOnly := getCapacityAgainstSingleDimension(
		available.CPU,
		reserved.CPU,
		singleExecutor.CPU,
	)
	capacityConsideringMemoryOnly := getCapacityAgainstSingleDimension(
		available.Memory,
		reserved.Memory,
		singleExecutor.Memory,
	)
	capacityConsideringNvidiaGPUOnly := getCapacityAgainstSingleDimension(
		available.NvidiaGPU,
		reserved.NvidiaGPU,
		singleExecutor.NvidiaGPU,
	)

	return min(capacityConsideringCPUOnly, capacityConsideringMemoryOnly, capacityConsideringNvidiaGPUOnly)
}

// GetNodeCapacities' return value is ordered according to nodePriorityOrder
func GetNodeCapacities(
	nodePriorityOrder []string,
	nodeGroupSchedulingMetadata resources.NodeGroupSchedulingMetadata,
	reservedResources resources.NodeGroupResources,
	singleExecutor *resources.Resources,
) []NodeAndExecutorCapacity {
	capacities := make([]NodeAndExecutorCapacity, 0, len(nodePriorityOrder))

	for _, nodeName := range nodePriorityOrder {
		if nodeSchedulingMetadata, ok := nodeGroupSchedulingMetadata[nodeName]; ok {
			reserved := resources.Zero()

			if alreadyReserved, ok := reservedResources[nodeName]; ok {
				reserved = alreadyReserved
			}

			capacities = append(capacities, NodeAndExecutorCapacity{
				nodeName,
				GetNodeCapacity(nodeSchedulingMetadata.AvailableResources, reserved, singleExecutor),
			})
		}
	}

	return capacities
}

func FilterOutNodesWithoutCapacity(capacities []NodeAndExecutorCapacity) []NodeAndExecutorCapacity {
	filteredCapacities := make([]NodeAndExecutorCapacity, 0, len(capacities))
	for _, nodeWithCapacity := range capacities {
		if nodeWithCapacity.Capacity > 0 {
			filteredCapacities = append(filteredCapacities, nodeWithCapacity)
		}
	}
	return filteredCapacities
}

func min(a, b, c int) int {
	if a <= b && a <= c {
		return a
	} else if b <= c {
		return b
	}
	return c
}
