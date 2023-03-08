package capacity

import (
	"math"
	"testing"

	"github.com/palantir/k8s-spark-scheduler-lib/pkg/resources"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMin(t *testing.T) {
	assert.Equal(t, 1, min(1, 2, 3))
	assert.Equal(t, 1, min(2, 1, 3))
	assert.Equal(t, 1, min(2, 3, 1))
}

func TestGetCapacityAgainstSingleDimension(t *testing.T) {
	assert.Equal(t, math.MaxInt, getCapacityAgainstSingleDimension(
		*resource.NewQuantity(2, resource.DecimalSI),
		*resource.NewQuantity(1, resource.DecimalSI),
		*resource.NewQuantity(0, resource.DecimalSI),
	))
	assert.Equal(t, 2, getCapacityAgainstSingleDimension(
		*resource.NewQuantity(2, resource.DecimalSI),
		*resource.NewQuantity(0, resource.DecimalSI),
		*resource.NewQuantity(1, resource.DecimalSI),
	))
	assert.Equal(t, 1, getCapacityAgainstSingleDimension(
		*resource.NewQuantity(3, resource.DecimalSI),
		*resource.NewQuantity(1, resource.DecimalSI),
		*resource.NewQuantity(2, resource.DecimalSI),
	))
	assert.Equal(t, 0, getCapacityAgainstSingleDimension(
		*resource.NewQuantity(2, resource.DecimalSI),
		*resource.NewQuantity(3, resource.DecimalSI),
		*resource.NewQuantity(1, resource.DecimalSI),
	))
}

func TestGetNodeCapacity(t *testing.T) {
	singleExecutor := &resources.Resources{
		CPU:       *resource.NewQuantity(1, resource.DecimalSI),
		Memory:    *resource.NewQuantity(1, resource.DecimalSI),
		NvidiaGPU: *resource.NewQuantity(1, resource.DecimalSI),
	}

	tests := []struct {
		name           string
		available      *resources.Resources
		reserved       *resources.Resources
		singleExecutor *resources.Resources
		expected       int
	}{{
		name:           "no available resources",
		available:      &resources.Resources{},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       0,
	}, {
		name:           "available resources fit exactly",
		available:      singleExecutor,
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       1,
	}, {
		name: "capacity is limited by cpu",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(3, resource.DecimalSI),
			Memory:    *resource.NewQuantity(4, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(4, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name: "capacity is limited by memory",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(4, resource.DecimalSI),
			Memory:    *resource.NewQuantity(3, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(4, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name: "capacity is limited by gpu",
		available: &resources.Resources{
			CPU:       *resource.NewQuantity(4, resource.DecimalSI),
			Memory:    *resource.NewQuantity(4, resource.DecimalSI),
			NvidiaGPU: *resource.NewQuantity(3, resource.DecimalSI),
		},
		reserved:       resources.Zero(),
		singleExecutor: singleExecutor,
		expected:       3,
	}, {
		name:      "does not fit due to existing reserved resources",
		available: singleExecutor,
		reserved: &resources.Resources{
			CPU: *resource.NewQuantity(1, resource.DecimalSI),
		},
		singleExecutor: singleExecutor,
		expected:       0,
	},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, GetNodeCapacity(test.available, test.reserved, test.singleExecutor))
		})
	}
}
