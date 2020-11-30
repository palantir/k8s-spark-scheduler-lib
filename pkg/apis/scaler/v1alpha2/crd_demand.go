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

package v1alpha2

import (
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// DemandPhaseEmpty is the state of a demand object when it is first created
	DemandPhaseEmpty string = ""
	// DemandPhasePending is the state a demand object is in when Scaler has acknowledged it but has not yet taken
	// action to fulfill the demand
	DemandPhasePending string = "pending"
	// DemandPhaseFulfilled is the state a demand object is in when Scaler has taken action and the action has completed
	// to fulfill the demand. At this point, it is expected that there is capacity to meet the demand the object represents
	DemandPhaseFulfilled string = "fulfilled"
	// DemandPhaseCannotFulfill is the state a demand object is in when Scaler is unable to satisfy the demand. This is
	// possible if the demand contains a single unit that is larger than the instance group is configured to use, or if
	// the instance group has reached its maximum capacity and cannot allocate more
	DemandPhaseCannotFulfill string = "cannot-fulfill"
)

var (
	// AllDemandPhases is a list of all phases that a demand object could be in
	AllDemandPhases = []string{
		DemandPhaseEmpty,
		DemandPhasePending,
		DemandPhaseFulfilled,
		DemandPhaseCannotFulfill,
	}

	pluralName                 = "demands"
	demandGroupVersionResource = SchemeGroupVersion.WithResource(pluralName) // k8s requires this must be plural name
	demandGroupResource        = demandGroupVersionResource.GroupResource()
	oneFloat                   = float64(1)
	oneInt                     = int64(1)
	additionalPrinterColumns   = []v1.CustomResourceColumnDefinition{{
		Name:        "status",
		Type:        "string",
		JSONPath:    ".status.phase",
		Description: "The phase of the Demand request",
	}, {
		Name:        "instance group",
		Type:        "string",
		JSONPath:    `.spec.instance-group`,
		Description: "The instance group for the Demand request",
	}, {
		Name:        "long lived",
		Type:        "boolean",
		JSONPath:    ".spec.is-long-lived",
		Description: "The lifecycle description of the Demand request",
	}, {
		Name:        "units",
		Type:        "string",
		JSONPath:    ".spec.units",
		Description: "The units of the Demand request",
		Priority:    1,
	}}
	v1alpha1VersionDefinition = v1.CustomResourceDefinitionVersion{
		Name:    "v1alpha1",
		Served:  true,
		Storage: false,
		Subresources: &v1.CustomResourceSubresources{
			Status: &v1.CustomResourceSubresourceStatus{},
		},
		Schema: &v1.CustomResourceValidation{
			OpenAPIV3Schema: &v1.JSONSchemaProps{
				Type:     "object",
				Required: []string{"spec", "metadata"},
				Properties: map[string]v1.JSONSchemaProps{
					"status": {
						Type:     "object",
						Required: []string{"phase"},
						Properties: map[string]v1.JSONSchemaProps{
							"phase": {
								Type: "string",
								Enum: getAllowedDemandPhasesEnum(),
							},
							"last-transition-time": {
								Type:     "string",
								Format:   "date-time",
								Nullable: true,
							},
						},
					},
					"spec": {
						Type:     "object",
						Required: []string{"units", "instance-group"},
						Properties: map[string]v1.JSONSchemaProps{
							"instance-group": {
								Type:      "string",
								MinLength: &oneInt,
							},
							"is-long-lived": {
								Type: "boolean",
							},
							"units": {
								Type: "array",
								Items: &v1.JSONSchemaPropsOrArray{
									Schema: &v1.JSONSchemaProps{
										Type:     "object",
										Required: []string{"count", "cpu", "memory"},
										Properties: map[string]v1.JSONSchemaProps{
											"count":  {Type: "integer", Minimum: &oneFloat},
											"cpu":    {Type: "string", MinLength: &oneInt},
											"memory": {Type: "string", MinLength: &oneInt},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		AdditionalPrinterColumns: additionalPrinterColumns,
	}
	v1alpha2VersionDefinition = v1.CustomResourceDefinitionVersion{
		Name:    SchemeGroupVersion.Version,
		Served:  true,
		Storage: true,
		Subresources: &v1.CustomResourceSubresources{
			Status: &v1.CustomResourceSubresourceStatus{},
		},
		Schema: &v1.CustomResourceValidation{
			OpenAPIV3Schema: &v1.JSONSchemaProps{
				Type:     "object",
				Required: []string{"spec", "metadata"},
				Properties: map[string]v1.JSONSchemaProps{
					"status": {
						Type:     "object",
						Required: []string{"phase"},
						Properties: map[string]v1.JSONSchemaProps{
							"phase": {
								Type: "string",
								Enum: getAllowedDemandPhasesEnum(),
							},
							"last-transition-time": {
								Type:     "string",
								Format:   "date-time",
								Nullable: true,
							},
						},
					},
					"spec": {
						Type:     "object",
						Required: []string{"units", "instance-group"},
						Properties: map[string]v1.JSONSchemaProps{
							"instance-group": {
								Type:      "string",
								MinLength: &oneInt,
							},
							"is-long-lived": {
								Type: "boolean",
							},
							"units": {
								Type: "array",
								Items: &v1.JSONSchemaPropsOrArray{
									Schema: &v1.JSONSchemaProps{
										Type:     "object",
										Required: []string{"count", "resources"},
										Properties: map[string]v1.JSONSchemaProps{
											"resources": {
												Type: "object",
												Properties: map[string]v1.JSONSchemaProps{
													"cpu":               {Type: "string", MinLength: &oneInt},
													"ephemeral-storage": {Type: "string", MinLength: &oneInt},
													"memory":            {Type: "string", MinLength: &oneInt},
													"nvidia.com/gpu":    {Type: "string", MinLength: &oneInt},
												},
											},
											"count":  {Type: "integer", Minimum: &oneFloat},
											"memory": {Type: "string", MinLength: &oneInt},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		AdditionalPrinterColumns: additionalPrinterColumns,
	}
	demandDefinition = v1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: DemandCustomResourceDefinitionName(),
		},
		Spec: v1.CustomResourceDefinitionSpec{
			Group: SchemeGroupVersion.Group,
			Versions: []v1.CustomResourceDefinitionVersion{
				v1alpha2VersionDefinition,
				v1alpha1VersionDefinition,
			},
			Scope: v1.NamespaceScoped,
			Names: v1.CustomResourceDefinitionNames{
				Plural:     pluralName,
				Singular:   "demand",
				Kind:       "Demand",
				ShortNames: []string{"dem"},
				Categories: []string{"all"},
			},
			Conversion: &v1.CustomResourceConversion{
				Strategy: v1.WebhookConverter,
				Webhook: &v1.WebhookConversion{
					ConversionReviewVersions: []string{"v1", "v1beta1"},
					ClientConfig:             nil,
				},
			},
		},
	}
)

// DemandCustomResourceDefinition returns the CustomResourceDefinition for the demand resource. Webhook provided has to
// support conversions between v1alpha1 and v1alpha2.
func DemandCustomResourceDefinition(webhook *v1.WebhookClientConfig) *v1.CustomResourceDefinition {
	demand := demandDefinition.DeepCopy()
	demand.Spec.Conversion.Webhook.ClientConfig = webhook
	return demand
}

// DemandGroupVersionResource returns the schema.GroupVersionResource for the demand resource
func DemandGroupVersionResource() schema.GroupVersionResource {
	return demandGroupVersionResource
}

// DemandCustomResourceDefinitionName returns the demand resource name as a string
func DemandCustomResourceDefinitionName() string {
	return (&demandGroupResource).String()
}

func getAllowedDemandPhasesEnum() []v1.JSON {
	var json []v1.JSON
	for _, phase := range AllDemandPhases {
		json = append(json, v1.JSON{Raw: []byte("\"" + phase + "\"")})
	}
	return json
}
