package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

var (
	userResourceType = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: getSkipEntitlementsAnnotation(),
	}
	accountRoleResourceType = &v2.ResourceType{
		Id:          "account_role",
		DisplayName: "Account Role",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_ROLE},
	}
	databaseResourceType = &v2.ResourceType{
		Id:          "database",
		DisplayName: "Database",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
	}
	tableResourceType = &v2.ResourceType{
		Id:          "table",
		DisplayName: "Table",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
	}
	secretResourceType = &v2.ResourceType{
		Id:          "secret",
		DisplayName: "Secret",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECRET},
		Annotations: getSkipEntitlementsAnnotation(),
	}
	rsaPublicKeyResourceType = &v2.ResourceType{
		Id:          "rsa_public_key",
		DisplayName: "RSA Public Key",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECRET},
		Annotations: getSkipEntitlementsAnnotation(),
	}
	// OptInRequired: a tenant that cannot grant OWNERSHIP or MODIFY PROGRAMMATIC
	// AUTHENTICATION METHODS on every user must be able to keep this type off in
	// the C1 UI. Syncing it without that privilege would fail every sync (the
	// builder's List refuses to return an error-free empty result on a denial,
	// which would silently delete previously synced tokens).
	programmaticAccessTokenResourceType = &v2.ResourceType{
		Id:          "programmatic_access_token",
		DisplayName: "Programmatic Access Token",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECRET},
		Annotations: getOptInAnnotations(),
	}
	integrationResourceType = &v2.ResourceType{
		Id:          "integration",
		DisplayName: "Integration",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP},
		Annotations: getSkipEntitlementsAnnotation(),
	}
	licenseResourceType = &v2.ResourceType{
		Id:          "license",
		DisplayName: "License",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_LICENSE_PROFILE},
		Annotations: getOptInAnnotations(),
	}
)

func getSkipEntitlementsAnnotation() annotations.Annotations {
	annotations := annotations.Annotations{}
	annotations.Update(&v2.SkipEntitlementsAndGrants{})

	return annotations
}

func getOptInAnnotations() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	annos.Update(&v2.OptInRequired{})

	return annos
}
