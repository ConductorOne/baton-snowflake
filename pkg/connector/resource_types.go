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
)

func getSkipEntitlementsAnnotation() annotations.Annotations {
	annotations := annotations.Annotations{}
	annotations.Update(&v2.SkipEntitlementsAndGrants{})

	return annotations
}
