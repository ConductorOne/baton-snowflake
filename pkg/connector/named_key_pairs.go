package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-snowflake/pkg/snowflake"
	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/durationpb"
)

type namedKeyPairBuilder struct {
	client *snowflake.Client
}

const (
	snowflakeServiceUserType       = "SERVICE"
	snowflakeLegacyServiceUserType = "LEGACY_SERVICE"
)

func newNamedKeyPairBuilder(client *snowflake.Client) *namedKeyPairBuilder {
	return &namedKeyPairBuilder{client: client}
}

func (*namedKeyPairBuilder) ResourceType(context.Context) *v2.ResourceType {
	return namedKeyPairResourceType
}

func (b *namedKeyPairBuilder) List(ctx context.Context, parent *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	if parent == nil || parent.GetResourceType() != userResourceType.Id {
		return nil, nil, nil
	}
	keyPairs, err := b.client.ListUserKeyPairs(ctx, parent.GetResource())
	if err != nil {
		return nil, nil, fmt.Errorf("baton-snowflake: list named key pairs: %w", err)
	}
	resources := make([]*v2.Resource, 0, len(keyPairs))
	for i := range keyPairs {
		resource, err := namedKeyPairResource(parent, &keyPairs[i])
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, resource)
	}
	return resources, nil, nil
}

func (*namedKeyPairBuilder) Entitlements(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (*namedKeyPairBuilder) Grants(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

type credentialIssuingUserBuilder struct {
	*userBuilder
	getUser    func(context.Context, string) (*snowflake.User, error)
	addKeyPair func(context.Context, string, string, string, int) error
	newKeyName func() string
	now        func() time.Time
}

var _ connectorbuilder.CredentialIssuerV2 = (*credentialIssuingUserBuilder)(nil)

func newCredentialIssuingUserBuilder(base *userBuilder) *credentialIssuingUserBuilder {
	return &credentialIssuingUserBuilder{
		userBuilder: base,
		getUser: func(ctx context.Context, username string) (*snowflake.User, error) {
			user, _, err := base.client.GetUser(ctx, nil, username)
			return user, err
		},
		addKeyPair: base.client.AddUserKeyPair,
		newKeyName: func() string { return "c1_" + ksuid.New().String() },
		now:        time.Now,
	}
}

func (b *credentialIssuingUserBuilder) Issue(
	ctx context.Context,
	input *connectorbuilder.CredentialIssueInput,
) (*connectorbuilder.CredentialIssueOutput, error) {
	identityID := input.IdentityID
	if identityID == nil || identityID.GetResourceType() != userResourceType.Id {
		return nil, fmt.Errorf("baton-snowflake: invalid service user identity")
	}
	user, err := b.getUser(ctx, identityID.GetResource())
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: get credential target: %w", err)
	}
	if user.Type != snowflakeServiceUserType && user.Type != snowflakeLegacyServiceUserType {
		return nil, fmt.Errorf("baton-snowflake: key pairs may only be issued for service users")
	}

	if input.CredentialOptions == nil {
		return nil, fmt.Errorf("baton-snowflake: credential options are required")
	}
	keypair := input.CredentialOptions.GetKeypair()
	if keypair == nil {
		return nil, fmt.Errorf("baton-snowflake: only keypair credentials are supported")
	}
	if keypair.GetProfile().GetKty() != "RSA" {
		return nil, fmt.Errorf("baton-snowflake: only RSA key pairs are supported")
	}
	bits := int(keypair.GetProfile().GetRsaModulusBits())
	if bits != 2048 && bits != 3072 && bits != 4096 {
		return nil, fmt.Errorf("baton-snowflake: unsupported RSA key size %d", bits)
	}

	daysToExpiry := 0
	if ttl := input.IssuanceConstraints.GetLifetime(); ttl != nil {
		if err := ttl.CheckValid(); err != nil || ttl.AsDuration() <= 0 || ttl.AsDuration()%(24*time.Hour) != 0 {
			return nil, fmt.Errorf("baton-snowflake: keypair lifetime must be a positive whole number of days")
		}
		daysToExpiry = int(ttl.AsDuration() / (24 * time.Hour))
	}

	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: generate RSA key: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: marshal private key: %w", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	publicDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("baton-snowflake: marshal public key: %w", err)
	}
	publicKey := base64.StdEncoding.EncodeToString(publicDER)

	keyName := b.newKeyName()
	if err := b.addKeyPair(ctx, identityID.GetResource(), keyName, publicKey, daysToExpiry); err != nil {
		return nil, fmt.Errorf("baton-snowflake: register named key pair: %w", err)
	}

	now := b.now().UTC()
	fingerprintBytes := sha256.Sum256(publicDER)
	metadata := &snowflake.NamedKeyPair{
		Name:        keyName,
		UserName:    identityID.GetResource(),
		Fingerprint: "SHA256:" + base64.StdEncoding.EncodeToString(fingerprintBytes[:]),
		Status:      "ACTIVE",
		CreatedOn:   now,
	}
	if daysToExpiry > 0 {
		metadata.ExpiresAt = now.Add(time.Duration(daysToExpiry) * 24 * time.Hour)
	}
	secret, err := namedKeyPairResource(identityID, metadata)
	if err != nil {
		return nil, err
	}
	return &connectorbuilder.CredentialIssueOutput{
		Secret: secret,
		PlaintextData: []*v2.PlaintextData{{
			Name:        "private_key.pem",
			Description: "Snowflake named key-pair private key",
			Schema:      "application/x-pem-file",
			Bytes:       privatePEM,
		}},
	}, nil
}

func (*credentialIssuingUserBuilder) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	profiles := make([]*v2.KeyGenerationProfile, 0, 3)
	for _, size := range []uint32{2048, 3072, 4096} {
		bits := size
		profiles = append(profiles, v2.KeyGenerationProfile_builder{Kty: "RSA", RsaModulusBits: &bits}.Build())
	}
	return v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{
			v2.CredentialIssueOptionDescriptor_builder{
				Option:      v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR,
				KeyProfiles: profiles,
				Lifetime: v2.IssuanceLifetimeCapability_builder{
					Min:         durationpb.New(24 * time.Hour),
					Granularity: durationpb.New(24 * time.Hour),
				}.Build(),
			}.Build(),
		},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR,
	}.Build(), nil, nil
}

func (b *credentialIssuingUserBuilder) GetCredentialIssueEligibility(ctx context.Context, identityID *v2.ResourceId, _ v2.CapabilityDetailCredentialOption) (*v2.GetCredentialIssueEligibilityResponse, error) {
	if identityID == nil || identityID.GetResourceType() != userResourceType.Id {
		return v2.GetCredentialIssueEligibilityResponse_builder{Status: v2.GetCredentialIssueEligibilityResponse_STATUS_INELIGIBLE, ReasonCode: "invalid_identity"}.Build(), nil
	}
	user, err := b.getUser(ctx, identityID.GetResource())
	if err != nil {
		return nil, err
	}
	if user.Type == snowflakeServiceUserType || user.Type == snowflakeLegacyServiceUserType {
		return v2.GetCredentialIssueEligibilityResponse_builder{Status: v2.GetCredentialIssueEligibilityResponse_STATUS_ELIGIBLE}.Build(), nil
	}
	return v2.GetCredentialIssueEligibilityResponse_builder{
		Status:      v2.GetCredentialIssueEligibilityResponse_STATUS_INELIGIBLE,
		ReasonCode:  "not_service_user",
		Explanation: "Snowflake named key pairs may only be issued for service users",
	}.Build(), nil
}

func namedKeyPairResource(identityID *v2.ResourceId, keyPair *snowflake.NamedKeyPair) (*v2.Resource, error) {
	resourceID := fmt.Sprintf("%s:%s", keyPair.UserName, keyPair.Name)
	secretOptions := []rs.SecretTraitOption{
		rs.WithSecretIdentityID(identityID),
		rs.WithSecretType(v2.SecretTrait_CREDENTIAL_TYPE_ASYMMETRIC_KEY),
		rs.WithSecretDetail("snowflake.named_key_pair"),
	}
	resourceOptions := []rs.ResourceOption{
		rs.WithParentResourceID(identityID),
		rs.WithDescription("Snowflake key pair " + keyPair.Fingerprint),
	}
	if !keyPair.CreatedOn.IsZero() {
		resourceOptions = append(resourceOptions, rs.WithResourceCreatedAt(keyPair.CreatedOn))
	}
	if !keyPair.LastUsedOn.IsZero() {
		secretOptions = append(secretOptions, rs.WithSecretLastUsedAt(keyPair.LastUsedOn))
	}
	if !keyPair.ExpiresAt.IsZero() {
		secretOptions = append(secretOptions, rs.WithSecretExpiresAt(keyPair.ExpiresAt))
	}
	return rs.NewSecretResource(
		keyPair.Name,
		namedKeyPairResourceType,
		resourceID,
		secretOptions,
		resourceOptions...,
	)
}
