package awscredentials

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/Analytical-Tradecraft-Technologies/llm-temporal-worker/golang/llm/provider"
)

func TestWrapMarksRetrieveErrorsAndKeepsTheCause(t *testing.T) {
	cause := errors.New("no usable credential source")
	wrapped := Wrap(aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return aws.Credentials{}, cause }))
	if _, err := wrapped.Retrieve(context.Background()); !errors.Is(err, provider.ErrProviderCredentials) || !errors.Is(err, cause) {
		t.Fatalf("Retrieve() error = %v, want the credential marker and the cause", err)
	}
	static := Wrap(credentials.NewStaticCredentialsProvider("access", "secret", ""))
	if got, err := static.Retrieve(context.Background()); err != nil || got.AccessKeyID != "access" {
		t.Fatalf("Retrieve() = %#v, %v, want the static credentials", got, err)
	}
	if !aws.IsCredentialsProvider(static, credentials.StaticCredentialsProvider{}) {
		t.Fatal("IsCredentialsProvider does not see through the wrapper")
	}
}

func TestWrapLeavesNilAndAnonymousProvidersUnchanged(t *testing.T) {
	if Wrap(nil) != nil {
		t.Fatal("Wrap(nil) is not nil")
	}
	anonymous := aws.AnonymousCredentials{}
	if got := Wrap(anonymous); got != anonymous {
		t.Fatalf("Wrap(anonymous) = %#v, want it unchanged", got)
	}
}
