//go:build wireinject
// +build wireinject

package provider

import (
	"github.com/CloudStriver/cloudmind-content/biz/adaptor"
	"github.com/google/wire"
)

func NewContentServerImpl() (*adaptor.ContentServerImpl, error) {
	wire.Build(
		wire.Struct(new(adaptor.ContentServerImpl), "*"),
		AllProvider,
	)
	return nil, nil
}
