package provider

import (
	"github.com/CloudStriver/cloudmind-content/biz/application/service"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/stores/redis"
	"github.com/google/wire"
)

var AllProvider = wire.NewSet(
	ApplicationSet,
	InfrastructureSet,
)

var ApplicationSet = wire.NewSet(
	service.FileSet,
	service.PostSet,
	service.LabelSet,
)

var InfrastructureSet = wire.NewSet(
	config.NewConfig,
	redis.NewRedis,
	MapperSet,
)

var MapperSet = wire.NewSet(
	file.NewMongoMapper,
	file.NewEsMapper,
	sharefile.NewMongoMapper,
	post.NewMongoMapper,
	post.NewEsMapper,
	label.NewMongoMapper,
)
