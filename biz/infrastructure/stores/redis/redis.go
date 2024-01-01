package redis

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

const (
	fileId = "fileId"
)

func NewRedis(config *config.Config) *redis.Redis {
	return redis.MustNewRedis(*config.Redis)
}
