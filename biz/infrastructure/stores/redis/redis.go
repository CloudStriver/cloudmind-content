package redis

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"time"
)

func NewRedis(config *config.Config) *redis.Redis {
	r := config.Redis
	r.PingTimeout = 5 * time.Second
	return redis.MustNewRedis(r)
}
