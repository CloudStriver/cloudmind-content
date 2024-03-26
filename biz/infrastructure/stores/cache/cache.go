package cache

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/zeromicro/go-zero/core/collection"
	"time"
)

func NewLocalCache(config *config.Config) *collection.Cache {
	localCache, err := collection.NewCache(time.Duration(config.LocalCacheConf.Expire))
	if err != nil {
		panic(err)
	}
	return localCache
}
