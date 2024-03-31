package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	hotmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/hot"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type IHotService interface {
	GetHotValue(ctx context.Context, req *gencontent.GetHotValueReq) (resp *gencontent.GetHotValueResp, err error)
	IncrHotValue(ctx context.Context, req *gencontent.IncrHotValueReq) (resp *gencontent.IncrHotValueResp, err error)
	CreateHot(ctx context.Context, req *gencontent.CreateHotReq) (resp *gencontent.CreateHotResp, err error)
	GetHotValues(ctx context.Context, req *gencontent.GetHotValuesReq) (resp *gencontent.GetHotValuesResp, err error)
}

var HotSet = wire.NewSet(
	wire.Struct(new(HotService), "*"),
	wire.Bind(new(IHotService), new(*HotService)),
)

type HotService struct {
	Config         *config.Config
	HotMongoMapper hotmapper.IHotMongoMapper
	Redis          *redis.Redis
	Cache          *collection.Cache
}

func (s *HotService) GetHotValues(ctx context.Context, req *gencontent.GetHotValuesReq) (resp *gencontent.GetHotValuesResp, err error) {
	resp = new(gencontent.GetHotValuesResp)
	hots, err := s.HotMongoMapper.FindManyByIds(ctx, req.HotIds)
	if err != nil {
		return resp, err
	}
	resp.HotValues = lo.Map[*hotmapper.Hot, float64](hots, func(item *hotmapper.Hot, index int) float64 {
		return item.HotValue
	})
	return resp, err
}

func (s *HotService) CreateHot(ctx context.Context, req *gencontent.CreateHotReq) (resp *gencontent.CreateHotResp, err error) {
	oid, _ := primitive.ObjectIDFromHex(req.HotId)
	if err = s.HotMongoMapper.Insert(ctx, &hotmapper.Hot{
		ID:       oid,
		HotValue: 0,
		Views:    1,
		Score_:   0,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}
func (s *HotService) GetHotValue(ctx context.Context, req *gencontent.GetHotValueReq) (resp *gencontent.GetHotValueResp, err error) {
	resp = new(gencontent.GetHotValueResp)
	hot, err := s.HotMongoMapper.FindOne(ctx, &hotmapper.FilterOptions{
		OnlyHotId: lo.ToPtr(req.HotId),
	})
	if err != nil {
		return resp, err
	}
	resp.HotValue = hot.HotValue
	return resp, err
}

func (s *HotService) IncrHotValue(ctx context.Context, req *gencontent.IncrHotValueReq) (resp *gencontent.IncrHotValueResp, err error) {
	var (
		rankKey string
		flag    bool
	)
	hot, err := s.HotMongoMapper.FindOne(ctx, &hotmapper.FilterOptions{
		OnlyHotId: lo.ToPtr(req.HotId),
	})
	if err != nil {
		return resp, err
	}

	hotValue := hot.HotValue
	views := hot.Views

	switch req.Action {
	case gencontent.Action_ViewType:
		hotValue += 4*math.Log10(float64(hot.Views+1)) - math.Log10(float64(hot.Views))
		views++
	default:
		hotValue += float64(req.Action)
	}

	updateAt := time.Now()
	nowHot := hotValue / math.Pow(1+(float64(hot.CreateAt.Unix())/2)+(float64(updateAt.Unix())/2), 1.5)
	switch req.TargetType {
	case gencontent.TargetType_UserType:
		rankKey = consts.UserRankKey
	case gencontent.TargetType_FileType:
		rankKey = consts.FileRankKey
	case gencontent.TargetType_PostType:
		rankKey = consts.PostRankKey
	}

	value, ok := s.Cache.Get(rankKey)
	if !ok || nowHot > (value.(float64)) {
		if !ok {
			flag = true
		} else {
			d := math.Float64bits(nowHot/(value.(float64))) * 10
			if d >= 100 || d <= uint64(rand.Intn(100)) {
				flag = true
			}
		}
	}
	if flag {
		val, err := s.Redis.EvalCtx(ctx, luaScript, []string{rankKey}, []any{nowHot, req.HotId})
		if err != nil {
			return resp, err
		}
		lastRank, err := strconv.ParseFloat(val.([]any)[1].(string), 64)
		if err != nil {
			return resp, err
		}
		s.Cache.SetWithExpire(rankKey, lastRank, time.Duration(s.Config.HotServiceConf.RankKeyExpire)*time.Second)
	}

	if err = s.HotMongoMapper.Update(ctx, &hotmapper.Hot{
		ID:       hot.ID,
		HotValue: hotValue,
		Views:    views,
		UpdateAt: updateAt,
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

const luaScript = `local limit = 100
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
local currentSize = redis.call('ZCARD', KEYS[1])
if currentSize > limit then
    redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(limit + 1))
end
local lastScore = redis.call('ZREVRANGE', KEYS[1], -1, -1, 'WITHSCORES')
return lastScore
`
