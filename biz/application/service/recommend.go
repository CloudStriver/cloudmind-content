package service

import (
	"context"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/gorse"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"strconv"
)

type IRecommendService interface {
	CreateItems(ctx context.Context, req *gencontent.CreateItemsReq) (resp *gencontent.CreateItemsResp, err error)
	UpdateItem(ctx context.Context, req *gencontent.UpdateItemReq) (resp *gencontent.UpdateItemResp, err error)
	DeleteItem(ctx context.Context, req *gencontent.DeleteItemReq) (resp *gencontent.DeleteItemResp, err error)
	GetPopularRecommend(ctx context.Context, req *gencontent.GetPopularRecommendReq) (resp *gencontent.GetPopularRecommendResp, err error)
	GetRecommendByItem(ctx context.Context, req *gencontent.GetRecommendByItemReq) (resp *gencontent.GetRecommendByItemResp, err error)
	GetRecommendByUser(ctx context.Context, req *gencontent.GetRecommendByUserReq) (resp *gencontent.GetRecommendByUserResp, err error)
	GetLatestRecommend(ctx context.Context, req *gencontent.GetLatestRecommendReq) (resp *gencontent.GetLatestRecommendResp, err error)
	CreateFeedBacks(ctx context.Context, req *gencontent.CreateFeedBacksReq) (resp *gencontent.CreateFeedBacksResp, err error)
}

var RecommendSet = wire.NewSet(
	wire.Struct(new(RecommendService), "*"),
	wire.Bind(new(IRecommendService), new(*RecommendService)),
)

type RecommendService struct {
	Redis *redis.Redis
	Gorse *gorse.GorseClient
}

func (s *RecommendService) CreateFeedBacks(ctx context.Context, req *gencontent.CreateFeedBacksReq) (resp *gencontent.CreateFeedBacksResp, err error) {
	feedbacks := lo.Map[*gencontent.FeedBack, gorse.Feedback](req.FeedBacks, func(feedback *gencontent.FeedBack, index int) gorse.Feedback {
		return convertor.FeedBackToGorseFeedBack(feedback)
	})
	if _, err = s.Gorse.InsertFeedback(ctx, feedbacks); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RecommendService) GetLatestRecommend(ctx context.Context, req *gencontent.GetLatestRecommendReq) (resp *gencontent.GetLatestRecommendResp, err error) {
	resp = new(gencontent.GetLatestRecommendResp)
	if req.Limit == nil {
		req.Limit = lo.ToPtr(int64(consts.DefaultLimit))
	}

	var (
		offset int
		val    string
		items  []gorse.Score
	)
	if val, _ = s.Redis.GetCtx(ctx, fmt.Sprintf("cache:latest:recommend:%s", req.UserId)); val == "" {
		offset = 0
	} else {
		if offset, err = strconv.Atoi(val); err != nil {
			offset = 0
		}
	}

	if items, err = s.Gorse.GetItemLatestWithCategory(ctx, req.UserId, req.Category, int(req.GetLimit()), offset); err != nil {
		return resp, err
	}
	if len(resp.ItemIds) < int(req.GetLimit()) {
		offset = 0
	} else {
		offset += int(req.GetLimit())
	}

	_ = s.Redis.SetexCtx(ctx, fmt.Sprintf("cache:latest:recommend:%s", req.UserId), strconv.Itoa(offset), 3600)
	resp.ItemIds = lo.Map[gorse.Score, string](items, func(score gorse.Score, _ int) string {
		return score.Id
	})

	return resp, nil
}

func (s *RecommendService) CreateItems(ctx context.Context, req *gencontent.CreateItemsReq) (resp *gencontent.CreateItemsResp, err error) {
	items := lo.Map[*gencontent.Item, gorse.Item](req.Items, func(item *gencontent.Item, index int) gorse.Item {
		return convertor.ItemToGorseItem(item)
	})
	if _, err = s.Gorse.InsertItems(ctx, items); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RecommendService) UpdateItem(ctx context.Context, req *gencontent.UpdateItemReq) (resp *gencontent.UpdateItemResp, err error) {
	if _, err = s.Gorse.UpdateItem(ctx, req.ItemId, &gorse.ItemPatch{
		IsHidden: req.IsHidden,
		Labels:   req.Labels,
		Comment:  req.Comment,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RecommendService) DeleteItem(ctx context.Context, req *gencontent.DeleteItemReq) (resp *gencontent.DeleteItemResp, err error) {
	if _, err = s.Gorse.DeleteItem(ctx, req.ItemId); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *RecommendService) GetPopularRecommend(ctx context.Context, req *gencontent.GetPopularRecommendReq) (resp *gencontent.GetPopularRecommendResp, err error) {
	resp = new(gencontent.GetPopularRecommendResp)
	if req.Limit == nil {
		req.Limit = lo.ToPtr(int64(consts.DefaultLimit))
	}

	var (
		offset int
		items  []gorse.Score
		val    string
	)
	if val, _ = s.Redis.GetCtx(ctx, fmt.Sprintf("cache:popular:recommend:%s", req.UserId)); val == "" {
		offset = 0
	} else {
		if offset, err = strconv.Atoi(val); err != nil {
			offset = 0
		}
	}

	if items, err = s.Gorse.GetItemPopularWithCategory(ctx, req.UserId, req.Category, int(req.GetLimit()), offset); err != nil {
		return resp, err
	}
	if len(items) < int(req.GetLimit()) {
		offset = 0
	} else {
		offset += int(req.GetLimit())
	}

	_ = s.Redis.SetexCtx(ctx, fmt.Sprintf("cache:popular:recommend:%s", req.UserId), strconv.Itoa(offset), 3600)

	resp.ItemIds = lo.Map[gorse.Score, string](items, func(score gorse.Score, _ int) string {
		return score.Id
	})
	return resp, nil
}

func (s *RecommendService) GetRecommendByItem(ctx context.Context, req *gencontent.GetRecommendByItemReq) (resp *gencontent.GetRecommendByItemResp, err error) {
	resp = new(gencontent.GetRecommendByItemResp)
	if req.Limit == nil {
		req.Limit = lo.ToPtr(int64(consts.DefaultLimit))
	}

	var (
		offset int
		val    string
		items  []gorse.Score
	)
	if val, _ = s.Redis.GetCtx(ctx, fmt.Sprintf("cache:item:recommend:%s", req.ItemId)); val == "" {
		offset = 0
	} else {
		if offset, err = strconv.Atoi(val); err != nil {
			offset = 0
		}
	}

	if items, err = s.Gorse.GetItemNeighborsWithCategory(ctx, req.ItemId, req.Category, int(req.GetLimit()), offset); err != nil {
		return resp, err
	}
	if len(resp.ItemIds) < int(req.GetLimit()) {
		offset = 0
	} else {
		offset += int(req.GetLimit())
	}

	_ = s.Redis.SetexCtx(ctx, fmt.Sprintf("cache:item:recommend:%s", req.ItemId), strconv.Itoa(offset), 3600)
	resp.ItemIds = lo.Map[gorse.Score, string](items, func(score gorse.Score, _ int) string {
		return score.Id
	})
	return resp, nil
}

func (s *RecommendService) GetRecommendByUser(ctx context.Context, req *gencontent.GetRecommendByUserReq) (resp *gencontent.GetRecommendByUserResp, err error) {
	resp = new(gencontent.GetRecommendByUserResp)
	if req.Limit == nil {
		req.Limit = lo.ToPtr(int64(consts.DefaultLimit))
	}

	var (
		offset int
		val    string
	)
	if val, _ = s.Redis.GetCtx(ctx, fmt.Sprintf("cache:user:recommend:%s", req.UserId)); val == "" {
		offset = 0
	} else {
		if offset, err = strconv.Atoi(val); err != nil {
			offset = 0
		}
	}

	if resp.ItemIds, err = s.Gorse.GetItemRecommendWithCategory(ctx, req.UserId, req.Category, "read", "60m", int(req.GetLimit()), offset); err != nil {
		return resp, err
	}
	if len(resp.ItemIds) < int(req.GetLimit()) {
		offset = 0
	} else {
		offset += int(req.GetLimit())
	}

	_ = s.Redis.SetexCtx(ctx, fmt.Sprintf("cache:user:recommend:%s", req.UserId), strconv.Itoa(offset), 3600)
	return resp, nil
}
