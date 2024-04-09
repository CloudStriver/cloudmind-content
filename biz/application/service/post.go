package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	postmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type IPostService interface {
	CreatePost(ctx context.Context, req *gencontent.CreatePostReq) (resp *gencontent.CreatePostResp, err error)
	GetPost(ctx context.Context, req *gencontent.GetPostReq) (resp *gencontent.GetPostResp, err error)
	GetPosts(ctx context.Context, req *gencontent.GetPostsReq) (resp *gencontent.GetPostsResp, err error)
	UpdatePost(ctx context.Context, req *gencontent.UpdatePostReq) (resp *gencontent.UpdatePostResp, err error)
	DeletePost(ctx context.Context, req *gencontent.DeletePostReq) (resp *gencontent.DeletePostResp, err error)
	GetPostsByPostIds(ctx context.Context, req *gencontent.GetPostsByPostIdsReq) (resp *gencontent.GetPostsByPostIdsResp, err error)
}

var PostSet = wire.NewSet(
	wire.Struct(new(PostService), "*"),
	wire.Bind(new(IPostService), new(*PostService)),
)

type PostService struct {
	Config          *config.Config
	PostMongoMapper postmapper.IPostMongoMapper
	PostEsMapper    postmapper.IEsMapper
	Redis           *redis.Redis
}

func (s *PostService) GetPostsByPostIds(ctx context.Context, req *gencontent.GetPostsByPostIdsReq) (resp *gencontent.GetPostsByPostIdsResp, err error) {
	resp = new(gencontent.GetPostsByPostIdsResp)
	posts, err := s.PostMongoMapper.FindManyByIds(ctx, req.PostIds)
	if err != nil {
		return resp, err
	}

	resp.Posts = lo.Map[*postmapper.Post, *gencontent.Post](posts, func(item *postmapper.Post, _ int) *gencontent.Post {
		return convertor.PostMapperToPost(item)
	})
	return resp, nil
}

func (s *PostService) CreatePost(ctx context.Context, req *gencontent.CreatePostReq) (resp *gencontent.CreatePostResp, err error) {
	resp = new(gencontent.CreatePostResp)
	if resp.PostId, err = s.PostMongoMapper.Insert(ctx, &postmapper.Post{
		Title:    req.Title,
		Text:     req.Text,
		Url:      req.Url,
		LabelIds: req.LabelIds,
		UserId:   req.UserId,
		Status:   req.Status,
		Score_:   0,
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *PostService) GetPost(ctx context.Context, req *gencontent.GetPostReq) (resp *gencontent.GetPostResp, err error) {
	post, err := s.PostMongoMapper.FindOne(ctx, req.PostId)
	if err != nil {
		return resp, err
	}
	return &gencontent.GetPostResp{
		UserId:     post.UserId,
		Title:      post.Title,
		Text:       post.Text,
		LabelIds:   post.LabelIds,
		Url:        post.Url,
		Status:     post.Status,
		CreateTime: post.CreateAt.UnixMilli(),
		UpdateTime: post.UpdateAt.UnixMilli(),
	}, nil
}

func (s *PostService) GetPosts(ctx context.Context, req *gencontent.GetPostsReq) (resp *gencontent.GetPostsResp, err error) {
	resp = new(gencontent.GetPostsResp)
	var (
		total int64
		posts []*postmapper.Post
	)

	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	filter := convertor.PostFilterOptionsToFilterOptions(req.PostFilterOptions)
	if req.SearchOption != nil {
		posts, total, err = s.PostEsMapper.Search(ctx, convertor.ConvertPostAllFieldsSearchQuery(*req.SearchOption.SearchKeyword),
			filter, p, req.SearchOption.SearchSortType)
	} else {
		posts, total, err = s.PostMongoMapper.FindManyAndCount(ctx, filter,
			p, mongop.IdCursorType)
	}
	if err != nil {
		return resp, err
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Posts = lo.Map[*postmapper.Post, *gencontent.Post](posts, func(item *postmapper.Post, _ int) *gencontent.Post {
		return convertor.PostMapperToPost(item)
	})
	return resp, nil
}

func (s *PostService) UpdatePost(ctx context.Context, req *gencontent.UpdatePostReq) (resp *gencontent.UpdatePostResp, err error) {
	oid, _ := primitive.ObjectIDFromHex(req.PostId)

	if err = s.PostMongoMapper.Update(ctx, &postmapper.Post{
		ID:       oid,
		Title:    req.Title,
		Text:     req.Text,
		Url:      req.Url,
		LabelIds: req.LabelIds,
		Status:   req.Status,
		Score_:   0,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *PostService) DeletePost(ctx context.Context, req *gencontent.DeletePostReq) (resp *gencontent.DeletePostResp, err error) {
	if err = s.PostMongoMapper.Delete(ctx, req.PostId); err != nil {
		return resp, err
	}
	return resp, nil
}
