package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	postmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
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
}

type PostService struct {
	Config          *config.Config
	PostMongoMapper postmapper.IPostMongoMapper
	PostEsMapper    postmapper.IEsMapper
	Redis           *redis.Redis
}

var PostSet = wire.NewSet(
	wire.Struct(new(PostService), "*"),
	wire.Bind(new(IPostService), new(*PostService)),
)

func (s *PostService) CreatePost(ctx context.Context, req *gencontent.CreatePostReq) (resp *gencontent.CreatePostResp, err error) {
	if resp.PostId, err = s.PostMongoMapper.Insert(ctx, &postmapper.Post{
		Title:  req.Title,
		Text:   req.Text,
		Url:    req.Url,
		Tags:   req.Tags,
		UserId: req.UserId,
		Status: req.Status,
		Score_: 0,
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
		Tags:       post.Tags,
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
	if req.SearchOptions != nil {
		switch o := req.SearchOptions.Type.(type) {
		case *gencontent.SearchOptions_AllFieldsKey:
			posts, total, err = s.PostEsMapper.Search(ctx, convertor.ConvertPostAllFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		case *gencontent.SearchOptions_MultiFieldsKey:
			posts, total, err = s.PostEsMapper.Search(ctx, convertor.ConvertPostMultiFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		}
	} else {
		posts, total, err = s.PostMongoMapper.FindManyAndCount(ctx, convertor.PostFilterOptionsToFilterOptions(req.PostFilterOptions),
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
		ID:     oid,
		Title:  req.Title,
		Text:   req.Text,
		Url:    req.Url,
		Tags:   req.Tags,
		Status: req.Status,
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
