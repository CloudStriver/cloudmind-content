package service

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	"github.com/google/wire"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type IPostService interface {
	//CreatePost(ctx context.Context, req *content.CreatePostReq) (*content.CreatePostResp, error)
	//RetrievePost(ctx context.Context, req *content.RetrievePostReq) (*content.RetrievePostResp, error)
	//UpdatePost(ctx context.Context, req *content.UpdatePostReq) (*content.UpdatePostResp, error)
	//DeletePost(ctx context.Context, req *content.DeletePostReq) (*content.DeletePostResp, error)
	//ListPost(ctx context.Context, req *content.ListPostReq) (*content.ListPostResp, error)
	//CountPost(ctx context.Context, req *content.CountPostReq) (*content.CountPostResp, error)
	//SetOfficial(ctx context.Context, req *content.SetOfficialReq) (*content.SetOfficialResp, error)
}

type PostService struct {
	Config          *config.Config
	PostMongoMapper post.IMongoMapper
	PostEsMapper    post.IEsMapper
	Redis           *redis.Redis
}

var PostSet = wire.NewSet(
	wire.Struct(new(PostService), "*"),
	wire.Bind(new(IPostService), new(*PostService)),
)

//func (s *PostService) CreatePost(ctx context.Context, req *content.CreatePostReq) (*content.CreatePostResp, error) {
//	resp := new(content.CreatePostResp)
//	p := &post.Post{
//		Title:    req.Title,
//		Text:     req.Text,
//		CoverUrl: req.CoverUrl,
//		Tags:     req.Tags,
//		UserId:   req.UserId,
//	}
//	err := s.PostMongoMapper.Insert(ctx, p)
//	if err != nil {
//		return nil, err
//	}
//	resp.PostId = p.ID.Hex()
//
//	//小鱼干奖励
//	data, err := s.Redis.GetCtx(ctx, "contentTimes"+req.UserId)
//	if err != nil {
//		return resp, nil
//	}
//	r, err := s.Redis.GetCtx(ctx, "contentDate"+req.UserId)
//	if err != nil {
//		return resp, nil
//	} else if r == "" {
//		resp.GetFish = true
//		resp.GetFishTimes = 1
//		err = s.Redis.SetexCtx(ctx, "contentTimes"+req.UserId, "1", 86400)
//		if err != nil {
//			resp.GetFish = false
//			return resp, nil
//		}
//		err = s.Redis.SetexCtx(ctx, "contentDate"+req.UserId, strconv.FormatInt(time.Now().Unix(), 10), 86400)
//		if err != nil {
//			resp.GetFish = false
//			return resp, nil
//		}
//	} else {
//		times, err := strconv.ParseInt(data, 10, 64)
//		if err != nil {
//			return resp, nil
//		}
//		resp.GetFishTimes = times + 1
//		m, err := strconv.ParseInt(r, 10, 64)
//		if err != nil {
//			return resp, nil
//		}
//		lastTime := time.Unix(m, 0)
//		err = s.Redis.SetexCtx(ctx, "contentTimes"+req.UserId, strconv.FormatInt(times+1, 10), 86400)
//		if err != nil {
//			return resp, nil
//		}
//		err = s.Redis.SetexCtx(ctx, "contentDate"+req.UserId, strconv.FormatInt(time.Now().Unix(), 10), 86400)
//		if err != nil {
//			return resp, nil
//		}
//		if lastTime.Day() == time.Now().Day() && lastTime.Month() == time.Now().Month() && lastTime.Year() == time.Now().Year() {
//			err = s.Redis.SetexCtx(ctx, "contentTimes"+req.UserId, strconv.FormatInt(times+1, 10), 86400)
//			if err != nil {
//				return resp, nil
//			}
//			if times >= s.Config.GetFishTimes {
//				resp.GetFish = false
//			} else {
//				resp.GetFish = true
//			}
//		} else {
//			err = s.Redis.SetexCtx(ctx, "contentTimes"+req.UserId, "1", 86400)
//			if err != nil {
//				return resp, nil
//			}
//			resp.GetFish = true
//			resp.GetFishTimes = 1
//		}
//	}
//	return resp, nil
//}
//
//func (s *PostService) RetrievePost(ctx context.Context, req *content.RetrievePostReq) (*content.RetrievePostResp, error) {
//	data, err := s.PostMongoMapper.FindOne(ctx, req.PostId)
//	switch err {
//	case nil:
//	case consts.ErrNotFound:
//		return nil, consts.ErrNoSuchPost
//	default:
//		return nil, err
//	}
//	return &content.RetrievePostResp{Post: convertor.ConvertPost(data)}, nil
//}
//
//func (s *PostService) UpdatePost(ctx context.Context, req *content.UpdatePostReq) (*content.UpdatePostResp, error) {
//	oid, err := primitive.ObjectIDFromHex(req.Id)
//	if err != nil {
//		return nil, consts.ErrInvalidObjectId
//	}
//	err = s.PostMongoMapper.Update(ctx, &post.Post{
//		ID:       oid,
//		Title:    req.Title,
//		Text:     req.Text,
//		CoverUrl: req.CoverUrl,
//		Tags:     req.Tags,
//	})
//	if err != nil {
//		return nil, err
//	}
//
//	return &content.UpdatePostResp{}, nil
//}
//
//func (s *PostService) DeletePost(ctx context.Context, req *content.DeletePostReq) (*content.DeletePostResp, error) {
//	err := s.PostMongoMapper.Delete(ctx, req.Id)
//	if err != nil {
//		return nil, err
//	}
//	return &content.DeletePostResp{}, nil
//}
//
//func (s *PostService) ListPost(ctx context.Context, req *content.ListPostReq) (*content.ListPostResp, error) {
//	resp := new(content.ListPostResp)
//	var posts []*post.Post
//	var total int64
//	var err error
//
//	filter := convertor.ParsePostFilter(req.FilterOptions)
//
//	p := convertor.ParsePagination(req.PaginationOptions)
//
//	if req.SearchOptions == nil {
//		posts, total, err = s.PostMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType)
//		if err != nil {
//			return nil, err
//		}
//	} else {
//		switch o := req.SearchOptions.Type.(type) {
//		case *content.SearchOptions_AllFieldsKey:
//			posts, total, err = s.PostEsMapper.Search(ctx, convertor.ConvertPostAllFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
//		case *content.SearchOptions_MultiFieldsKey:
//			posts, total, err = s.PostEsMapper.Search(ctx, convertor.ConvertPostMultiFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
//		}
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	resp.Total = total
//	if p.LastToken != nil {
//		resp.Token = *p.LastToken
//	}
//	resp.Posts = make([]*content.Post, 0, len(posts))
//	for _, post_ := range posts {
//		resp.Posts = append(resp.Posts, convertor.ConvertPost(post_))
//	}
//	return resp, nil
//}
//
//func (s *PostService) CountPost(ctx context.Context, req *content.CountPostReq) (*content.CountPostResp, error) {
//	var total int64
//	var err error
//
//	filter := convertor.ParsePostFilter(req.FilterOptions)
//
//	if req.SearchOptions == nil {
//		total, err = s.PostMongoMapper.Count(ctx, filter)
//		if err != nil {
//			return nil, err
//		}
//	} else {
//		switch o := req.SearchOptions.Type.(type) {
//		case *content.SearchOptions_AllFieldsKey:
//			total, err = s.PostEsMapper.CountWithQuery(ctx, convertor.ConvertPostAllFieldsSearchQuery(o), filter)
//		case *content.SearchOptions_MultiFieldsKey:
//			total, err = s.PostEsMapper.CountWithQuery(ctx, convertor.ConvertPostMultiFieldsSearchQuery(o), filter)
//		}
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	return &content.CountPostResp{Total: total}, nil
//}
//
//func (s *PostService) SetOfficial(ctx context.Context, req *content.SetOfficialReq) (*content.SetOfficialResp, error) {
//	err := s.PostMongoMapper.UpdateFlags(ctx, req.PostId, map[post.Flag]bool{
//		post.OfficialFlag: !req.IsRemove,
//	})
//	if err != nil {
//		return nil, err
//	}
//	return &content.SetOfficialResp{}, nil
//}
