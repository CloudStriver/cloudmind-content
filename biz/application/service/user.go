package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type IUserService interface {
	GetUser(ctx context.Context, req *gencontent.GetUserReq) (resp *gencontent.GetUserResp, err error)
	CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error)
	UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error)
	GetUsers(ctx context.Context, req *gencontent.GetUsersReq) (resp *gencontent.GetUsersResp, err error)
	DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error)
	GetUsersByUserIds(ctx context.Context, req *gencontent.GetUsersByUserIdsReq) (resp *gencontent.GetUsersByUserIdsResp, err error)
}

type UserService struct {
	Config          *config.Config
	UserMongoMapper usermapper.IUserMongoMapper
	UserEsMapper    usermapper.IEsMapper
	Redis           *redis.Redis
}

var UserSet = wire.NewSet(
	wire.Struct(new(UserService), "*"),
	wire.Bind(new(IUserService), new(*UserService)),
)

func (s *UserService) GetUsersByUserIds(ctx context.Context, req *gencontent.GetUsersByUserIdsReq) (resp *gencontent.GetUsersByUserIdsResp, err error) {
	resp = new(gencontent.GetUsersByUserIdsResp)
	users, err := s.UserMongoMapper.FindManyByIds(ctx, req.UserIds)
	if err != nil {
		return resp, err
	}
	resp.Users = lo.Map[*usermapper.User, *gencontent.User](users, func(item *usermapper.User, _ int) *gencontent.User {
		return convertor.UserMapperToUser(item)
	})
	return resp, nil
}
func (s *UserService) DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error) {
	if _, err = s.UserMongoMapper.Delete(ctx, req.UserId); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *UserService) GetUsers(ctx context.Context, req *gencontent.GetUsersReq) (resp *gencontent.GetUsersResp, err error) {
	resp = new(gencontent.GetUsersResp)
	var (
		users []*usermapper.User
		total int64
	)

	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	if req.SearchOption != nil {
		users, total, err = s.UserEsMapper.Search(ctx, convertor.ConvertUserAllFieldsSearchQuery(*req.SearchOption.SearchKeyword), p, req.SearchOption)
	}
	if err != nil {
		return resp, err
	}

	if p.LastToken != nil {
		resp.LastToken = *p.LastToken
	}
	resp.Users = lo.Map[*usermapper.User, *gencontent.User](users, func(item *usermapper.User, _ int) *gencontent.User {
		return convertor.UserMapperToUser(item)
	})
	resp.Total = total
	return resp, nil
}

func (s *UserService) GetUser(ctx context.Context, req *gencontent.GetUserReq) (resp *gencontent.GetUserResp, err error) {
	var user *usermapper.User
	if user, err = s.UserMongoMapper.FindOne(ctx, req.UserId); err != nil {
		return resp, err
	}
	return &gencontent.GetUserResp{
		Name:          user.Name,
		Sex:           user.Sex,
		FullName:      user.FullName,
		IdCard:        user.IdCard,
		Description:   user.Description,
		Url:           user.Url,
		CreateTime:    user.CreateAt.UnixMilli(),
		UpdateTime:    user.UpdateAt.UnixMilli(),
		Labels:        user.Labels,
		BackgroundUrl: user.BackgroundUrl,
	}, nil
}

func (s *UserService) CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error) {
	oid, _ := primitive.ObjectIDFromHex(req.UserId)
	url := req.Url
	if url == "" {
		url = consts.DefaultAvatarUrl
	}
	if _, err = s.UserMongoMapper.Insert(ctx, &usermapper.User{
		ID:          oid,
		Name:        req.Name,
		Sex:         req.Sex,
		Url:         url,
		Description: consts.DefaultDescription,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *UserService) UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error) {
	oid, _ := primitive.ObjectIDFromHex(req.UserId)
	if _, err = s.UserMongoMapper.Update(ctx, &usermapper.User{
		ID:            oid,
		Name:          req.Name,
		Sex:           req.Sex,
		FullName:      req.FullName,
		IdCard:        req.IdCard,
		Description:   req.Description,
		Url:           req.Url,
		Labels:        req.Labels,
		BackgroundUrl: req.BackgroundUrl,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}
