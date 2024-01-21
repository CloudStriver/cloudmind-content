package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type IUserService interface {
	GetUser(ctx context.Context, req *gencontent.GetUserReq) (resp *gencontent.GetUserResp, err error)
	CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error)
	UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error)
	SearchUser(ctx context.Context, req *gencontent.SearchUserReq) (resp *gencontent.SearchUserResp, err error)
	DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error)
}

type UserService struct {
	Config          *config.Config
	UserMongoMapper usermapper.IUserMongoMapper
	UserEsMapper    usermapper.IUserEsMapper
	Redis           *redis.Redis
}

var UserSet = wire.NewSet(
	wire.Struct(new(UserService), "*"),
	wire.Bind(new(IUserService), new(*UserService)),
)

func (s *UserService) DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error) {
	resp = new(gencontent.DeleteUserResp)
	if _, err = s.UserMongoMapper.Delete(ctx, req.UserId); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *UserService) SearchUser(ctx context.Context, req *gencontent.SearchUserReq) (resp *gencontent.SearchUserResp, err error) {
	resp = new(gencontent.SearchUserResp)
	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	users, total, err := s.UserEsMapper.Search(ctx, req.Keyword, p, esp.ScoreCursorType)
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
	resp = new(gencontent.GetUserResp)
	var user *usermapper.User
	if user, err = s.UserMongoMapper.FindOne(ctx, req.UserId); err != nil {
		return resp, err
	}

	resp.User = convertor.UserMapperToUser(user)
	return resp, nil
}

func (s *UserService) CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error) {
	resp = new(gencontent.CreateUserResp)
	if _, err = s.UserMongoMapper.Insert(ctx, convertor.UserToUserMapper(req.User)); err != nil {
		log.CtxError(ctx, "插入用户信息异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *UserService) UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error) {
	resp = new(gencontent.UpdateUserResp)
	if _, err = s.UserMongoMapper.Update(ctx, convertor.UserToUserMapper(req.User)); err != nil {
		log.CtxError(ctx, "修改用户信息异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}
