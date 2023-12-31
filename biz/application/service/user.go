package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserService interface {
	GetUser(ctx context.Context, req *gencontent.GetUserReq) (resp *gencontent.GetUserResp, err error)
	CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error)
	UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error)
	GetUserDetail(ctx context.Context, req *gencontent.GetUserDetailReq) (resp *gencontent.GetUserDetailResp, err error)
	SearchUser(ctx context.Context, req *gencontent.SearchUserReq) (resp *gencontent.SearchUserResp, err error)
	DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error)
}

type UserServiceImpl struct {
	Config          *config.Config
	UserMongoMapper usermapper.UserMongoMapper
	UserEsMapper    usermapper.UserEsMapper
	Redis           *redis.Redis
}

var UserSet = wire.NewSet(
	wire.Struct(new(UserServiceImpl), "*"),
	wire.Bind(new(UserService), new(*UserServiceImpl)),
)

func (s *UserServiceImpl) DeleteUser(ctx context.Context, req *gencontent.DeleteUserReq) (resp *gencontent.DeleteUserResp, err error) {
	resp = new(gencontent.DeleteUserResp)
	_, err = s.UserMongoMapper.Delete(ctx, req.UserId)
	if err != nil {
		log.CtxError(ctx, "删除用户信息异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *UserServiceImpl) SearchUser(ctx context.Context, req *gencontent.SearchUserReq) (resp *gencontent.SearchUserResp, err error) {
	resp = new(gencontent.SearchUserResp)
	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	user, total, err := s.UserEsMapper.Search(ctx, req.Keyword, p, esp.ScoreCursorType)
	if err != nil {
		log.CtxError(ctx, "搜索用户信息异常[%v]\n", err)
		return resp, err
	}

	if p.LastToken != nil {
		resp.LastToken = *p.LastToken
	}
	resp.Total = total
	resp.Users = make([]*gencontent.User, 0, len(user))
	for _, u := range user {
		resp.Users = append(resp.Users, convertor.UserMapperToUser(u))
	}

	return resp, nil
}

func (s *UserServiceImpl) GetUserDetail(ctx context.Context, req *gencontent.GetUserDetailReq) (resp *gencontent.GetUserDetailResp, err error) {
	resp = new(gencontent.GetUserDetailResp)
	var user *usermapper.User
	user, err = s.UserMongoMapper.FindOne(ctx, req.UserId)
	if err != nil {
		log.CtxError(ctx, "查询用户信息异常[%v]\n", err)
		return resp, err
	}

	resp.UserDetail = convertor.UserMapperToUserDetail(user)
	return resp, nil
}

func (s *UserServiceImpl) GetUser(ctx context.Context, req *gencontent.GetUserReq) (resp *gencontent.GetUserResp, err error) {
	resp = new(gencontent.GetUserResp)
	var user *usermapper.User
	user, err = s.UserMongoMapper.FindOne(ctx, req.UserId)
	if err != nil {
		log.CtxError(ctx, "查询用户信息异常[%v]\n", err)
		return resp, err
	}

	resp.User = convertor.UserMapperToUser(user)
	return resp, nil
}

func (s *UserServiceImpl) CreateUser(ctx context.Context, req *gencontent.CreateUserReq) (resp *gencontent.CreateUserResp, err error) {
	resp = new(gencontent.CreateUserResp)
	ID, err := primitive.ObjectIDFromHex(req.UserInfo.UserId)
	if err != nil {
		return resp, consts.ErrInvalidObjectId
	}
	if _, err = s.UserMongoMapper.Insert(ctx, &usermapper.User{
		ID:          ID,
		Name:        req.UserInfo.Name,
		Sex:         int32(req.UserInfo.Sex),
		Description: consts.DefaultDescription,
		Url:         consts.DefaultAvatarUrl,
	}); err != nil {
		log.CtxError(ctx, "插入用户信息异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *UserServiceImpl) UpdateUser(ctx context.Context, req *gencontent.UpdateUserReq) (resp *gencontent.UpdateUserResp, err error) {
	resp = new(gencontent.UpdateUserResp)
	if _, err = s.UserMongoMapper.Update(ctx, convertor.UserDetailToUserMapper(req.UserDetailInfo)); err != nil {
		log.CtxError(ctx, "修改用户信息异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}
