package convertor

import (
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func UserMapperToUserDetail(in *usermapper.User) *gencontent.UserDetail {
	return &gencontent.UserDetail{
		Name:        in.Name,
		Sex:         in.Sex,
		FullName:    in.FullName,
		IdCard:      in.IdCard,
		CreatedAt:   in.CreateAt.UnixMilli(),
		UpdatedAt:   in.UpdateAt.UnixMilli(),
		Description: in.Description,
		Url:         in.Url,
		UserId:      in.ID.Hex(),
	}
}

func UserDetailToUserMapper(in *gencontent.UserDetailInfo) *usermapper.User {
	ID, _ := primitive.ObjectIDFromHex(in.UserId)
	return &usermapper.User{
		ID:          ID,
		Name:        in.Name,
		Sex:         int32(in.GetSex()),
		FullName:    in.FullName,
		IdCard:      in.IdCard,
		Description: in.Description,
		Url:         in.Url,
	}
}

func UserMapperToUser(in *usermapper.User) *gencontent.User {
	return &gencontent.User{
		UserId: in.ID.Hex(),
		Name:   in.Name,
		Url:    in.Url,
	}
}
