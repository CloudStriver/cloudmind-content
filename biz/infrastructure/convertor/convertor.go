package convertor

import (
	"fmt"
	postmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/xh-polaris/meowchat-content/biz/infrastructure/consts"
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
	fmt.Println(in)
	ID, _ := primitive.ObjectIDFromHex(in.UserId)
	return &usermapper.User{
		ID:          ID,
		Name:        in.Name,
		Sex:         in.GetSex(),
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

func PostInfoToPostMapper(in *gencontent.PostInfo) *postmapper.Post {
	post := &postmapper.Post{
		Title:  in.Title,
		Text:   in.Text,
		Url:    in.Url,
		Tags:   in.Tags,
		UserId: in.UserId,
		Status: in.Status,
	}
	if in.PostId != "" {
		ID, _ := primitive.ObjectIDFromHex(in.PostId)
		post.ID = ID
	}
	return post
}

func PostMapperToPostInfo(in *postmapper.Post) *gencontent.Post {
	return &gencontent.Post{
		PostId: in.ID.Hex(),
		Title:  in.Title,
		Text:   in.Text,
		Url:    in.Url,
		Tags:   in.Tags,
		UserId: in.UserId,
		Status: in.Status,
	}
}

func PostFilterOptionsToFilterOptions(in *gencontent.PostFilterOptions) *postmapper.FilterOptions {
	return &postmapper.FilterOptions{
		OnlyUserId: in.OnlyUserId,
		OnlyPostId: in.OnlyPostId,
		OnlyTitle:  in.OnlyTitle,
		OnlyText:   in.OnlyText,
		OnlyTag:    in.OnlyTag,
		OnlyStatus: in.OnlyStatus,
	}
}

func PostMapperToPost(in *postmapper.Post) *gencontent.Post {
	return &gencontent.Post{
		PostId:     in.ID.Hex(),
		UserId:     in.UserId,
		Title:      in.Title,
		Text:       in.Text,
		Tags:       in.Tags,
		Status:     in.Status,
		Url:        in.Url,
		CreateTime: in.CreateAt.UnixMilli(),
		UpdateTime: in.UpdateAt.UnixMilli(),
	}
}

func ConvertPostAllFieldsSearchQuery(in *gencontent.SearchOptions_AllFieldsKey) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in.AllFieldsKey,
			Fields: []string{consts.Title + "^3", consts.Text, consts.Tags},
		}},
	}
}

func ConvertPostMultiFieldsSearchQuery(in *gencontent.SearchOptions_MultiFieldsKey) []types.Query {
	var q []types.Query
	if in.MultiFieldsKey.Title != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Title: {
					Query: *in.MultiFieldsKey.Title + "^3",
				},
			},
		})
	}
	if in.MultiFieldsKey.Text != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Text: {
					Query: *in.MultiFieldsKey.Text,
				},
			},
		})
	}
	if in.MultiFieldsKey.Tag != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Tags: {
					Query: *in.MultiFieldsKey.Tag,
				},
			},
		})
	}
	return q
}
