package convertor

import (
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	postmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"

	//"github.com/xh-polaris/meowchat-content/biz/infrastructure/consts"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
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

func PostFilterOptionsToFilterOptions(in *gencontent.PostFilterOptions) *postmapper.FilterOptions {
	if in == nil {
		return &postmapper.FilterOptions{}
	}
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
	if in == nil {
		return &gencontent.Post{}
	}
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

func ConvertFileSlice(data []*file.File) []*gencontent.FileInfo {
	res := make([]*gencontent.FileInfo, len(data))
	for i, d := range data {
		m := &gencontent.FileInfo{
			FileId:    d.ID.Hex(),
			Name:      d.Name,
			Type:      gencontent.Type(d.Type),
			Path:      d.Path,
			UserId:    d.UserId,
			FatherId:  d.FatherId,
			SpaceSize: *d.Size,
			Md5:       d.Md5,
			UpdateAt:  d.UpdateAt.Unix(),
		}
		res[i] = m
	}
	return res
}

func FileMapperToFile(data *file.File) *gencontent.FileInfo {
	return &gencontent.FileInfo{
		FileId:    data.ID.Hex(),
		Name:      data.Name,
		Type:      gencontent.Type(data.Type),
		Path:      data.Path,
		UserId:    data.UserId,
		FatherId:  data.FatherId,
		SpaceSize: *data.Size,
		Md5:       data.Md5,
		UpdateAt:  data.UpdateAt.Unix(),
	}
}

func FileToFileMapper(data *gencontent.File) (*file.File, error) {
	var err error
	var oid primitive.ObjectID
	if data.FileId == "" {
		oid = primitive.NewObjectID()
	} else {
		if oid, err = primitive.ObjectIDFromHex(data.FileId); err != nil {
			log.Error("更新文件信息: 发生异常[%v]\n", err)
			return nil, consts.ErrInvalidId
		}
	}
	return &file.File{
		ID:          oid,
		UserId:      data.UserId,
		Name:        data.Name,
		Type:        int64(data.Type),
		Path:        data.Path,
		FatherId:    data.FatherId,
		Size:        data.SpaceSize,
		Md5:         data.Md5,
		IsDel:       data.IsDel,
		Tags:        data.Tag,
		Description: data.Description,
	}, nil
}

// 0:永久有效 1:有效 2:已失效 ...

func IsExpired(ctime time.Time, effectiveTime int64) int64 {
	if effectiveTime < 0 {
		return 0
	}
	now := time.Now()
	ctime = ctime.Add(time.Duration(effectiveTime) * time.Second)
	if now.After(ctime) {
		return 2
	} else {
		return 1
	}
}

func ShareFileMapperToShareFile(data *sharefile.ShareFile) *gencontent.ShareFile {
	return &gencontent.ShareFile{
		Code:          data.ID.Hex(),
		UserId:        data.UserId,
		Name:          data.Name,
		Status:        IsExpired(data.CreateAt, data.EffectiveTime),
		EffectiveTime: data.EffectiveTime,
		BrowseNumber:  *data.BrowseNumber,
		CreateAt:      data.CreateAt.Unix(),
		FileList:      data.FileList,
	}
}

func ShareFileToShareFileMapper(data *gencontent.ShareFile) (*sharefile.ShareFile, error) {
	var err error
	var oid primitive.ObjectID
	if data.Code == "" {
		oid = primitive.NewObjectID()
	} else {
		if oid, err = primitive.ObjectIDFromHex(data.Code); err != nil {
			log.Error("更新文件信息: 发生异常[%v]\n", err)
			return nil, consts.ErrInvalidId
		}
	}

	return &sharefile.ShareFile{
		ID:            oid,
		UserId:        data.UserId,
		Name:          data.Name,
		FileList:      data.FileList,
		EffectiveTime: data.EffectiveTime,
		BrowseNumber:  &data.BrowseNumber,
	}, nil
}

func ShareFileToShareCode(data *sharefile.ShareFile) *gencontent.ShareCode {
	return &gencontent.ShareCode{
		Code:         data.ID.Hex(),
		Name:         data.Name,
		Status:       IsExpired(data.CreateAt, data.EffectiveTime),
		BrowseNumber: *data.BrowseNumber,
		CreateAt:     data.CreateAt.Unix(),
	}
}

func FileFilterOptionsToFilterOptions(opts *gencontent.FileFilterOptions) (filter *file.FilterOptions) {
	if opts == nil {
		filter = &file.FilterOptions{}
	} else {
		filter = &file.FilterOptions{
			OnlyUserId:       opts.OnlyUserId,
			OnlyFatherId:     opts.OnlyFatherId,
			OnlyFileType:     opts.OnlyFileType,
			OnlyIsDel:        lo.ToPtr(opts.IsDel),
			OnlyDocumentType: lo.ToPtr(opts.DocumentType),
			OnlyTags:         opts.OnlyTags,
		}
		if opts.OnlyFileId != nil {
			filter.OnlyFileIds = []string{*opts.OnlyFileId}
		}
	}
	return
}

func ShareFileFilterOptionsToShareCodeOptions(opts *gencontent.ShareFileFilterOptions) (filter *sharefile.ShareCodeOptions) {
	if opts == nil {
		filter = &sharefile.ShareCodeOptions{}
	} else {
		filter = &sharefile.ShareCodeOptions{
			OnlyCode:   opts.OnlyCode,
			OnlyUserId: opts.OnlyUserId,
		}
	}
	return
}

func ParsePagination(opts *basic.PaginationOptions) (p *pagination.PaginationOptions) {
	if opts == nil {
		p = &pagination.PaginationOptions{}
	} else {
		p = &pagination.PaginationOptions{
			Limit:     opts.Limit,
			Offset:    opts.Offset,
			Backward:  opts.Backward,
			LastToken: opts.LastToken,
		}
	}
	return
}

func LabelMapperToLabel(data *label.Label) *gencontent.Label {
	return &gencontent.Label{
		Id:    data.ID.Hex(),
		Value: data.Value,
	}
}

func ConvertPostAllFieldsSearchQuery(in *gencontent.SearchOptions_AllFieldsKey) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in.AllFieldsKey,
			Fields: []string{consts.Name + "^3", consts.ID, consts.Tags, consts.Title + "^3", consts.Text},
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
	if in.MultiFieldsKey.Name != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Name: {
					Query: *in.MultiFieldsKey.Name + "^3",
				},
			},
		})
	}
	if in.MultiFieldsKey.Id != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.ID: {
					Query: *in.MultiFieldsKey.Id,
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
