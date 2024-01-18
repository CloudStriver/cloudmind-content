package convertor

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

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
			Md5:       d.FileMd5,
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
		Md5:       data.FileMd5,
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
		FileMd5:     data.Md5,
		IsDel:       data.IsDel,
		Tag:         data.Tag,
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
			OnlyUserId:   opts.OnlyUserId,
			OnlyFileId:   opts.OnlyFileId,
			OnlyFatherId: opts.OnlyFatherId,
			OnlyFileType: opts.OnlyFileType,
			OnlyTag:      opts.OnlyTag,
			IsDel:        opts.IsDel,
			DocumentType: opts.DocumentType,
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
			Fields: []string{consts.Name + "^3", consts.ID, consts.Tag},
		}},
	}
}

func ConvertPostMultiFieldsSearchQuery(in *gencontent.SearchOptions_MultiFieldsKey) []types.Query {
	var q []types.Query
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
				consts.Tag: {
					Query: *in.MultiFieldsKey.Tag,
				},
			},
		})
	}
	return q
}
