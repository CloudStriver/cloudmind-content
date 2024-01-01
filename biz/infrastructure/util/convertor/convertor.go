package convertor

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
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
			UpdateAt:  d.CreateAt.Unix(),
		}
		res[i] = m
	}
	return res
}

func ConvertFile(data *file.File) *gencontent.FileInfo {
	return &gencontent.FileInfo{
		FileId:    data.ID.Hex(),
		Name:      data.Name,
		Type:      gencontent.Type(data.Type),
		Path:      data.Path,
		UserId:    data.UserId,
		FatherId:  data.FatherId,
		SpaceSize: *data.Size,
		Md5:       data.FileMd5,
		UpdateAt:  data.CreateAt.Unix(),
	}
}

func ParseFileFilter(opts *gencontent.FileFilterOptions) (filter *file.FilterOptions) {
	if opts == nil {
		filter = &file.FilterOptions{}
	} else {
		filter = &file.FilterOptions{
			OnlyUserId:   opts.OnlyUserId,
			OnlyFileId:   opts.OnlyFileId,
			OnlyFatherId: opts.OnlyFatherId,
			OnlyFileType: opts.OnlyFileType,
			IsDel:        opts.IsDel,
			DocumentType: opts.DocumentType,
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

func ConvertLabel(data *label.Label) *gencontent.Label {
	return &gencontent.Label{
		Id:    data.ID.Hex(),
		Value: data.Value,
	}
}
