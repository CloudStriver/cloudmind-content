package convertor

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	couponmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/coupon"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	ordermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/order"
	postmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/post"
	productmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/product"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/publicfile"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	usermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/user"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

func UserMapperToUser(in *usermapper.User) *gencontent.User {
	return &gencontent.User{
		UserId:      in.ID.Hex(),
		Name:        in.Name,
		Sex:         in.Sex,
		FullName:    in.FullName,
		IdCard:      in.IdCard,
		CreateTime:  in.CreateAt.UnixMilli(),
		UpdateTime:  in.UpdateAt.UnixMilli(),
		Description: in.Description,
		Url:         in.Url,
		Labels:      in.Labels,
	}
}

func FileMapperToFile(data *file.File) *gencontent.File {
	return &gencontent.File{
		FileId:    data.ID.Hex(),
		UserId:    data.UserId,
		Name:      data.Name,
		Type:      data.Type,
		Path:      data.Path,
		FatherId:  data.FatherId,
		SpaceSize: data.Size,
		Md5:       data.FileMd5,
		IsDel:     data.IsDel,
		CreateAt:  data.CreateAt.UnixMilli(),
		UpdateAt:  data.UpdateAt.UnixMilli(),
		DeleteAt:  data.DeletedAt.UnixMilli(),
	}
}

func PublicFileMapperToPublicFile(data *publicfile.PublicFile) *gencontent.PublicFile {
	return &gencontent.PublicFile{
		FileId:      data.ID.Hex(),
		UserId:      data.UserId,
		Name:        data.Name,
		Type:        data.Type,
		SpaceSize:   data.Size,
		Md5:         data.FileMd5,
		Zone:        data.Zone,
		Description: data.Description,
		AuditStatus: data.AuditStatus,
		Labels:      data.Labels,
		CreateAt:    data.CreateAt.UnixMilli(),
	}
}

func IsExpired(ctime time.Time, effectiveTime int64) int64 {
	if effectiveTime < 0 {
		return int64(gencontent.Validity_Validity_perpetuity)
	}
	now := time.Now()
	ctime = ctime.Add(time.Duration(effectiveTime) * time.Second)
	if now.After(ctime) {
		return int64(gencontent.Validity_Validity_expired)
	} else {
		return int64(gencontent.Validity_Validity_temporary)
	}
}

func ShareFileToShareCode(data *sharefile.ShareFile) *gencontent.ShareCode {
	return &gencontent.ShareCode{
		Code:         data.ID.Hex(),
		Name:         data.Name,
		Status:       IsExpired(data.CreateAt, data.EffectiveTime),
		BrowseNumber: *data.BrowseNumber,
		CreateAt:     data.CreateAt.Unix(),
		Key:          data.Key,
	}
}

func FileFilterOptionsToFilterOptions(opts *gencontent.FileFilterOptions) (filter *file.FilterOptions) {
	if opts == nil {
		filter = &file.FilterOptions{}
	} else {
		filter = &file.FilterOptions{
			OnlyUserId:   opts.OnlyUserId,
			OnlyFatherId: opts.OnlyFatherId,
			OnlyIsDel:    opts.OnlyIsDel,
			OnlyType:     opts.OnlyType,
			OnlyCategory: opts.OnlyCategory,
		}
	}

	return filter
}

func PublicFilterOptionsToFilterOptions(opts *gencontent.PublicFileFilterOptions) (filter *publicfile.FilterOptions) {
	if opts == nil {
		filter = &publicfile.FilterOptions{}
	} else {
		filter = &publicfile.FilterOptions{
			OnlyUserId:      opts.OnlyUserId,
			OnlyZone:        opts.OnlyZone,
			OnlyType:        opts.OnlyType,
			OnlyAuditStatus: opts.OnlyAuditStatus,
			OnlyLabelId:     opts.OnlyLabelId,
		}
	}

	return filter
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

func PostFilterOptionsToFilterOptions(in *gencontent.PostFilterOptions) *postmapper.FilterOptions {
	if in == nil {
		return &postmapper.FilterOptions{}
	}
	return &postmapper.FilterOptions{
		OnlyUserId:  in.OnlyUserId,
		OnlyLabelId: in.OnlyLabelId,
		OnlyStatus:  in.OnlyStatus,
	}
}

func PostMapperToPost(in *postmapper.Post) *gencontent.Post {
	if in == nil {
		return &gencontent.Post{}
	}
	text := in.Text
	if len(text) > 200 {
		text = text[0:200]
	}
	return &gencontent.Post{
		PostId:     in.ID.Hex(),
		UserId:     in.UserId,
		Title:      in.Title,
		Text:       text,
		LabelIds:   in.LabelIds,
		Status:     in.Status,
		Url:        in.Url,
		CreateTime: in.CreateAt.UnixMilli(),
		UpdateTime: in.UpdateAt.UnixMilli(),
	}
}

func ConvertFileAllFieldsSearchQuery(in string) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in,
			Fields: []string{consts.Name},
		}},
	}
}

func ConvertFileMultiFieldsSearchQuery(in *gencontent.SearchOptions_MultiFieldsKey) []types.Query {
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
	if in.MultiFieldsKey.Description != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Description: {
					Query: *in.MultiFieldsKey.Description + "^3",
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
	return q
}

func ProductFilterOptionsToFilterOptions(in *gencontent.ProductFilterOptions) *productmapper.FilterOptions {
	if in == nil {
		return &productmapper.FilterOptions{}
	}
	return &productmapper.FilterOptions{
		OnlyUserId:      in.OnlyUserId,
		OnlyProductId:   in.OnlyProductId,
		OnlyProductIds:  in.OnlyProductIds,
		OnlyTags:        in.OnlyTags,
		OnlySetRelation: in.OnlySetRelation,
		OnlyStatus:      in.OnlyStatus,
		OnlyType:        in.OnlyType,
	}
}

func ProductMapperToProduct(in *productmapper.Product) *gencontent.Product {
	if in == nil {
		return &gencontent.Product{}
	}
	return &gencontent.Product{
		ProductId:   in.ID.Hex(),
		UserId:      in.UserId,
		Name:        in.Name,
		Description: in.Description,
		Status:      in.Status,
		Urls:        in.Urls,
		Tags:        in.Tags,
		Type:        in.Type,
		Price:       in.Price,
		ProductSize: in.ProductSize,
		CreateTime:  in.CreateAt.UnixMilli(),
		UpdateTime:  in.UpdateAt.UnixMilli(),
	}
}

func ConvertProductAllFieldsSearchQuery(in *gencontent.SearchOptions_AllFieldsKey) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in.AllFieldsKey,
			Fields: []string{consts.Name + "^3", consts.Description},
		}},
	}
}

func OrderFilterOptionsToFilterOptions(in *gencontent.OrderFilterOptions) *ordermapper.FilterOptions {
	if in == nil {
		return &ordermapper.FilterOptions{}
	}
	return &ordermapper.FilterOptions{
		OnlyUserId:    in.OnlyUserId,
		OnlyOrderId:   in.OnlyOrderId,
		OnlyOrderIds:  in.OnlyOrderIds,
		OnlyStatus:    in.OnlyStatus,
		OnlyProductId: in.OnlyProductId,
	}
}

func OrderToOrderMapper(in *gencontent.Order) *ordermapper.Order {
	oid, _ := primitive.ObjectIDFromHex(in.OrderId)
	return &ordermapper.Order{
		ID:          oid,
		UserId:      in.UserId,
		ProductId:   in.ProductId,
		Status:      in.Status,
		SumPrice:    in.SumPrice,
		ProductName: in.ProductName,
	}
}

func OrderMapperToOrder(in *ordermapper.Order) *gencontent.Order {
	if in == nil {
		return &gencontent.Order{}
	}
	return &gencontent.Order{
		OrderId:     in.ID.Hex(),
		UserId:      in.UserId,
		ProductId:   in.ProductId,
		Status:      in.Status,
		SumPrice:    in.SumPrice,
		CreateTime:  in.CreateAt.UnixMilli(),
		UpdateTime:  in.UpdateAt.UnixMilli(),
		ProductName: in.ProductName,
	}
}

func ConvertOrderAllFieldsSearchQuery(in *gencontent.SearchOptions_AllFieldsKey) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in.AllFieldsKey,
			Fields: []string{consts.ProductName + "^3", consts.ID},
		}},
	}
}

func ConvertOrderMultiFieldsSearchQuery(in *gencontent.SearchOptions_MultiFieldsKey) []types.Query {
	var q []types.Query
	if in.MultiFieldsKey.ProductName != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.ProductName: {
					Query: *in.MultiFieldsKey.ProductName + "^3",
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
	return q
}

func CouponFilterOptionsToFilterOptions(in *gencontent.CouponFilterOptions) *couponmapper.FilterOptions {
	if in == nil {
		return &couponmapper.FilterOptions{}
	}
	return &couponmapper.FilterOptions{
		OnlyUserId:    in.OnlyUserId,
		OnlyCouponId:  in.OnlyCouponId,
		OnlyCouponIds: in.OnlyCouponIds,
		OnlyStatus:    in.OnlyStatus,
		OnlyType:      in.OnlyProductType,
	}
}

func CouponToCouponMapper(in *gencontent.Coupon) *couponmapper.Coupon {
	oid, _ := primitive.ObjectIDFromHex(in.CouponId)
	return &couponmapper.Coupon{
		ID:            oid,
		UserId:        in.UserId,
		Name:          in.Name,
		Status:        in.Status,
		Description:   in.Description,
		Type:          in.ProductType,
		LowSumPrice:   in.LowSumPrice,
		ProductType:   in.ProductType,
		Discount:      in.Discount,
		DiscountPrice: in.DiscountPrice,
	}
}

func CouponMapperToCoupon(in *couponmapper.Coupon) *gencontent.Coupon {
	if in == nil {
		return &gencontent.Coupon{}
	}
	return &gencontent.Coupon{
		CouponId:      in.ID.Hex(),
		UserId:        in.UserId,
		Status:        in.Status,
		CreateTime:    in.CreateAt.UnixMilli(),
		ExpireTime:    in.ExpireTime,
		Name:          in.Name,
		Description:   in.Description,
		LowSumPrice:   in.LowSumPrice,
		ProductType:   in.ProductType,
		Discount:      in.Discount,
		DiscountPrice: in.DiscountPrice,
	}
}

func ConvertCouponAllFieldsSearchQuery(in *gencontent.SearchOptions_AllFieldsKey) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in.AllFieldsKey,
			Fields: []string{consts.Name + "^3", consts.ID, consts.Description},
		}},
	}
}

func ConvertCouponMultiFieldsSearchQuery(in *gencontent.SearchOptions_MultiFieldsKey) []types.Query {
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
	if in.MultiFieldsKey.Description != nil {
		q = append(q, types.Query{
			Match: map[string]types.MatchQuery{
				consts.Description: {
					Query: *in.MultiFieldsKey.Description,
				},
			},
		})
	}
	return q
}

func ConvertPublicFileAllFieldsSearchQuery(in string) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in,
			Fields: []string{consts.Name + "^3", consts.Description + "^3"},
		}},
	}
}

func ConvertUserAllFieldsSearchQuery(in string) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in,
			Fields: []string{consts.Name + "^3", consts.Description},
		}},
	}
}

func ConvertPostAllFieldsSearchQuery(in string) []types.Query {
	return []types.Query{{
		MultiMatch: &types.MultiMatchQuery{
			Query:  in,
			Fields: []string{consts.Title + "^3", consts.Text},
		}},
	}
}
