package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	couponmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/coupon"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type ICouponService interface {
	GetCoupon(ctx context.Context, req *gencontent.GetCouponReq) (resp *gencontent.GetCouponResp, err error)
	GetCoupons(ctx context.Context, req *gencontent.GetCouponsReq) (resp *gencontent.GetCouponsResp, err error)
	CreateCoupon(ctx context.Context, req *gencontent.CreateCouponReq) (resp *gencontent.CreateCouponResp, err error)
	UpdateCoupon(ctx context.Context, req *gencontent.UpdateCouponReq) (resp *gencontent.UpdateCouponResp, err error)
	DeleteCoupon(ctx context.Context, req *gencontent.DeleteCouponReq) (resp *gencontent.DeleteCouponResp, err error)
}

var CouponSet = wire.NewSet(
	wire.Struct(new(CouponService), "*"),
	wire.Bind(new(ICouponService), new(*CouponService)),
)

type CouponService struct {
	Config            *config.Config
	CouponMongoMapper couponmapper.ICouponMongoMapper
	CouponEsMapper    couponmapper.IEsMapper
	Redis             *redis.Redis
}

func (s *CouponService) GetCoupon(ctx context.Context, req *gencontent.GetCouponReq) (resp *gencontent.GetCouponResp, err error) {
	resp = new(gencontent.GetCouponResp)
	coupon, err := s.CouponMongoMapper.FindOne(ctx, convertor.CouponFilterOptionsToFilterOptions(req.CouponFilterOptions))
	if err != nil {
		return resp, err
	}
	resp.Coupon = convertor.CouponMapperToCoupon(coupon)
	return resp, nil
}

func (s *CouponService) GetCoupons(ctx context.Context, req *gencontent.GetCouponsReq) (resp *gencontent.GetCouponsResp, err error) {
	resp = new(gencontent.GetCouponsResp)
	var (
		total   int64
		coupons []*couponmapper.Coupon
	)
	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	filter := convertor.CouponFilterOptionsToFilterOptions(req.CouponFilterOptions)
	if req.SearchOptions != nil {
		switch o := req.SearchOptions.Type.(type) {
		case *gencontent.SearchOptions_AllFieldsKey:
			coupons, total, err = s.CouponEsMapper.Search(ctx, convertor.ConvertCouponAllFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		case *gencontent.SearchOptions_MultiFieldsKey:
			coupons, total, err = s.CouponEsMapper.Search(ctx, convertor.ConvertCouponMultiFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		}
	} else {
		coupons, total, err = s.CouponMongoMapper.FindManyAndCount(ctx, convertor.CouponFilterOptionsToFilterOptions(req.CouponFilterOptions),
			p, mongop.IdCursorType)
	}
	if err != nil {
		return resp, err
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Coupons = lo.Map[*couponmapper.Coupon, *gencontent.Coupon](coupons, func(item *couponmapper.Coupon, _ int) *gencontent.Coupon {
		return convertor.CouponMapperToCoupon(item)
	})
	return resp, nil
}

func (s *CouponService) CreateCoupon(ctx context.Context, req *gencontent.CreateCouponReq) (resp *gencontent.CreateCouponResp, err error) {
	resp = new(gencontent.CreateCouponResp)
	if err = s.CouponMongoMapper.Insert(ctx, convertor.CouponToCouponMapper(req.Coupon)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *CouponService) UpdateCoupon(ctx context.Context, req *gencontent.UpdateCouponReq) (resp *gencontent.UpdateCouponResp, err error) {
	resp = new(gencontent.UpdateCouponResp)
	if err = s.CouponMongoMapper.Update(ctx, convertor.CouponToCouponMapper(req.Coupon)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *CouponService) DeleteCoupon(ctx context.Context, req *gencontent.DeleteCouponReq) (resp *gencontent.DeleteCouponResp, err error) {
	resp = new(gencontent.DeleteCouponResp)
	if err = s.CouponMongoMapper.Delete(ctx, req.CouponId); err != nil {
		return resp, err
	}
	return resp, nil
}
