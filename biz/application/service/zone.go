package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	zonemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/zone"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
)

type IZoneService interface {
	GetZone(ctx context.Context, req *gencontent.GetZoneReq) (*gencontent.GetZoneResp, error)
	GetZones(ctx context.Context, req *gencontent.GetZonesReq) (*gencontent.GetZonesResp, error)
	CreateZone(ctx context.Context, req *gencontent.CreateZoneReq) (*gencontent.CreateZoneResp, error)
	UpdateZone(ctx context.Context, req *gencontent.UpdateZoneReq) (*gencontent.UpdateZoneResp, error)
	DeleteZone(ctx context.Context, req *gencontent.DeleteZoneReq) (*gencontent.DeleteZoneResp, error)
}

type ZoneService struct {
	ZoneMongoMapper zonemapper.IMongoMapper
}

var ZoneSet = wire.NewSet(
	wire.Struct(new(ZoneService), "*"),
	wire.Bind(new(IZoneService), new(*ZoneService)),
)

func (s *ZoneService) GetZone(ctx context.Context, req *gencontent.GetZoneReq) (resp *gencontent.GetZoneResp, err error) {
	resp = new(gencontent.GetZoneResp)
	label, err := s.ZoneMongoMapper.FindOne(ctx, req.Id)
	if err != nil {
		return resp, err
	}
	resp.Zone = convertor.ZoneMapperToZone(label)
	return resp, nil
}

func (s *ZoneService) GetZones(ctx context.Context, req *gencontent.GetZonesReq) (resp *gencontent.GetZonesResp, err error) {
	resp = new(gencontent.GetZonesResp)
	var total int64
	var zones []*zonemapper.Zone
	p := convertor.ParsePagination(req.PaginationOptions)
	if zones, total, err = s.ZoneMongoMapper.FindManyAndCount(ctx, req.FatherId, p, mongop.IdCursorType); err != nil {
		log.CtxError(ctx, "查询文件列表: 发生异常[%v]\n", err)
		return resp, err
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Zones = lo.Map[*zonemapper.Zone, *gencontent.Zone](zones, func(item *zonemapper.Zone, _ int) *gencontent.Zone {
		return convertor.ZoneMapperToZone(item)
	})
	return resp, nil
}

func (s *ZoneService) CreateZone(ctx context.Context, req *gencontent.CreateZoneReq) (resp *gencontent.CreateZoneResp, err error) {
	resp = new(gencontent.CreateZoneResp)
	if resp.Id, err = s.ZoneMongoMapper.Insert(ctx, convertor.ZoneToZoneMapper(req.Zone)); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ZoneService) UpdateZone(ctx context.Context, req *gencontent.UpdateZoneReq) (resp *gencontent.UpdateZoneResp, err error) {
	resp = new(gencontent.UpdateZoneResp)
	if err = s.ZoneMongoMapper.Update(ctx, convertor.ZoneToZoneMapper(req.Zone)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *ZoneService) DeleteZone(ctx context.Context, req *gencontent.DeleteZoneReq) (resp *gencontent.DeleteZoneResp, err error) {
	resp = new(gencontent.DeleteZoneResp)
	if _, err = s.ZoneMongoMapper.Delete(ctx, req.Id); err != nil {
		return resp, err
	}
	return resp, nil
}
