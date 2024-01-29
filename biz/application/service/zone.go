package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	zonemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
)

type IZoneService interface {
	GetZone(ctx context.Context, req *content.GetZoneReq) (*content.GetLabelResp, error)
	CreateZone(ctx context.Context, req *content.CreateLabelReq) (*content.CreateLabelResp, error)
	UpdateZone(ctx context.Context, req *content.UpdateLabelReq) (*content.UpdateLabelResp, error)
	DeleteZone(ctx context.Context, req *content.DeleteLabelReq) (*content.DeleteLabelResp, error)
}

type ZoneService struct {
	LabelMongoMapper zonemapper.IMongoMapper
}

var ZoneSet = wire.NewSet(
	wire.Struct(new(ZoneService), "*"),
	wire.Bind(new(IZoneService), new(*ZoneService)),
)

func (s *ZoneService) GetLabel(ctx context.Context, req *content.GetLabelReq) (resp *content.GetLabelResp, err error) {
	resp = new(content.GetLabelResp)
	label, err := s.LabelMongoMapper.FindOne(ctx, req.Id)
	if err != nil {
		return resp, err
	}
	resp.Zone = convertor.LabelMapperToLabel(label)
	return resp, nil
}

func (s *ZoneService) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (resp *content.CreateLabelResp, err error) {
	resp = new(content.CreateLabelResp)
	if resp.Id, err = s.LabelMongoMapper.Insert(ctx, convertor.LabelToLabelMapper(req.Zone)); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ZoneService) UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (resp *content.UpdateLabelResp, err error) {
	resp = new(content.UpdateLabelResp)
	if err = s.LabelMongoMapper.Update(ctx, convertor.LabelToLabelMapper(req.Zone)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *ZoneService) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (resp *content.DeleteLabelResp, err error) {
	resp = new(content.DeleteLabelResp)
	if _, err = s.LabelMongoMapper.Delete(ctx, req.Id); err != nil {
		return resp, err
	}
	return resp, nil
}
