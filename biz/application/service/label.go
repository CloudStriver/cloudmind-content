package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	labelmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
)

type ILabelService interface {
	GetLabel(ctx context.Context, req *content.GetLabelReq) (*content.GetLabelResp, error)
	CreateLabel(ctx context.Context, req *content.CreateLabelReq) (*content.CreateLabelResp, error)
	UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (*content.UpdateLabelResp, error)
	DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (*content.DeleteLabelResp, error)
}

type LabelService struct {
	LabelMongoMapper labelmapper.IMongoMapper
}

var LabelSet = wire.NewSet(
	wire.Struct(new(LabelService), "*"),
	wire.Bind(new(ILabelService), new(*LabelService)),
)

func (s *LabelService) GetLabel(ctx context.Context, req *content.GetLabelReq) (resp *content.GetLabelResp, err error) {
	resp = new(content.GetLabelResp)
	label, err := s.LabelMongoMapper.FindOne(ctx, req.Id)
	if err != nil {
		return resp, err
	}
	resp.Label = convertor.LabelMapperToLabel(label)
	return resp, nil
}

func (s *LabelService) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (resp *content.CreateLabelResp, err error) {
	resp = new(content.CreateLabelResp)
	if resp.Id, err = s.LabelMongoMapper.Insert(ctx, convertor.LabelToLabelMapper(req.Label)); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *LabelService) UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (resp *content.UpdateLabelResp, err error) {
	resp = new(content.UpdateLabelResp)
	if err = s.LabelMongoMapper.Update(ctx, convertor.LabelToLabelMapper(req.Label)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *LabelService) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (resp *content.DeleteLabelResp, err error) {
	resp = new(content.DeleteLabelResp)
	if _, err = s.LabelMongoMapper.Delete(ctx, req.Id); err != nil {
		return resp, err
	}
	return resp, nil
}
