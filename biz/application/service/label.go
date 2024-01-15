package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	labelmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (s *LabelService) GetLabel(ctx context.Context, req *content.GetLabelReq) (*content.GetLabelResp, error) {
	resp := new(content.GetLabelResp)
	label, err := s.LabelMongoMapper.FindOne(ctx, req.Id)
	if err != nil {
		return resp, err
	}

	resp.Label.Value = label.Value
	return resp, nil
}

func (s *LabelService) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (*content.CreateLabelResp, error) {
	resp := new(content.CreateLabelResp)
	label := &labelmapper.Label{Value: req.Label.Value}
	if req.Label.Id != "" {
		oid, err := primitive.ObjectIDFromHex(req.Label.Id)
		if err != nil {
			return nil, consts.ErrInvalidId
		}
		label.ID = oid
	}

	id, err := s.LabelMongoMapper.Insert(ctx, label)
	if err != nil {
		return nil, err
	}

	resp.Id = id
	return resp, nil
}

func (s *LabelService) UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (*content.UpdateLabelResp, error) {
	resp := new(content.UpdateLabelResp)
	label := &labelmapper.Label{Value: req.Label.Value}
	if req.Label.Id != "" {
		oid, err := primitive.ObjectIDFromHex(req.Label.Id)
		if err != nil {
			return nil, consts.ErrInvalidId
		}
		label.ID = oid
	}

	if err := s.LabelMongoMapper.Update(ctx, label); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *LabelService) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (*content.DeleteLabelResp, error) {
	resp := new(content.DeleteLabelResp)
	if _, err := s.LabelMongoMapper.Delete(ctx, req.Id); err != nil {
		return nil, err
	}
	return resp, nil
}
