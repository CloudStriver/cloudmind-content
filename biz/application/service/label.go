package service

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	labelmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/label"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/jinzhu/copier"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ILabelService interface {
	GetLabel(ctx context.Context, req *content.GetLabelReq) (res *content.GetLabelResp, err error)
	CreateLabel(ctx context.Context, req *content.CreateLabelReq) (res *content.CreateLabelResp, err error)
	UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (res *content.UpdateLabelResp, err error)
	DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (res *content.DeleteLabelResp, err error)
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
	if _, err := primitive.ObjectIDFromHex(req.Id); err != nil {
		return resp, consts.ErrInvalidId
	}

	Label, err := s.LabelMongoMapper.FindOne(ctx, req.Id)
	if errorx.Is(err, consts.ErrNotFound) {
		return resp, consts.ErrNoSuchLabel
	} else if err != nil {
		return resp, err
	}

	resp.Label.Value = Label.Value
	return resp, nil
}

func (s *LabelService) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (*content.CreateLabelResp, error) {
	resp := new(content.CreateLabelResp)
	label := &labelmapper.Label{}
	err := copier.Copy(label, req.Label)
	if err != nil {
		return nil, err
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
	label := &labelmapper.Label{}
	err := copier.Copy(&label, req.Label)
	if err != nil {
		return nil, err
	}
	label.ID, err = primitive.ObjectIDFromHex(req.Label.Id)
	if err != nil {
		return nil, consts.ErrInvalidId
	}
	err = s.LabelMongoMapper.Update(ctx, label)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *LabelService) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (*content.DeleteLabelResp, error) {
	resp := new(content.DeleteLabelResp)
	_, err := s.LabelMongoMapper.Delete(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
