package service

import (
	commentmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/comment"
	"github.com/google/wire"
)

type ICommentService interface {
	//CreateComment(ctx context.Context, req *content.CreateCommentReq) (*content.CreateCommentResp, error)
}

type CommentService struct {
	CommentModel commentmapper.IMongoMapper
}

var CommentSet = wire.NewSet(
	wire.Struct(new(CommentService), "*"),
	wire.Bind(new(ICommentService), new(*CommentService)),
)
