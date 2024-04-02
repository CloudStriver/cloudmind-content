package kq

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/zeromicro/go-queue/kq"
)

type DeleteFileRelationKq struct {
	*kq.Pusher
}

func NewDeleteFileRelationKq(c *config.Config) *DeleteFileRelationKq {
	pusher := kq.NewPusher(c.DeleteFileRelationKq.Brokers, c.DeleteFileRelationKq.Topic)
	return &DeleteFileRelationKq{
		Pusher: pusher,
	}
}
