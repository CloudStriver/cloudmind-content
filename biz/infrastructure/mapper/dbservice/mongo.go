package dbservice

import (
	"context"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/mongo"
)

type TransactionFunc func(ctx mongo.SessionContext) error

func WithTransaction(ctx context.Context, client *monc.Model, txnFn TransactionFunc) error {
	session, err := client.StartSession()
	if err != nil {
		logx.Errorf("mongoDB开启Session失败[%v]\n", err)
		return err
	}
	defer session.EndSession(ctx)

	if err = session.StartTransaction(); err != nil {
		logx.Errorf("mongoDB开启事务失败[%v]\n", err)
		return err
	}

	err = mongo.WithSession(ctx, session, func(sessionContext mongo.SessionContext) error {
		err := txnFn(sessionContext)
		if err != nil {
			_ = session.AbortTransaction(sessionContext)
			return err
		}
		return session.CommitTransaction(sessionContext)
	})
	return err
}
