package sharefile

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ShareCodeOptions struct {
	OnlyCode   *string
	OnlyUserId *string
}

type MongoShareCodeFilter struct {
	m bson.M
	*ShareCodeOptions
}

func makeMongoShareCodeFilter(options *ShareCodeOptions) bson.M {
	return (&MongoShareCodeFilter{
		m:                bson.M{},
		ShareCodeOptions: options,
	}).toBson()
}

func (f *MongoShareCodeFilter) toBson() bson.M {
	f.CheckOnlyCode()
	f.CheckOnlyUserId()
	return f.m
}

func (f *MongoShareCodeFilter) CheckOnlyCode() {
	if f.OnlyCode != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyCode)
		f.m[consts.ID] = oid
	}
}

func (f *MongoShareCodeFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}
