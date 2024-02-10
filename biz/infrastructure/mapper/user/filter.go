package user

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserIds []string
}

type MongoFilter struct {
	m bson.M
	*FilterOptions
}

func MakeBsonFilter(options *FilterOptions) bson.M {
	return (&MongoFilter{
		m:             bson.M{},
		FilterOptions: options,
	}).toBson()
}

func (f *MongoFilter) toBson() bson.M {
	f.CheckOnlyUserIds()
	return f.m
}

func (f *MongoFilter) CheckOnlyUserIds() {
	if f.OnlyUserIds != nil {
		f.m[consts.ID] = bson.M{
			"$in": lo.Map[string, primitive.ObjectID](f.OnlyUserIds, func(s string, _ int) primitive.ObjectID {
				oid, _ := primitive.ObjectIDFromHex(s)
				return oid
			}),
		}
	}
}
