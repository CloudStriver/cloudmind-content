package hot

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserId *string
	OnlyHotId  *string
	OnlyHotIds []string
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
	f.CheckOnlyUserId()
	f.CheckOnlyHotIds()
	f.CheckOnlyHotId()
	return f.m
}

func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFilter) CheckOnlyHotIds() {
	if f.OnlyHotIds != nil {
		f.m[consts.ID] = bson.M{
			"$in": lo.Map[string, primitive.ObjectID](f.OnlyHotIds, func(s string, _ int) primitive.ObjectID {
				oid, _ := primitive.ObjectIDFromHex(s)
				return oid
			}),
		}
	}
}

func (f *MongoFilter) CheckOnlyHotId() {
	if f.OnlyHotId != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyHotId)
		f.m[consts.ID] = oid
	}
}

//
//type postFilter struct {
//	q []types.Query
//	*FilterOptions
//}

//func newHotFilter(options *FilterOptions) []types.Query {
//	return (&postFilter{
//		q:             make([]types.Query, 0),
//		FilterOptions: options,
//	}).toEsQuery()
//}
//
//func (f *postFilter) toEsQuery() []types.Query {
//	f.CheckOnlyUserId()
//	return f.q
//}
//
//func (f *postFilter) CheckOnlyUserId() {
//	if f.OnlyUserId != nil {
//		f.q = append(f.q, types.Query{
//			Term: map[string]types.TermQuery{
//				consts.UserId: {Value: *f.OnlyUserId},
//			},
//		})
//	}
//}
