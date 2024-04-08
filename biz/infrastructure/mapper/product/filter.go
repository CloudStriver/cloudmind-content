package product

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserId      *string
	OnlyProductId   *string
	OnlyProductIds  []string
	OnlyTags        []string
	OnlySetRelation *int64
	OnlyStatus      *int64
	OnlyType        *int64
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
	f.CheckOnlyProductIds()
	f.CheckOnlyProductId()
	f.CheckOnlyTags()
	f.CheckOnlyStatus()
	f.CheckOnlyTyee()
	return f.m
}

func (f *MongoFilter) CheckOnlyTyee() {
	if f.OnlyType != nil {
		f.m[consts.Type] = *f.OnlyType
	}
}
func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFilter) CheckOnlyProductIds() {
	if f.OnlyProductIds != nil {
		f.m[consts.ID] = bson.M{
			"$in": lo.Map[string, primitive.ObjectID](f.OnlyProductIds, func(s string, _ int) primitive.ObjectID {
				oid, _ := primitive.ObjectIDFromHex(s)
				return oid
			}),
		}
	}
}

func (f *MongoFilter) CheckOnlyProductId() {
	if f.OnlyProductId != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyProductId)
		f.m[consts.ID] = oid
	}
}

func (f *MongoFilter) CheckOnlyTags() {
	if f.OnlyTags != nil {

	}
}

func (f *MongoFilter) CheckOnlyStatus() {
	if f.OnlyStatus != nil {
		f.m[consts.Status] = *f.OnlyStatus
	}
}

type postFilter struct {
	q []types.Query
	*FilterOptions
}

func newProductFilter(options *FilterOptions) []types.Query {
	return (&postFilter{
		q:             make([]types.Query, 0),
		FilterOptions: options,
	}).toEsQuery()
}

func (f *postFilter) toEsQuery() []types.Query {
	f.CheckOnlyUserId()
	return f.q
}

func (f *postFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.q = append(f.q, types.Query{
			Term: map[string]types.TermQuery{
				consts.UserId: {Value: *f.OnlyUserId},
			},
		})
	}
}
