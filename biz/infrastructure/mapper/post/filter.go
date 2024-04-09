package post

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserId  *string
	OnlyPostId  *string
	OnlyPostIds []string
	OnlyLabelId *string
	OnlyStatus  *int64
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
	f.CheckOnlyPostIds()
	f.CheckOnlyPostId()
	f.CheckOnlyLabelId()
	f.CheckOnlyStatus()
	return f.m
}

func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFilter) CheckOnlyPostIds() {
	if f.OnlyPostIds != nil {
		f.m[consts.ID] = bson.M{
			"$in": lo.Map[string, primitive.ObjectID](f.OnlyPostIds, func(s string, _ int) primitive.ObjectID {
				oid, _ := primitive.ObjectIDFromHex(s)
				return oid
			}),
		}
	}
}

func (f *MongoFilter) CheckOnlyPostId() {
	if f.OnlyPostId != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyPostId)
		f.m[consts.ID] = oid
	}
}

func (f *MongoFilter) CheckOnlyLabelId() {
	if f.OnlyLabelId != nil {
		f.m[consts.LabelIds] = bson.M{
			"$in": []string{*f.OnlyLabelId},
		}
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

func newPostFilter(options *FilterOptions) []types.Query {
	return (&postFilter{
		q:             make([]types.Query, 0),
		FilterOptions: options,
	}).toEsQuery()
}

func (f *postFilter) toEsQuery() []types.Query {
	f.CheckOnlyUserId()
	f.CheckOnlyStatus()
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
func (f *postFilter) CheckOnlyStatus() {
	if f.OnlyStatus != nil {
		f.q = append(f.q, types.Query{
			Term: map[string]types.TermQuery{
				consts.Status: {Value: *f.OnlyStatus},
			},
		})
	}
}
