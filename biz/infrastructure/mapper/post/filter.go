package post

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.mongodb.org/mongo-driver/bson"
)

type FilterOptions struct {
	OnlyUserId *string
	OnlyPostId *string
	OnlyTitle  *string
	OnlyText   *string
	OnlyTag    *string
	OnlyStatus *int64
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
	return f.m
}

func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFilter) CheckOnlyPostId() {
	if f.OnlyPostId != nil {
		f.m[consts.ID] = *f.OnlyPostId
	}
}

func (f *MongoFilter) CheckOnlyTitle() {
	if f.OnlyTitle != nil {
		f.m[consts.Title] = *f.OnlyTitle
	}
}

func (f *MongoFilter) CheckOnlyText() {
	if f.OnlyText != nil {
		f.m[consts.Text] = *f.OnlyText
	}
}

func (f *MongoFilter) CheckOnlyTag() {
	if f.OnlyTag != nil {
		f.m[consts.Tag] = *f.OnlyTag
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
