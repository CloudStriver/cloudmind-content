package post

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserId      *string
	OnlyPostId      *string
	OnlyPostIds     []string
	OnlyTitle       *string
	OnlyText        *string
	OnlyTags        []string
	OnlySetRelation *int64
	OnlyStatus      *int64
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
	f.CheckOnlyTitle()
	f.CheckOnlyText()
	f.CheckOnlyTags()
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

func (f *MongoFilter) CheckOnlyTags() {
	if f.OnlyTags != nil {
		if f.OnlySetRelation != nil {
			if *f.OnlySetRelation == int64(gencontent.SetRelation_Set_intersection) {
				f.m[consts.Tags] = bson.M{"$all": f.OnlyTags}
			} else if *f.OnlySetRelation == int64(gencontent.SetRelation_Set_unionSet) {
				f.m[consts.Tags] = bson.M{"$in": f.OnlyTags}
			}
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
