package publicfile

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type FilterOptions struct {
	OnlyUserId      *string
	OnlyFileIds     []string
	OnlyZone        *string
	OnlyType        []string
	OnlyAuditStatus *int64
	OnlyLabelId     *string
}

type MongoFileFilter struct {
	m bson.M
	*FilterOptions
}

func makeMongoFilter(options *FilterOptions) bson.M {
	return (&MongoFileFilter{
		m:             bson.M{},
		FilterOptions: options,
	}).toBson()
}

func (f *MongoFileFilter) toBson() bson.M {
	f.CheckOnlyUserId()
	f.CheckOnlyFileIds()
	f.CheckOnlyDocumentType()
	f.CheckOnlyType()
	f.CheckOnlyAuditStatus()
	f.CheckOnlyLabelId()
	return f.m
}

func (f *MongoFileFilter) CheckOnlyAuditStatus() {
	if f.OnlyAuditStatus != nil {
		f.m[consts.AuditStatus] = *f.OnlyAuditStatus
	}
}

func (f *MongoFileFilter) CheckOnlyType() {
	if f.OnlyType != nil {
		f.m[consts.Type] = bson.M{
			"$in": f.OnlyType,
		}
	}
}

func (f *MongoFileFilter) CheckOnlyLabelId() {
	if f.OnlyLabelId != nil {
		f.m[consts.Labels] = bson.M{"$in": *f.OnlyLabelId}
	}
}

func (f *MongoFileFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFileFilter) CheckOnlyFileIds() {
	if f.OnlyFileIds != nil {
		f.m[consts.ID] = bson.M{
			"$in": lo.Map[string, primitive.ObjectID](f.OnlyFileIds, func(s string, _ int) primitive.ObjectID {
				oid, _ := primitive.ObjectIDFromHex(s)
				return oid
			}),
		}
	}
}

func (f *MongoFileFilter) CheckOnlyDocumentType() {
	if f.OnlyZone != nil {
		f.m[consts.Zone] = *f.OnlyZone
	}
}

type EsFilter struct {
	q []types.Query
	*FilterOptions
}

func makeEsFilter(opts *FilterOptions) []types.Query {
	return (&EsFilter{
		q:             make([]types.Query, 0),
		FilterOptions: opts,
	}).toEsQuery()
}

func (f *EsFilter) toEsQuery() []types.Query {
	f.checkOnlyUserId()
	return f.q
}

func (f *EsFilter) checkOnlyUserId() {
	if f.OnlyUserId != nil {
		f.q = append(f.q, types.Query{
			Term: map[string]types.TermQuery{
				consts.UserId: {Value: *f.OnlyUserId},
			},
		})
	}
}
