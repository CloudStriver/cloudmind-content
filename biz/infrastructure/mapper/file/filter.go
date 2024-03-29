package file

import (
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
)

type FilterOptions struct {
	OnlyUserId       *string
	OnlyFileId       *string
	OnlyFileIds      []string
	OnlyFatherId     *string
	OnlyZone         *string
	OnlySubZone      *string
	OnlyIsDel        *int64
	OnlyDocumentType *int64
	OnlyType         []string
	OnlyCategory     *int64
	OnlyLabelId      *string
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
	f.CheckOnlyFileId()
	f.CheckOnlyFileIds()
	f.CheckOnlyFatherId()
	f.CheckOnlyIsDel()
	f.CheckOnlyDocumentType()
	f.CheckOnlyType()
	f.CheckOnlyCategory()
	f.CheckOnlyLabelId()
	return f.m
}

func (f *MongoFileFilter) CheckOnlyType() {
	if f.OnlyType != nil {
		f.m[consts.Type] = bson.M{
			"$in": f.OnlyType,
		}
	}
}

func (f *MongoFileFilter) CheckOnlyCategory() {
	if f.OnlyCategory != nil {
		f.m[consts.Category] = *f.OnlyCategory
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

func (f *MongoFileFilter) CheckOnlyFileId() {
	if f.OnlyFileId != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyFileId)
		f.m[consts.ID] = oid
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

func (f *MongoFileFilter) CheckOnlyFatherId() {
	if f.OnlyFatherId != nil {
		f.m[consts.FatherId] = *f.OnlyFatherId
	}
}

func (f *MongoFileFilter) CheckOnlyIsDel() {
	if f.OnlyIsDel != nil {
		f.m[consts.IsDel] = *f.OnlyIsDel
	}
}

func (f *MongoFileFilter) CheckOnlyDocumentType() {
	if f.OnlyDocumentType != nil && *f.OnlyDocumentType == int64(gencontent.Space_Space_public) {
		if f.OnlyZone != nil {
			f.m[consts.Zone] = *f.OnlyZone
		} else {
			f.m[consts.Zone] = bson.M{"$exists": true, "$ne": ""}
		}
		if f.OnlySubZone != nil {
			f.m[consts.SubZone] = *f.OnlySubZone
		} else {
			f.m[consts.SubZone] = bson.M{"$exists": true, "$ne": ""}
		}
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
	f.checkOnlyIsDel()
	f.checkOnlyDocumentType()
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

func (f *EsFilter) checkOnlyIsDel() {
	if f.OnlyIsDel != nil {
		f.q = append(f.q, types.Query{
			Term: map[string]types.TermQuery{
				consts.IsDel: {Value: *f.OnlyIsDel},
			},
		})
	}
}

func (f *EsFilter) checkOnlyDocumentType() {
	if f.OnlyDocumentType != nil && *f.OnlyDocumentType == int64(gencontent.Space_Space_public) {
		if f.OnlyZone != nil {
			f.q = append(f.q, types.Query{
				Term: map[string]types.TermQuery{
					consts.Zone: {Value: *f.OnlyZone},
				},
			})
		} else {
			f.q = append(f.q, types.Query{
				Bool: &types.BoolQuery{
					MustNot: []types.Query{
						{
							Term: map[string]types.TermQuery{
								consts.Zone: {Value: ""},
							},
						},
					},
				},
			})
		}
		if f.OnlySubZone != nil {
			f.q = append(f.q, types.Query{
				Term: map[string]types.TermQuery{
					consts.SubZone: {Value: *f.OnlySubZone},
				},
			})
		} else {
			f.q = append(f.q, types.Query{
				Bool: &types.BoolQuery{
					MustNot: []types.Query{
						{
							Term: map[string]types.TermQuery{
								consts.SubZone: {Value: ""},
							},
						},
					},
				},
			})
		}
	}
}
