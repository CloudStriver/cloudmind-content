package file

import (
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
)

type FilterOptions struct {
	OnlyUserId   *string
	OnlyFileId   *string
	OnlyFatherId *string
	OnlyFileType *int64
	IsDel        int64
	DocumentType int64
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
	f.CheckOnlyFatherId()
	f.CheckOnlyFileType()
	f.CheckDocumentType()
	return f.m
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

func (f *MongoFileFilter) CheckOnlyFatherId() {
	if f.OnlyFatherId != nil {
		f.m[consts.FatherId] = *f.OnlyFatherId
	}
}

func (f *MongoFileFilter) CheckOnlyFileType() {
	if f.OnlyFileType != nil {
		f.m[consts.Type] = *f.OnlyFileType
	}
}

func (f *MongoFileFilter) CheckIsDel() {
	f.m[consts.IsDel] = f.IsDel
}

func (f *MongoFileFilter) CheckDocumentType() {
	if f.DocumentType == 2 {
		f.m[consts.Tag] = bson.M{"$ne": nil}
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
	f.checkOnlyFileId()
	//f.checkOnlyCommunityId()
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

func (f *EsFilter) checkOnlyFileId() {
	if f.OnlyFileId != nil {
		f.q = append(f.q, types.Query{
			Term: map[string]types.TermQuery{
				consts.ID: {Value: *f.OnlyFileId},
			},
		})
	}
}

// 对应查看某个群组的文件列表
//func (f *EsFilter) checkOnlyCommunityId() {
//	if f.IncludeGlobal == nil {
//		if f.OnlyCommunityId != nil {
//			f.q = append(f.q, types.Query{
//				Term: map[string]types.TermQuery{
//					consts.CommunityId: {Value: *f.OnlyCommunityId},
//				},
//			})
//		}
//	} else if *f.IncludeGlobal == false {
//		if f.OnlyCommunityId != nil {
//			f.q = append(f.q, types.Query{
//				Term: map[string]types.TermQuery{
//					consts.CommunityId: {Value: *f.OnlyCommunityId},
//				},
//			})
//		}
//	} else {
//		if f.OnlyCommunityId != nil {
//			BoolQuery := make([]types.Query, 0)
//			BoolQuery = append(BoolQuery, types.Query{
//				Bool: &types.BoolQuery{
//					MustNot: []types.Query{
//						types.Query{
//							Exists: &types.ExistsQuery{
//								Field: consts.CommunityId,
//							},
//						},
//					},
//				},
//			})
//			BoolQuery = append(BoolQuery, types.Query{
//				Term: map[string]types.TermQuery{
//					consts.CommunityId: {Value: *f.OnlyCommunityId},
//				},
//			})
//			f.q = append(f.q, types.Query{
//				Bool: &types.BoolQuery{
//					Should: BoolQuery,
//				},
//			})
//		} else {
//			BoolQuery := make([]types.Query, 0)
//			BoolQuery = append(BoolQuery, types.Query{
//				Bool: &types.BoolQuery{
//					MustNot: []types.Query{
//						types.Query{
//							Exists: &types.ExistsQuery{
//								Field: consts.CommunityId,
//							},
//						},
//					},
//				},
//			})
//			f.q = append(f.q, types.Query{
//				Bool: &types.BoolQuery{
//					Should: BoolQuery,
//				},
//			})
//		}
//	}
//}
