package file

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
)

type FilterOptions struct {
	OnlyUserId   *string
	OnlyFileId   *string
	OnlyFatherId *string
	OnlyFileType *int32
	IsDel        int32
	DocumentType int32
}

type MongoFilter struct {
	m bson.M
	*FilterOptions
}

func makeMongoFilter(options *FilterOptions) bson.M {
	return (&MongoFilter{
		m:             bson.M{},
		FilterOptions: options,
	}).toBson()
}

func (f *MongoFilter) toBson() bson.M {
	f.CheckOnlyUserId()
	f.CheckOnlyFileId()
	f.CheckOnlyFatherId()
	f.CheckOnlyFileType()
	f.CheckDocumentType()
	return f.m
}

func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

func (f *MongoFilter) CheckOnlyFileId() {
	if f.OnlyFileId != nil {
		oid, _ := primitive.ObjectIDFromHex(*f.OnlyFileId)
		f.m[consts.ID] = oid
	}
}

func (f *MongoFilter) CheckOnlyFatherId() {
	if f.OnlyFatherId != nil {
		f.m[consts.FatherId] = *f.OnlyFatherId
	}
}

func (f *MongoFilter) CheckOnlyFileType() {
	if f.OnlyFileType != nil {
		f.m[consts.Type] = *f.OnlyFileType
	}
}

func (f *MongoFilter) CheckIsDel() {
	f.m[consts.IsDel] = f.IsDel
}

func (f *MongoFilter) CheckDocumentType() {
	if f.DocumentType == 2 {
		f.m[consts.Tag] = bson.M{"$ne": bson.A{}}
	}
}

//type EsFilter struct {
//	q []types.Query
//	*FilterOptions
//}
//
//func makeEsFilter(opts *FilterOptions) []types.Query {
//	return (&EsFilter{
//		q:             make([]types.Query, 0),
//		FilterOptions: opts,
//	}).toQuery()
//}
//
//func (f *EsFilter) toQuery() []types.Query {
//	f.checkOnlyUserId()
//	f.checkOnlyCatId()
//	f.checkOnlyCommunityId()
//	return f.q
//}
//
//func (f *EsFilter) checkOnlyUserId() {
//	if f.OnlyUserId != nil {
//		f.q = append(f.q, types.Query{
//			Term: map[string]types.TermQuery{
//				consts.InitiatorId: {Value: *f.OnlyUserId},
//			},
//		})
//	}
//}
//
//func (f *EsFilter) checkOnlyCatId() {
//	if f.OnlyCatId != nil {
//		f.q = append(f.q, types.Query{
//			Term: map[string]types.TermQuery{
//				consts.CatId: {Value: *f.OnlyCatId},
//			},
//		})
//	}
//}
//
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
