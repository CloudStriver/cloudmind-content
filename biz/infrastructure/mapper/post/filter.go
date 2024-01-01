package post

import (
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/xh-polaris/meowchat-content/biz/infrastructure/consts"
)

type FilterOptions struct {
	OnlyUserId   *string
	OnlyOfficial *bool
}

type BaseFilter struct {
	MustFlags    *Flag
	MustNotFlags *Flag
	*FilterOptions
}

func (f *BaseFilter) CheckOnlyOfficial() {
	if f.OnlyOfficial != nil {
		f.MustFlags = f.MustFlags.SetFlag(OfficialFlag, *f.OnlyOfficial)
	}
}

type MongoFilter struct {
	m bson.M
	*BaseFilter
}

func MakeBsonFilter(options *FilterOptions) bson.M {
	return (&MongoFilter{
		m: bson.M{},
		BaseFilter: &BaseFilter{
			FilterOptions: options,
		},
	}).toBson()
}

func (f *MongoFilter) toBson() bson.M {
	f.CheckOnlyUserId()
	f.CheckOnlyOfficial()
	f.CheckFlags()
	return f.m
}

func (f *MongoFilter) CheckFlags() {
	if f.MustFlags != nil && *f.MustFlags != 0 {
		f.m[consts.Flags] = bson.M{"$bitsAllSet": *f.MustFlags}
	}
	if f.MustNotFlags != nil && *f.MustNotFlags != 0 {
		or, exist := f.m["$or"]
		if !exist {
			or = bson.A{}
		}

		_ = append(or.(bson.A), bson.M{
			consts.Flags: bson.M{
				"$bitsAllClear": *f.MustNotFlags},
		}, bson.M{
			consts.Flags: bson.M{
				"$exists": false,
			},
		})
		f.m["$or"] = or
	}
}

func (f *MongoFilter) CheckOnlyUserId() {
	if f.OnlyUserId != nil {
		f.m[consts.UserId] = *f.OnlyUserId
	}
}

type postFilter struct {
	q []types.Query
	*BaseFilter
}

func newPostFilter(options *FilterOptions) []types.Query {
	return (&postFilter{
		q: make([]types.Query, 0),
		BaseFilter: &BaseFilter{
			FilterOptions: options,
		},
	}).toEsQuery()
}

func (f *postFilter) toEsQuery() []types.Query {
	f.CheckOnlyUserId()
	f.CheckOnlyOfficial()
	f.CheckFlags()
	return f.q
}

func (f *postFilter) CheckFlags() {
	if f.MustFlags != nil && *f.MustFlags != 0 {
		raw, _ := json.Marshal(*f.MustFlags)
		f.q = append(f.q, types.Query{
			//TODO 也许会造成潜在的性能风险
			Script: &types.ScriptQuery{
				Script: types.InlineScript{
					Source: fmt.Sprintf("doc['%s'].size() != 0 && "+
						"(doc['%s'].value & params.%s) == params.%s", consts.Flags, consts.Flags, consts.Flags, consts.Flags),
					Params: map[string]json.RawMessage{
						consts.Flags: raw,
					},
				},
			},
		})
	}
	if f.MustNotFlags != nil && *f.MustNotFlags != 0 {
		raw, _ := json.Marshal(*f.MustNotFlags)
		f.q = append(f.q, types.Query{
			//TODO 也许会造成潜在的性能风险
			Script: &types.ScriptQuery{
				Script: types.InlineScript{
					Source: fmt.Sprintf("doc['%s'].size() == 0 || "+
						"(doc['%s'].value & params.%s) == 0", consts.Flags, consts.Flags, consts.Flags),
					Params: map[string]json.RawMessage{
						consts.Flags: raw,
					},
				},
			},
		})
	}
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
