package user

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/logx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"net/http"
	"time"
)

type (
	UserEsMapper interface {
		Search(ctx context.Context, keyword string, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*User, int64, error)
	}

	EsMapper struct {
		es        *elasticsearch.TypedClient
		IndexName string
	}
)

func (e *EsMapper) Search(ctx context.Context, keyword string, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*User, int64, error) {
	p := esp.NewEsPaginator(pagination.NewRawStore(sorter), popts)
	s, sa, err := p.MakeSortOptions(ctx)
	if err != nil {
		log.CtxError(ctx, "创建索引异常[%v]\n", err)
		return nil, 0, err
	}
	res, err := e.es.Search().Index(e.IndexName).Request(&search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						MultiMatch: &types.MultiMatchQuery{
							Fields: []string{consts.Name, consts.ID},
							Query:  keyword,
						},
					},
				},
			},
		},
		Sort:        s,
		SearchAfter: sa,
		Size:        lo.ToPtr(int(*popts.Limit)),
	}).Do(ctx)
	if err != nil {
		logx.Errorf("es查询异常[%v]\n", err)
		return nil, 0, err
	}

	total := res.Hits.Total.Value
	users := make([]*User, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		user := &User{}
		source := make(map[string]any)
		err = sonic.Unmarshal(hit.Source_, &source)
		if err != nil {
			return nil, 0, err
		}
		if source[consts.CreateAt], err = time.Parse("2006-01-02T15:04:05Z07:00", source[consts.CreateAt].(string)); err != nil {
			return nil, 0, err
		}
		if source[consts.UpdateAt], err = time.Parse("2006-01-02T15:04:05Z07:00", source[consts.UpdateAt].(string)); err != nil {
			return nil, 0, err
		}
		err = mapstructure.Decode(source, user)
		if err != nil {
			return nil, 0, err
		}

		oid := hit.Id_
		user.ID, err = primitive.ObjectIDFromHex(oid)
		if err != nil {
			return nil, 0, err
		}
		user.Score_ = float64(hit.Score_)
		users = append(users, user)
	}

	if *popts.Backward {
		users = lo.Reverse(users)
	}

	// 更新游标
	if len(users) > 0 {
		err = p.StoreCursor(ctx, users[0], users[len(users)-1])
		if err != nil {
			return nil, 0, err
		}
	}
	return users, int64(total), nil
}

func NewEsMapper(config *config.Config) UserEsMapper {
	es, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: config.Elasticsearch.Addresses,
		Username:  config.Elasticsearch.Username,
		Password:  config.Elasticsearch.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		logx.Errorf("elasticsearch连接异常[%v]\n", err)
	}
	return &EsMapper{
		es:        es,
		IndexName: fmt.Sprintf("cloudmind_content.user"),
	}
}
