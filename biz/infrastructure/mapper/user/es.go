package user

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson/primitive"
	oteltrace "go.opentelemetry.io/otel/trace"
	"net/http"
	"time"
)

type (
	IUserEsMapper interface {
		Search(ctx context.Context, query []types.Query, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*User, int64, error)
	}

	EsMapper struct {
		es        *elasticsearch.TypedClient
		indexName string
	}
)

func (m *EsMapper) Search(ctx context.Context, query []types.Query, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*User, int64, error) {
	ctx, span := trace.TracerFromContext(ctx).Start(ctx, "elasticsearch/Search", oteltrace.WithTimestamp(time.Now()), oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer func() {
		span.End(oteltrace.WithTimestamp(time.Now()))
	}()

	p := esp.NewEsPaginator(pagination.NewRawStore(sorter), popts)
	s, sa, err := p.MakeSortOptions(ctx)
	if err != nil {
		return nil, 0, err
	}
	res, err := m.es.Search().From(int(*popts.Offset)).Size(int(*popts.Limit)).Index(m.indexName).Request(&search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: query,
			},
		},
		SearchAfter: sa,
		Sort:        s,
	}).Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	hits := res.Hits.Hits
	total := res.Hits.Total.Value
	users := make([]*User, 0, len(hits))
	for i := range hits {
		hit := hits[i]
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
			return nil, 0, consts.ErrInvalidId
		}
		user.Score_ = float64(hit.Score_)
		users = append(users, user)
	}
	// 如果是反向查询，反转数据
	if *popts.Backward {
		lo.Reverse(users)
	}
	if len(users) > 0 {
		err = p.StoreCursor(ctx, users[0], users[len(users)-1])
		if err != nil {
			return nil, 0, err
		}
	}
	return users, total, nil
}

func NewEsMapper(config *config.Config) IUserEsMapper {
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
		indexName: fmt.Sprintf("%s.%s", config.Mongo.DB, CollectionName),
	}
}
