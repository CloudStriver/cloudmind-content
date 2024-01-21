package order

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/samber/lo"
	"log"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/count"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/mitchellh/mapstructure"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson/primitive"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
)

type (
	IEsMapper interface {
		Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*Order, int64, error)
		CountWithQuery(ctx context.Context, query []types.Query, fopts *FilterOptions) (int64, error)
	}

	EsMapper struct {
		es        *elasticsearch.TypedClient
		indexName string
	}
)

func NewEsMapper(config *config.Config) IEsMapper {
	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Username:  config.Elasticsearch.Username,
		Password:  config.Elasticsearch.Password,
		Addresses: config.Elasticsearch.Addresses,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return &EsMapper{
		es:        esClient,
		indexName: fmt.Sprintf("%s.%s", config.Mongo.DB, CollectionName),
	}
}

func (m *EsMapper) CountWithQuery(ctx context.Context, query []types.Query, fopts *FilterOptions) (int64, error) {
	ctx, span := trace.TracerFromContext(ctx).Start(ctx, "elasticsearch/Count", oteltrace.WithTimestamp(time.Now()), oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer func() {
		span.End(oteltrace.WithTimestamp(time.Now()))
	}()
	filter := newOrderFilter(fopts)
	res, err := m.es.Count().Index(m.indexName).Request(&count.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must:   query,
				Filter: filter,
			},
		},
	}).Do(ctx)
	if err != nil {
		return 0, err
	}

	return res.Count, nil
}

func (m *EsMapper) Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*Order, int64, error) {
	ctx, span := trace.TracerFromContext(ctx).Start(ctx, "elasticsearch/Search", oteltrace.WithTimestamp(time.Now()), oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer func() {
		span.End(oteltrace.WithTimestamp(time.Now()))
	}()
	p := esp.NewEsPaginator(pagination.NewRawStore(sorter), popts)
	filter := newOrderFilter(fopts)
	s, sa, err := p.MakeSortOptions(ctx)
	if err != nil {
		return nil, 0, err
	}
	res, err := m.es.Search().Index(m.indexName).Request(&search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must:   query,
				Filter: filter,
			},
		},
		SearchAfter: sa,
		Sort:        s,
		Size:        lo.ToPtr(int(*popts.Limit)),
	}).Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	hits := res.Hits.Hits
	total := res.Hits.Total.Value
	orders := make([]*Order, 0, len(hits))
	for i := range hits {
		hit := hits[i]
		order := &Order{}
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
		err = mapstructure.Decode(source, order)
		if err != nil {
			return nil, 0, err
		}

		oid := hit.Id_
		order.ID, err = primitive.ObjectIDFromHex(oid)
		if err != nil {
			return nil, 0, consts.ErrInvalidId
		}
		order.Score_ = float64(hit.Score_)
		orders = append(orders, order)
	}
	// 如果是反向查询，反转数据
	if *popts.Backward {
		for i := 0; i < len(orders)/2; i++ {
			orders[i], orders[len(orders)-i-1] = orders[len(orders)-i-1], orders[i]
		}
	}
	if len(orders) > 0 {
		err = p.StoreCursor(ctx, orders[0], orders[len(orders)-1])
		if err != nil {
			return nil, 0, err
		}
	}
	return orders, total, nil
}
