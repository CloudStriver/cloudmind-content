package file

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"github.com/mitchellh/mapstructure"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson/primitive"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
)

type (
	IEsMapper interface {
		Search(ctx context.Context, communityId, keyword string, skip, count int) ([]*File, int64, error)
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
		indexName: fmt.Sprintf("%s.%s-alias", config.Mongo.DB, CollectionName),
	}
}

func (m *EsMapper) Search(ctx context.Context, communityId, keyword string, skip, count int) ([]*File, int64, error) {
	ctx, span := trace.TracerFromContext(ctx).Start(ctx, "elasticsearch/Search", oteltrace.WithTimestamp(time.Now()), oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer func() {
		span.End(oteltrace.WithTimestamp(time.Now()))
	}()
	res, err := m.es.Search().From(skip).Size(count).Index(m.indexName).Request(&search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must: []types.Query{
					{
						MultiMatch: &types.MultiMatchQuery{
							Query: keyword,
							//Fields: []string{consts.Details, consts.Name + "^5", consts.Area, consts.Color},
						},
					},
					{
						Term: map[string]types.TermQuery{
							//consts.CommunityId: {
							//	Value: communityId,
							//},
						},
					},
				},
			},
		},
		Sort: types.Sort{
			&types.SortOptions{
				SortOptions: map[string]types.FieldSort{
					//consts.Score: {
					//	Order: &sortorder.Desc,
					//},
					consts.CreateAt: {
						Order: &sortorder.Desc,
					},
				},
			},
		},
	}).Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	total := res.Hits.Total.Value
	cats := make([]*File, 0, 10)
	for _, hit := range res.Hits.Hits {
		cat := &File{}
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
		err = mapstructure.Decode(source, cat)
		if err != nil {
			return nil, 0, err
		}

		oid := hit.Id_
		cat.ID, err = primitive.ObjectIDFromHex(oid)
		if err != nil {
			return nil, 0, err
		}
		cats = append(cats, cat)
	}
	return cats, total, nil
}
