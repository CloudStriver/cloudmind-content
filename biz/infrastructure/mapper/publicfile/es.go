package publicfile

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/utils"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/bytedance/sonic"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/count"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson/primitive"
	oteltrace "go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"time"
)

type (
	IEsMapper interface {
		Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sopts *gencontent.SearchOption) ([]*PublicFile, int64, error)
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
	filter := makeEsFilter(fopts)
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

func SortTypeToCursorType(sortType gencontent.SearchSortType) esp.EsCursor {
	switch sortType {
	case gencontent.SearchSortType_ScoreSearchSortType:
		return esp.ScoreCursorType
	case gencontent.SearchSortType_CreateTimeSearchSortType:
		return esp.IdCursorType
	case gencontent.SearchSortType_SynthesisSearchSortType:
		return esp.ScoreCursorType
	default:
		return nil
	}
}

func (m *EsMapper) Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sopts *gencontent.SearchOption) ([]*PublicFile, int64, error) {
	ctx, span := trace.TracerFromContext(ctx).Start(ctx, "elasticsearch/Search", oteltrace.WithTimestamp(time.Now()), oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer func() {
		span.End(oteltrace.WithTimestamp(time.Now()))
	}()
	p := esp.NewEsPaginator(pagination.NewRawStore(SortTypeToCursorType(sopts.SearchSortType)), popts)
	filter := makeEsFilter(fopts)
	s, sa, err := p.MakeSortOptions(ctx)
	if err != nil {
		return nil, 0, err
	}
	req := utils.GetQuery(query, filter, s, sa, sopts)
	res, err := m.es.Search().From(int(*popts.Offset)).Size(int(*popts.Limit)).Index(m.indexName).Request(req).Do(ctx)
	if err != nil {
		return nil, 0, err
	}

	hits := res.Hits.Hits
	total := res.Hits.Total.Value
	files := make([]*PublicFile, 0, len(hits))
	for i := range hits {
		hit := hits[i]
		file := &PublicFile{}
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
		err = mapstructure.Decode(source, file)
		if err != nil {
			return nil, 0, err
		}

		oid := hit.Id_
		file.ID, err = primitive.ObjectIDFromHex(oid)
		if err != nil {
			return nil, 0, consts.ErrInvalidId
		}
		file.Score_ = float64(hit.Score_)
		files = append(files, file)
	}
	if sopts.SearchSortType != gencontent.SearchSortType_SynthesisSearchSortType {
		// 如果是反向查询，反转数据
		if *popts.Backward {
			lo.Reverse(files)
		}
		if len(files) > 0 {
			err = p.StoreCursor(ctx, files[0], files[len(files)-1])
			if err != nil {
				return nil, 0, err
			}
		}
	}
	return files, total, nil
}
