package file

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
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"net/http"
	"time"
)

type (
	IFileEsMapper interface {
		Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*File, int64, error)
	}

	EsMapper struct {
		es        *elasticsearch.TypedClient
		IndexName string
	}
)

func (e *EsMapper) Search(ctx context.Context, query []types.Query, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter esp.EsCursor) ([]*File, int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "elasticsearch.Search", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	p := esp.NewEsPaginator(pagination.NewRawStore(sorter), popts)
	filter := makeEsFilter(fopts)
	s, sa, err := p.MakeSortOptions(ctx)
	if err != nil {
		log.CtxError(ctx, "创建索引异常[%v]\n", err)
		return nil, 0, err
	}
	res, err := e.es.Search().Index(e.IndexName).Request(&search.Request{
		Query: &types.Query{
			Bool: &types.BoolQuery{
				Must:   query,
				Filter: filter,
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
	files := make([]*File, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		file := &File{}
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

	if *popts.Backward {
		files = lo.Reverse(files)
	}

	// 更新游标
	if len(files) > 0 {
		err = p.StoreCursor(ctx, files[0], files[len(files)-1])
		if err != nil {
			return nil, 0, err
		}
	}
	return files, total, nil
}

func NewEsMapper(config *config.Config) IFileEsMapper {
	esClient, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Username:  config.Elasticsearch.Username,
		Password:  config.Elasticsearch.Password,
		Addresses: config.Elasticsearch.Addresses,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	})
	if err != nil {
		logx.Errorf("elasticsearch连接异常[%v]\n", err)
	}
	return &EsMapper{
		es:        esClient,
		IndexName: fmt.Sprintf("%s.%s", config.Mongo.DB, CollectionName),
	}
}
