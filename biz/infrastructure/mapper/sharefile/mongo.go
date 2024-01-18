package sharefile

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"time"
)

const CollectionName = "shareFile"

var prefixPublicFileCacheKey = "cache:shareFile:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		Count(ctx context.Context, filter *ShareCodeOptions) (int64, error)
		Insert(ctx context.Context, data *ShareFile) (string, error)
		FindOne(ctx context.Context, id string) (*ShareFile, error)
		FindMany(ctx context.Context, fopts *ShareCodeOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*ShareFile, error)
		FindManyAndCount(ctx context.Context, fopts *ShareCodeOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*ShareFile, int64, error)
		Update(ctx context.Context, data *ShareFile) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, fopts *ShareCodeOptions) (int64, error)
		GetConn() *monc.Model
	}

	ShareFile struct {
		ID            primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId        string             `bson:"userId,omitempty" json:"userId,omitempty"`
		Name          string             `bson:"name,omitempty" json:"name,omitempty"`
		FileList      []string           `bson:"fileList,omitempty" json:"fileList,omitempty"`
		EffectiveTime int64              `bson:"effectiveTime,omitempty" json:"effectiveTime,omitempty"` // 有效期
		BrowseNumber  *int64             `bson:"browseNumber,omitempty" json:"browseNumber,omitempty"`   // 浏览次数
		CreateAt      time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`           // 创建时间
		DeletedAt     time.Time          `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	indexModel := mongo.IndexModel{
		Keys: bson.M{
			"deletedAt": 1, // 索引字段
		},
		Options: options.Index().SetExpireAfterSeconds(0), // 一周后过期
	}
	_, err := conn.Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		log.Error("shareFileModel TTL index created 失败[%v]\n", err)
	} else {
		log.Info("shareFileModel TTL index created successfully")
	}

	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Count(ctx context.Context, fopts *ShareCodeOptions) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Count", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	filter := makeMongoShareCodeFilter(fopts)
	return m.conn.CountDocuments(ctx, filter)
}

func (m *MongoMapper) Insert(ctx context.Context, data *ShareFile) (string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Insert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
	}

	key := prefixPublicFileCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		return "", err
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*ShareFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindOne", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidId
	}
	var data ShareFile
	key := prefixPublicFileCacheKey + id
	err = m.conn.FindOne(ctx, key, &data, bson.M{"_id": oid})
	switch {
	case err == nil:
		return &data, nil
	case errorx.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *ShareCodeOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*ShareFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := makeMongoShareCodeFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*ShareFile
	if err = m.conn.Find(ctx, &data, filter, &options.FindOptions{
		Sort:  sort,
		Limit: popts.Limit,
		Skip:  popts.Offset,
	}); err != nil {
		if errorx.Is(err, monc.ErrNotFound) {
			return nil, consts.ErrNotFound
		}
		return nil, err
	}

	if *popts.Backward {
		for i := 0; i < len(data)/2; i++ {
			data[i], data[len(data)-i-1] = data[len(data)-i-1], data[i]
		}
	}
	if len(data) > 0 {
		if err = p.StoreCursor(ctx, data[0], data[len(data)-1]); err != nil {
			return nil, err
		}
	}

	return data, nil
}

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *ShareCodeOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*ShareFile, int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyAndCount", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*ShareFile
	var total int64
	var err, err1, err2 error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err = mr.Finish(func() error {
		data, err1 = m.FindMany(ctx, fopts, popts, sorter)
		if err1 != nil {
			return err1
		}
		return nil
	}, func() error {
		total, err2 = m.Count(ctx, fopts)
		if err2 != nil {
			return err2
		}
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	return data, total, nil
}

func (m *MongoMapper) Update(ctx context.Context, data *ShareFile) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Update", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	key := prefixPublicFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{"_id": data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, fopts *ShareCodeOptions) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Delete", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	if fopts.OnlyCode != nil {
		_, err := primitive.ObjectIDFromHex(*fopts.OnlyCode)
		if err != nil {
			return 0, consts.ErrInvalidId
		}
	}

	filter := makeMongoShareCodeFilter(fopts)
	key := prefixPublicFileCacheKey + *fopts.OnlyCode
	res, err := m.conn.DeleteOne(ctx, key, filter)
	return res, err
}

func (m *MongoMapper) GetConn() *monc.Model {
	return m.conn
}
