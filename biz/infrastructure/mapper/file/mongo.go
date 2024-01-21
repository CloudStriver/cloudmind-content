package file

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	"github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"sync"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "file"

var prefixFileCacheKey = "cache:file:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		Count(ctx context.Context, filter *FilterOptions) (int64, error)
		Insert(ctx context.Context, data *File) (string, error)
		FindOne(ctx context.Context, fopts *FilterOptions) (*File, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error)
		FindManyNotPagination(ctx context.Context, fopts *FilterOptions) ([]*File, error)
		FindFolderSize(ctx context.Context, path string) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, int64, error)
		Upsert(ctx context.Context, data *File) (*mongo.UpdateResult, error)
		Update(ctx context.Context, data *File) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		GetConn() *monc.Model
		StartClient() *mongo.Client
	}

	File struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `bson:"userId,omitempty" json:"userId,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Type        int64              `bson:"type,omitempty" json:"type,omitempty"`
		Path        string             `bson:"path,omitempty" json:"path,omitempty"`
		FatherId    string             `bson:"fatherId,omitempty" json:"fatherId,omitempty"`
		Size        *int64             `bson:"size,omitempty" json:"size,omitempty"`
		Md5         string             `bson:"md5,omitempty" json:"md5,omitempty"`
		IsDel       int64              `bson:"isDel,omitempty" json:"isDel,omitempty"`
		Tags        []string           `bson:"tags,omitempty" json:"tags,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		DeletedAt   time.Time          `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
		Score_      float64            `bson:"score_,omitempty" json:"score_,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}

	AggregateSizeResult struct {
		Size int64 `bson:"size,omitempty"`
	}
)

func (m *MongoMapper) FindManyNotPagination(ctx context.Context, fopts *FilterOptions) ([]*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyNotPagination", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	filter := makeMongoFilter(fopts)
	var data []*File
	if err := m.conn.Find(ctx, &data, filter, &options.FindOptions{}); err != nil {
		if errorx.Is(err, monc.ErrNotFound) {
			return nil, consts.ErrNotFound
		}
		return nil, err
	}
	return data, nil
}

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	indexModel := mongo.IndexModel{
		Keys: bson.M{
			"deletedAt": 1, // 索引字段
		},
		Options: options.Index().SetExpireAfterSeconds(604800), // 一周后过期
	}
	_, err := conn.Indexes().CreateOne(context.Background(), indexModel)
	if err != nil {
		log.Error("fileModel TTL index created 失败[%v]\n", err)
	} else {
		log.Info("fileModel TTL index created successfully")
	}

	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Insert(ctx context.Context, data *File) (string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Insert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixFileCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		return "", err
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), err
}

func (m *MongoMapper) FindOne(ctx context.Context, fopts *FilterOptions) (*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindOne", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data File
	if fopts.OnlyFileId != nil {
		_, err := primitive.ObjectIDFromHex(*fopts.OnlyFileId)
		if err != nil {
			return nil, consts.ErrInvalidId
		}
	}

	filter := makeMongoFilter(fopts)
	key := prefixFileCacheKey + *fopts.OnlyFileId
	if err := m.conn.FindOne(ctx, key, &data, filter); err != nil {
		if errorx.Is(err, monc.ErrNotFound) {
			return nil, consts.ErrNotFound
		} else {
			return nil, err
		}
	}
	return &data, nil
}

func (m *MongoMapper) Count(ctx context.Context, fopts *FilterOptions) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Count", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	filter := makeMongoFilter(fopts)
	return m.conn.CountDocuments(ctx, filter)
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := makeMongoFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*File
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

	// 如果是反向查询，反转数据
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

func (m *MongoMapper) FindFolderSize(ctx context.Context, path string) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindFolderSize", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var size AggregateSizeResult
	pipeline := mongo.Pipeline{
		{
			{"$match", bson.M{
				"path":  bson.M{"$regex": "^" + path},
				"type":  bson.M{"$ne": gencontent.Type_Type_folder},
				"isDel": gencontent.IsDel_Is_no,
			}},
		},
		{
			{"$group", bson.M{
				"_id":  nil,
				"size": bson.M{"$sum": "$size"},
			}},
		},
	}

	result, err := m.conn.Database().Collection("cloudmind_contentcenter").Aggregate(ctx, pipeline)
	switch {
	case err == nil:
		if result.Next(ctx) {
			err = result.Decode(&size)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, nil
		}

		if err = result.Err(); err != nil {
			return 0, err
		}
		return size.Size, nil
	case errorx.Is(err, monc.ErrNotFound):
		return 0, consts.ErrNotFound
	default:
		return 0, err
	}
}

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyAndCount", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*File
	var total int64
	wait := sync.WaitGroup{}
	wait.Add(2)
	c := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer wait.Done()
		var err error
		data, err = m.FindMany(ctx, fopts, popts, sorter)
		if err != nil {
			c <- err
			return
		}
	}()
	go func() {
		defer wait.Done()
		var err error
		total, err = m.Count(ctx, fopts)
		if err != nil {
			c <- err
			return
		}
	}()
	go func() {
		wait.Wait()
		defer close(c)
	}()
	if err := <-c; err != nil {
		return nil, 0, err
	}
	return data, total, nil
}

func (m *MongoMapper) Upsert(ctx context.Context, data *File) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Upsert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	key := prefixFileCacheKey + data.ID.Hex()
	update := bson.M{
		"$set": bson.M{"$set": data},
		"$setOnInsert": bson.M{
			consts.ID:       data.ID,
			consts.CreateAt: time.Now(),
			consts.UpdateAt: time.Now(),
		},
	}

	option := options.UpdateOptions{}
	option.SetUpsert(true)

	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, update, &option)
	return res, err
}

func (m *MongoMapper) Update(ctx context.Context, data *File) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Update", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	data.UpdateAt = time.Now()
	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID, consts.UserId: data.UserId}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Delete", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return 0, consts.ErrInvalidId
	}
	key := prefixFileCacheKey + id
	resp, err := m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return resp, err
}

func (m *MongoMapper) GetConn() *monc.Model {
	return m.conn
}

func (m *MongoMapper) StartClient() *mongo.Client {
	return m.conn.Database().Client()
}
