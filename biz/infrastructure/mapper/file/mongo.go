package file

import (
	"context"
	errorx "errors"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"github.com/zeromicro/go-zero/core/trace"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"strconv"
	"time"
)

const CollectionName = "file"

var prefixFileCacheKey = "cache:file:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		FindFileIsExist(ctx context.Context, md5 string) (bool, error)
		Count(ctx context.Context, filter *FilterOptions) (int64, error)
		Insert(ctx context.Context, data *File) (string, error)
		FindOne(ctx context.Context, fopts *FilterOptions) (*File, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error)
		FindManyNotPagination(ctx context.Context, fopts *FilterOptions) ([]*File, error)
		FindFolderSize(ctx context.Context, path string) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, int64, error)
		Update(ctx context.Context, data *File) (*mongo.UpdateResult, error)
		UpdateMany(ctx context.Context, ids []string, userId string, update bson.M) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id, userId string) (int64, error)
		GetConn() *monc.Model
		StartClient() *mongo.Client
	}

	File struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `bson:"userId,omitempty" json:"userId,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Type        string             `bson:"type,omitempty" json:"type,omitempty"`
		Path        string             `bson:"path,omitempty" json:"path,omitempty"`
		FatherId    string             `bson:"fatherId,omitempty" json:"fatherId,omitempty"`
		Size        *int64             `bson:"size,omitempty" json:"size,omitempty"`
		FileMd5     string             `bson:"fileMd5,omitempty" json:"fileMd5,omitempty"`
		IsDel       int64              `bson:"isDel,omitempty" json:"isDel,omitempty"`
		Zone        string             `bson:"zone,omitempty" json:"zone,omitempty"`
		SubZone     string             `bson:"subZone,omitempty" json:"subZone,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		Labels      []string           `bson:"labels,omitempty" json:"labels,omitempty"`
		Url         string             `bson:"url,omitempty" json:"url,omitempty"`
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

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	indexModel := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: consts.DeletedAt, Value: 1}},     // 保持原有的单个字段索引不变
			Options: options.Index().SetExpireAfterSeconds(604800), // 一周后过期
		}, {
			Keys: bson.D{
				{Key: consts.FatherId, Value: 1}, // 保证字段顺序
				{Key: consts.Name, Value: 1},
				{Key: consts.IsDel, Value: 1},
			},
			Options: options.Index().SetUnique(true), // 唯一索引
		},
	}
	_, err := conn.Indexes().CreateMany(context.Background(), indexModel)
	if err != nil {
		log.Error("fileModel index created err[%v]\n", err)
	} else {
		log.Info("fileModel index created successfully")
	}

	return &MongoMapper{
		conn: conn,
	}
}

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

func (m *MongoMapper) FindFileIsExist(ctx context.Context, md5 string) (bool, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindFileIsExist", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*File
	if err := m.conn.Find(ctx, &data, bson.M{consts.FileMd5: md5}, &options.FindOptions{Limit: lo.ToPtr(int64(1))}); err != nil {
		return false, err
	}
	if len(data) == 0 {
		return false, nil
	}
	return true, nil
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

	data.Path = data.Path + "/" + data.ID.Hex()
	key := prefixFileCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		data.Name = data.Name + "_" + strconv.FormatInt(time.Now().Unix(), 10)
		if ID, err = m.conn.InsertOne(ctx, key, data); err != nil {
			return "", err
		}
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), nil
}

func (m *MongoMapper) FindOne(ctx context.Context, fopts *FilterOptions) (*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindOne", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data File
	filter := makeMongoFilter(fopts)
	if err := m.conn.FindOneNoCache(ctx, &data, filter); err != nil {
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
		lo.Reverse(data)
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
				consts.Path:  bson.M{"$regex": "^" + path},
				consts.Size:  bson.M{"$ne": consts.FolderSize},
				consts.IsDel: gencontent.IsDel_Is_no,
			}},
		},
		{
			{"$group", bson.M{
				consts.ID:   nil,
				consts.Size: bson.M{"$sum": "$size"},
			}},
		},
	}

	result, err := m.conn.Database().Collection(CollectionName).Aggregate(ctx, pipeline)
	switch {
	case err == nil:
		if result.Next(ctx) {
			err = result.Decode(&size)
			if err != nil {
				return 0, err
			}
			fmt.Printf("[%v]\n", size)
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

	var (
		err, err1, err2 error
		data            []*File
		total           int64
	)
	if err = mr.Finish(func() error {
		data, err1 = m.FindMany(ctx, fopts, popts, sorter)
		return err1
	}, func() error {
		total, err2 = m.Count(ctx, fopts)
		return err2
	}); err != nil {
		return nil, 0, err
	}

	return data, total, nil
}

func (m *MongoMapper) Update(ctx context.Context, data *File) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Update", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	data.UpdateAt = time.Now()
	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID, consts.UserId: data.UserId}, bson.M{"$set": data})
	if err != nil {
		data.Name = data.Name + "_" + strconv.FormatInt(time.Now().Unix(), 10)
		if res, err = m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID, consts.UserId: data.UserId}, bson.M{"$set": data}); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (m *MongoMapper) UpdateMany(ctx context.Context, ids []string, userId string, update bson.M) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.UpdateMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var keys []string
	keys = lo.Map(ids, func(id string, _ int) string {
		return prefixFileCacheKey + id
	})
	filter := bson.M{consts.ID: bson.M{
		"$in": lo.Map[string, primitive.ObjectID](ids, func(s string, _ int) primitive.ObjectID {
			oid, _ := primitive.ObjectIDFromHex(s)
			return oid
		}),
	}, consts.UserId: userId}
	res, err := m.conn.UpdateMany(ctx, keys, filter, update)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MongoMapper) Delete(ctx context.Context, id, userId string) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Delete", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return 0, consts.ErrInvalidId
	}
	key := prefixFileCacheKey + id
	resp, err := m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid, consts.UserId: userId})
	return resp, err
}

func (m *MongoMapper) GetConn() *monc.Model {
	return m.conn
}

func (m *MongoMapper) StartClient() *mongo.Client {
	return m.conn.Database().Client()
}
