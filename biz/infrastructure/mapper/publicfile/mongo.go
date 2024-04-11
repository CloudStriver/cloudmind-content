package publicfile

import (
	"context"
	"errors"
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
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"time"
)

const CollectionName = "publicFile"

var prefixFileCacheKey = "cache:publicFile:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		Count(ctx context.Context, filter *FilterOptions) (int64, error)
		Insert(ctx context.Context, data *PublicFile) (string, error)
		InsertMany(ctx context.Context, data []*PublicFile) ([]string, error)
		FindOne(ctx context.Context, id string) (*PublicFile, error)
		Find(ctx context.Context, filter bson.M) ([]*PublicFile, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*PublicFile, error)
		FindManyByIds(ctx context.Context, ids []string) ([]*PublicFile, error)
		FindFolderSize(ctx context.Context, path string) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*PublicFile, int64, error)
		Update(ctx context.Context, data *PublicFile) (*mongo.UpdateResult, error)
		UpdateMany(ctx context.Context, ids []string, update bson.M) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		DeleteMany(ctx context.Context, ids []string) (int64, error)
		GetConn() *monc.Model
		StartClient() *mongo.Client
	}

	PublicFile struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `bson:"userId,omitempty" json:"userId,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Type        string             `bson:"type,omitempty" json:"type,omitempty"`
		Size        int64              `bson:"size,omitempty" json:"size,omitempty"`
		Path        string             `bson:"path,omitempty" json:"path,omitempty"`
		FileMd5     string             `bson:"fileMd5,omitempty" json:"fileMd5,omitempty"`
		Zone        string             `bson:"zone,omitempty" json:"zone,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		AuditStatus int64              `bson:"auditStatus,omitempty" json:"auditStatus,omitempty"`
		Labels      []string           `bson:"labels,omitempty" json:"labels,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
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
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Insert(ctx context.Context, data *PublicFile) (string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Insert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
	}
	data.CreateAt = time.Now()
	data.Path = data.Path + "/" + data.ID.Hex()
	key := prefixFileCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		log.CtxError(ctx, "插入文件信息: 发生异常[%v]\n", err)
		return "", err
	}
	return data.ID.Hex(), nil
}

func (m *MongoMapper) InsertMany(ctx context.Context, data []*PublicFile) ([]string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.InsertMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	ids := make([]string, len(data))
	for i := 0; i < len(data); i++ {
		if data[i].ID.IsZero() {
			data[i].ID = primitive.NewObjectID()
			data[i].CreateAt = time.Now()
			ids[i] = data[i].ID.Hex()
		}
	}
	dataAny := lo.Map(data, func(item *PublicFile, _ int) any { return item })
	_, err := m.conn.InsertMany(ctx, dataAny)
	return ids, err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*PublicFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindOne", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidId
	}
	key := prefixFileCacheKey + id
	var data PublicFile
	err = m.conn.FindOne(ctx, key, &data, bson.M{consts.ID: oid})
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	case err == nil:
		return &data, nil
	default:
		log.CtxError(ctx, "查询文件详细信息: 发生异常[%v]\n", err)
		return nil, err
	}
}

func (m *MongoMapper) Find(ctx context.Context, filter bson.M) ([]*PublicFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Find", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*PublicFile
	err := m.conn.Find(ctx, &data, filter)
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	case err != nil:
		log.CtxError(ctx, "查询文件列表: 发生异常[%v]\n", err)
		return nil, err
	}
	return data, nil
}

func (m *MongoMapper) FindManyByIds(ctx context.Context, ids []string) ([]*PublicFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyByIds", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*PublicFile
	fopts := &FilterOptions{OnlyFileIds: ids}
	filter := makeMongoFilter(fopts)
	// 创建聚合管道
	pipeline := mongo.Pipeline{
		{{"$match", filter}}, // 应用筛选条件
		{{"$addFields", bson.M{
			"description": bson.M{"$substrCP": []interface{}{"description", 0, 200}},
		}}},
	}

	// 使用聚合管道执行查询
	err := m.conn.Aggregate(ctx, &data, pipeline)
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	case err == nil:
		return data, nil
	default:
		log.CtxError(ctx, "获取文件列表异常[%v]\n", err)
		return nil, err
	}
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
				consts.Size:  bson.M{"$ne": int64(gencontent.Folder_Folder_Size)},
				consts.IsDel: int64(gencontent.Deletion_Deletion_notDel),
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
				log.CtxError(ctx, "查询文件夹大小: 解码结果失败[%v]\n", err)
				return 0, err
			}
		} else {
			log.CtxError(ctx, "查询文件夹大小: 未查询到结果\n")
			return 0, nil
		}

		if err = result.Err(); err != nil {
			log.CtxError(ctx, "查询文件夹大小: 获取结果失败[%v]\n", err)
			return 0, err
		}
		return size.Size, nil
	case errors.Is(err, monc.ErrNotFound):
		return 0, consts.ErrNotFound
	default:
		log.CtxError(ctx, "查询文件夹大小: 发生异常[%v]\n", err)
		return 0, err
	}
}

func (m *MongoMapper) Count(ctx context.Context, fopts *FilterOptions) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Count", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	filter := makeMongoFilter(fopts)
	return m.conn.CountDocuments(ctx, filter)
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*PublicFile, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := makeMongoFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*PublicFile
	pipeline := mongo.Pipeline{
		{{"$match", filter}}, // 应用筛选条件
		{{"$addFields", bson.M{
			"description": bson.M{"$substrCP": []interface{}{"description", 0, 200}},
		}}},
	}

	// 考虑到排序和分页
	pipeline = append(pipeline, bson.D{{"$sort", sort}})
	if popts.Limit != nil {
		pipeline = append(pipeline, bson.D{{"$limit", *popts.Limit}})
	}
	if popts.Offset != nil {
		pipeline = append(pipeline, bson.D{{"$skip", *popts.Offset}})
	}

	// 使用聚合管道执行查询
	err = m.conn.Aggregate(ctx, &data, pipeline)
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	case err != nil:
		log.CtxError(ctx, "查询文件列表: 发生异常[%v]\n", err)
		return nil, err
	}

	// 如果是反向查询，反转数据
	if *popts.Backward {
		lo.Reverse(data)
	}

	if len(data) > 0 {
		switch sorter.(type) {
		case *mongop.CreateAtDescCursor:
			if err = p.StoreCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.CreateAtAscCursor:
			if err = p.StoreCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.NameAscCursor:
			if err = p.StoreStringCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.NameDescCursor:
			if err = p.StoreStringCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.UpdateAtAscCursor:
			if err = p.StoreTimeCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.UpdateAtDescCursor:
			if err = p.StoreTimeCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.TypeAscCursor:
			if err = p.StoreStringCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		case *mongop.TypeDescCursor:
			if err = p.StoreStringCursor(ctx, data[0], data[len(data)-1]); err != nil {
				return nil, err
			}
		}
	}
	return data, nil
}

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*PublicFile, int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyAndCount", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()
	var (
		err, err1, err2 error
		data            []*PublicFile
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

func (m *MongoMapper) Update(ctx context.Context, data *PublicFile) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Update", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	if err != nil {
		log.CtxError(ctx, "更新文件信息: 发生异常[%v]\n", err)
		return res, err
	}
	return res, nil
}

func (m *MongoMapper) UpdateMany(ctx context.Context, ids []string, update bson.M) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.UpdateMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var keys []string
	keys = lo.Map(ids, func(id string, _ int) string {
		return prefixFileCacheKey + id
	})
	filter := makeMongoFilter(&FilterOptions{OnlyFileIds: ids})
	res, err := m.conn.UpdateMany(ctx, keys, filter, update)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Delete", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()
	oid, _ := primitive.ObjectIDFromHex(id)
	key := prefixFileCacheKey + id
	resp, err := m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	if err != nil {
		log.CtxError(ctx, "删除文件信息: 发生异常[%v]\n", err)
		return 0, err
	}
	return resp, err
}

func (m *MongoMapper) DeleteMany(ctx context.Context, ids []string) (int64, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.DeleteMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var resp int64
	var err1, err2 error
	keys := lo.Map(ids, func(id string, _ int) string {
		return prefixFileCacheKey + id
	})
	filter := makeMongoFilter(&FilterOptions{OnlyFileIds: ids})
	err := mr.Finish(func() error {
		resp, err1 = m.conn.DeleteMany(ctx, filter)
		return err1
	}, func() error {
		err2 = m.conn.DelCache(ctx, keys...)
		return err2
	})
	if err != nil {
		log.CtxError(ctx, "删除文件信息: 发生异常[%v]\n", err)
		return 0, err
	}
	return resp, err
}

func (m *MongoMapper) GetConn() *monc.Model {
	return m.conn
}

func (m *MongoMapper) StartClient() *mongo.Client {
	return m.conn.Database().Client()
}
