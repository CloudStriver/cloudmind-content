package file

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
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"strconv"
	"strings"
	"time"
)

const CollectionName = "file"

var prefixFileCacheKey = "cache:file:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		FindFileIsExist(ctx context.Context, md5 string) (bool, error)
		Count(ctx context.Context, filter *FilterOptions) (int64, error)
		Insert(ctx context.Context, data *File) (string, string, error)
		FindAndInsert(ctx context.Context, data *File) (string, string, error)
		FindAndInsertMany(ctx context.Context, data []*File) ([]string, error)
		FindOne(ctx context.Context, id string) (*File, error)
		Find(ctx context.Context, filter bson.M) ([]*File, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error)
		FindManyByIds(ctx context.Context, ids []string) ([]*File, error)
		FindFolderSize(ctx context.Context, path string) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, int64, error)
		FindAndUpdate(ctx context.Context, data *File) (*mongo.UpdateResult, error)
		Update(ctx context.Context, data *File) (*mongo.UpdateResult, error)
		UpdateUnset(ctx context.Context, data *File, update bson.M) (*mongo.UpdateResult, error)
		UpdateMany(ctx context.Context, ids []string, update bson.M) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		DeleteMany(ctx context.Context, ids []string) (int64, error)
		Rename(data *File)
		GetConn() *monc.Model
		StartClient() *mongo.Client
	}

	File struct {
		ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId    string             `bson:"userId,omitempty" json:"userId,omitempty"`
		Name      string             `bson:"name,omitempty" json:"name,omitempty"`
		Category  int64              `bson:"category,omitempty" json:"category,omitempty"`
		Type      string             `bson:"type,omitempty" json:"type,omitempty"`
		Path      string             `bson:"path,omitempty" json:"path,omitempty"`
		FatherId  string             `bson:"fatherId,omitempty" json:"fatherId,omitempty"`
		Size      int64              `bson:"size,omitempty" json:"size,omitempty"`
		FileMd5   string             `bson:"fileMd5,omitempty" json:"fileMd5,omitempty"`
		IsDel     int64              `bson:"isDel,omitempty" json:"isDel,omitempty"`
		CreateAt  time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		UpdateAt  time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		DeletedAt time.Time          `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
		Score_    float64            `bson:"score_,omitempty" json:"score_,omitempty"`
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

func (m *MongoMapper) Rename(data *File) {
	var builder strings.Builder
	var flag bool
	if data.Size != -1 {
		for i := len(data.Name) - 1; i >= 0; i-- {
			if data.Name[i] == '.' {
				builder.WriteString(data.Name[:i])
				builder.WriteString("_" + strconv.FormatInt(time.Now().UnixMicro(), 10))
				builder.WriteString(data.Name[i:])
				flag = true
				break
			}
		}
	}

	if !flag {
		builder.WriteString(data.Name)
		builder.WriteString("_" + strconv.FormatInt(time.Now().UnixMicro(), 10))
	}

	data.Name = builder.String()
	builder.Reset()
}

func (m *MongoMapper) FindManyByIds(ctx context.Context, ids []string) ([]*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindManyByIds", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*File
	fopts := &FilterOptions{OnlyFileIds: ids}
	filter := makeMongoFilter(fopts)
	err := m.conn.Find(ctx, &data, filter)
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

func (m *MongoMapper) FindAndInsert(ctx context.Context, data *File) (string, string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindAndInsert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var res File
	if err := m.conn.FindOneNoCache(ctx, &res, bson.M{consts.FatherId: data.FatherId, consts.Name: data.Name, consts.IsDel: data.IsDel}); err != nil {
		if errors.Is(err, monc.ErrNotFound) {
			return m.Insert(ctx, data)
		}
		return "", "", err
	}

	m.Rename(data)
	return m.Insert(ctx, data)
}

func (m *MongoMapper) FindAndInsertMany(ctx context.Context, data []*File) ([]string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindAndInsertMany", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	mp := make(map[string]int)
	for i := 0; i < len(data); i++ {
		if data[i].ID.IsZero() {
			data[i].ID = primitive.NewObjectID()
			data[i].CreateAt = time.Now()
		}
		data[i].UpdateAt = time.Now()
		mp[data[i].ID.Hex()] = i
		data[i].Path = data[i].Path + "/" + data[i].ID.Hex()
	}

	if err := mr.Finish(lo.Map(data, func(item *File, _ int) func() error {
		return func() error {
			var file File
			if err := m.conn.FindOneNoCache(ctx, &file, bson.M{consts.FatherId: item.FatherId, consts.Name: item.Name, consts.IsDel: item.IsDel}); err != nil {
				if errors.Is(err, monc.ErrNotFound) {
					return nil
				} else {
					return err
				}
			}
			m.Rename(item)
			return nil
		}
	})...); err != nil {
		return nil, err
	}

	dataAny := lo.Map(data, func(item *File, _ int) any { return item })
	res, err := m.conn.InsertMany(ctx, dataAny)
	ids := make([]string, len(dataAny))
	for _, v := range res.InsertedIDs {
		ids[mp[v.(primitive.ObjectID).Hex()]] = v.(primitive.ObjectID).Hex()
	}
	return ids, err
}

func (m *MongoMapper) FindFileIsExist(ctx context.Context, md5 string) (bool, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindFileIsExist", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*File
	key := prefixFileCacheKey + md5
	err := m.conn.FindOne(ctx, key, &data, bson.M{consts.FileMd5: md5})
	switch {
	case errors.Is(err, monc.ErrNotFound):
		return false, nil
	case err == nil:
		return true, nil
	default:
		log.CtxError(ctx, "查询文件md5值是否存在: 发生异常[%v]\n", err)
		return false, err
	}
}

func (m *MongoMapper) Insert(ctx context.Context, data *File) (string, string, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Insert", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
	}
	data.CreateAt = time.Now()
	data.UpdateAt = time.Now()
	data.Path = data.Path + "/" + data.ID.Hex()
	key := prefixFileCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		m.Rename(data)
		if _, err = m.conn.InsertOne(ctx, key, data); err != nil {
			log.CtxError(ctx, "插入文件信息: 发生异常[%v]\n", err)
			return "", "", err
		}
	}
	return data.ID.Hex(), data.Name, nil
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindOne", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidId
	}
	key := prefixFileCacheKey + id
	var data File
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

func (m *MongoMapper) Find(ctx context.Context, filter bson.M) ([]*File, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Find", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	var data []*File
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
	err = m.conn.Find(ctx, &data, filter, &options.FindOptions{
		Sort:  sort,
		Limit: popts.Limit,
		Skip:  popts.Offset,
	})
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

func (m *MongoMapper) FindAndUpdate(ctx context.Context, data *File) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.FindAndUpdate", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	data.UpdateAt = time.Now()
	var res File
	if err := m.conn.FindOneNoCache(ctx, &res, bson.M{consts.FatherId: data.FatherId, consts.Name: data.Name, consts.IsDel: data.IsDel}); err != nil {
		if errors.Is(err, monc.ErrNotFound) {
			return m.Update(ctx, data)
		}
		return nil, err
	}

	m.Rename(data)
	return m.Update(ctx, data)
}

func (m *MongoMapper) Update(ctx context.Context, data *File) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.Update", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	data.UpdateAt = time.Now()
	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	if err != nil {
		m.Rename(data)
		if res, err = m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data}); err != nil {
			log.CtxError(ctx, "更新文件信息: 发生异常[%v]\n", err)
			return res, err
		}
	}
	return res, nil
}

func (m *MongoMapper) UpdateUnset(ctx context.Context, data *File, update bson.M) (*mongo.UpdateResult, error) {
	tracer := otel.GetTracerProvider().Tracer(trace.TraceName)
	_, span := tracer.Start(ctx, "mongo.UpdateUnset", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	defer span.End()

	data.UpdateAt = time.Now()
	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data, "$unset": update})
	if err != nil {
		m.Rename(data)
		if res, err = m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data, "$unset": update}); err != nil {
			log.CtxError(ctx, "更新文件信息: 发生异常[%v]\n", err)
			return res, err
		}
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
