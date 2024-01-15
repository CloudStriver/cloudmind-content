package file

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
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

const CollectionName = "File"

var prefixFileCacheKey = "cache:file:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		Count(ctx context.Context, filter *FilterOptions) (int64, error)
		Insert(ctx context.Context, data *File) (string, error)
		FindOne(ctx context.Context, fopts *FilterOptions) (*File, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error)
		FindByMd5(ctx context.Context, md5 string) (*File, error)
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
		Type        int32              `bson:"type,omitempty" json:"type,omitempty"`
		Path        string             `bson:"path,omitempty" json:"path,omitempty"`
		FatherId    string             `bson:"fatherId,omitempty" json:"fatherId,omitempty"`
		Size        *int64             `bson:"size,omitempty" json:"size,omitempty"`
		FileMd5     string             `bson:"fileMd5,omitempty" json:"fileMd5,omitempty"`
		IsDel       int32              `bson:"isDel,omitempty" json:"isDel,omitempty"`
		Tag         []string           `bson:"tag,omitempty" json:"tag,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		DeletedAt   time.Time          `bson:"deletedAt,omitempty" json:"deletedAt,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}

	AggregateSizeResult struct {
		Size int64 `bson:"size"`
	}
)

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Insert(ctx context.Context, data *File) (string, error) {
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
	var data File
	if fopts.OnlyFileId != nil {
		_, err := primitive.ObjectIDFromHex(*fopts.OnlyFileId)
		if err != nil {
			return nil, consts.ErrInvalidId
		}
	}

	filter := makeMongoFilter(fopts)
	key := prefixFileCacheKey + *fopts.OnlyFileId
	err := m.conn.FindOne(ctx, key, &data, filter)
	switch {
	case err == nil:
		return &data, nil
	case errorx.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) FindByMd5(ctx context.Context, md5 string) (*File, error) {
	var data File
	err := m.conn.FindOneNoCache(ctx, &data, bson.M{"md5": md5})
	switch {
	case err == nil:
		return &data, nil
	case errorx.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) Count(ctx context.Context, fopts *FilterOptions) (int64, error) {
	filter := makeMongoFilter(fopts)
	return m.conn.CountDocuments(ctx, filter)
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*File, error) {
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
	data.UpdateAt = time.Now()
	key := prefixFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
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
