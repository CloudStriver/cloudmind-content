package sharefile

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "ShareFile"

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
		Status        int32              `bson:"status,omitempty" json:"status,omitempty"`               // 链接当前状态
		Limit         int32              `bson:"limit,omitempty" json:"limit,omitempty"`                 // -1：不限制，其余为限制的次数(不包含0) -> 此处为人数限制
		Persons       []string           `bson:"persons,omitempty" json:"persons,omitempty"`             // 当前访问人数
		EffectiveTime int32              `bson:"effectiveTime,omitempty" json:"effectiveTime,omitempty"` // 有效期
		BrowseNumber  *int64             `bson:"browseNumber,omitempty" json:"browseNumber,omitempty"`   // 浏览次数
		CreateAt      time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`           // 创建时间
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Count(ctx context.Context, fopts *ShareCodeOptions) (int64, error) {
	filter := makeMongoShareCodeFilter(fopts)
	return m.conn.CountDocuments(ctx, filter)
}

func (m *MongoMapper) Insert(ctx context.Context, data *ShareFile) (string, error) {
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
	key := prefixPublicFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{"_id": data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, fopts *ShareCodeOptions) (int64, error) {
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
