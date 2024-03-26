package hot

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "hot"

var prefixHotCacheKey = "cache:hot:"

var _ IHotMongoMapper = (*MongoMapper)(nil)

type (
	IHotMongoMapper interface {
		FindManyByIds(ctx context.Context, ids []string) ([]*Hot, error)
		Insert(ctx context.Context, data *Hot) error
		FindOne(ctx context.Context, fopts *FilterOptions) (*Hot, error)
		Update(ctx context.Context, data *Hot) error
		Delete(ctx context.Context, id string) error
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Hot, error)
	}
	Hot struct {
		ID       primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		HotValue float64            `bson:"hotValue,omitempty" json:"hotValue"`
		Views    int64              `bson:"views,omitempty" json:"views,omitempty"`
		Score_   float64            `bson:"_score,omitempty" json:"_score,omitempty"`
		CreateAt time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		UpdateAt time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IHotMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindManyByIds(ctx context.Context, ids []string) ([]*Hot, error) {
	var (
		hots []*Hot
		err  error
	)

	fopts := &FilterOptions{OnlyHotIds: ids}
	filter := MakeBsonFilter(fopts)
	if err = m.conn.Find(ctx, &hots, filter, &options.FindOptions{
		Limit: lo.ToPtr(int64(len(ids))),
	}); err != nil {
		return nil, err
	}
	return hots, nil
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Hot, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)

	filter := MakeBsonFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Hot
	if err = m.conn.Find(ctx, &data, filter, &options.FindOptions{
		Sort:  sort,
		Limit: popts.Limit,
		Skip:  popts.Offset,
	}); err != nil {
		return nil, err
	}

	// 如果是反向查询，反转数据
	if *popts.Backward {
		lo.Reverse(data)
	}
	if len(data) > 0 {
		err = p.StoreCursor(ctx, data[0], data[len(data)-1])
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *Hot) error {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
	}
	data.CreateAt = time.Now()
	data.UpdateAt = time.Now()
	key := prefixHotCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return err
}

func (m *MongoMapper) FindOne(ctx context.Context, fopts *FilterOptions) (*Hot, error) {
	filter := MakeBsonFilter(fopts)
	var data Hot
	if fopts.OnlyHotId == nil {
		return nil, consts.ErrInvalidId
	}
	key := prefixHotCacheKey + *fopts.OnlyHotId
	err := m.conn.FindOne(ctx, key, &data, filter)
	switch {
	case err == nil:
		return &data, nil
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) Update(ctx context.Context, data *Hot) error {
	data.UpdateAt = time.Now()
	key := prefixHotCacheKey + data.ID.Hex()
	_, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	return err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return consts.ErrInvalidId
	}
	key := prefixHotCacheKey + id
	_, err = m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return err
}
