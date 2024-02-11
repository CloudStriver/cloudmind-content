package product

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "product"

var prefixProductCacheKey = "cache:product:"

var _ IProductMongoMapper = (*MongoMapper)(nil)

type (
	IProductMongoMapper interface {
		Insert(ctx context.Context, data *Product) (string, error)
		FindOne(ctx context.Context, id string) (*Product, error)
		Update(ctx context.Context, data *Product) error
		Delete(ctx context.Context, id string) error
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Product, error)
		Count(ctx context.Context, fopts *FilterOptions) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Product, int64, error)
	}
	Product struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `json:"userId,omitempty" bson:"userId,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Status      int64              `json:"status,omitempty" bson:"status,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		Urls        []string           `bson:"urls,omitempty" json:"urls,omitempty"`
		Tags        []string           `bson:"tags,omitempty" json:"tags,omitempty"`
		Type        int64              `json:"type,omitempty" bson:"type,omitempty"`
		Price       int64              `json:"price,omitempty" bson:"price,omitempty"`
		ProductSize int64              `json:"productSize,omitempty" bson:"productSize,omitempty"`
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		Score_      float64            `bson:"_score,omitempty" json:"_score,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IProductMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Product, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)

	filter := MakeBsonFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Product
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

func (m *MongoMapper) Count(ctx context.Context, filter *FilterOptions) (int64, error) {
	f := MakeBsonFilter(filter)
	return m.conn.CountDocuments(ctx, f)
}

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Product, int64, error) {
	var (
		products        []*Product
		total           int64
		err, err1, err2 error
	)
	if err = mr.Finish(func() error {
		total, err1 = m.Count(ctx, fopts)
		if err1 != nil {
			return err1
		}
		return nil
	}, func() error {
		products, err2 = m.FindMany(ctx, fopts, popts, sorter)
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	return products, total, nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *Product) (string, error) {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixProductCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return data.ID.Hex(), err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*Product, error) {
	var data Product
	key := prefixProductCacheKey + id
	oid, _ := primitive.ObjectIDFromHex(id)
	err := m.conn.FindOne(ctx, key, &data, bson.M{consts.ID: oid})
	switch {
	case err == nil:
		return &data, nil
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) Update(ctx context.Context, data *Product) error {
	data.UpdateAt = time.Now()
	key := prefixProductCacheKey + data.ID.Hex()
	_, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	return err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return consts.ErrInvalidId
	}
	key := prefixProductCacheKey + id
	_, err = m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return err
}
