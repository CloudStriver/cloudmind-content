package coupon

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

const CollectionName = "coupon"

var prefixCouponCacheKey = "cache:coupon:"

var _ ICouponMongoMapper = (*MongoMapper)(nil)

type (
	ICouponMongoMapper interface {
		Insert(ctx context.Context, data *Coupon) error
		FindOne(ctx context.Context, fopts *FilterOptions) (*Coupon, error)
		Update(ctx context.Context, data *Coupon) error
		Delete(ctx context.Context, id string) error
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Coupon, error)
		Count(ctx context.Context, fopts *FilterOptions) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Coupon, int64, error)
	}
	Coupon struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `json:"userId,omitempty" bson:"userId,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Status      int64              `json:"status,omitempty" bson:"status,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		ExpireTime  int64              `json:"expireTime,omitempty" bson:"expireTime,omitempty"`

		Type          *int64    `json:"type,omitempty" bson:"type,omitempty"`
		LowSumPrice   *int64    `json:"lowSumPrice,omitempty" bson:"lowSumPrice,omitempty"`
		ProductType   *int64    `json:"productType,omitempty" bson:"productType,omitempty"`
		Discount      *int64    `json:"discount,omitempty" bson:"discount,omitempty"`
		DiscountPrice *int64    `json:"discountPrice,omitempty" bson:"discountPrice,omitempty"`
		CreateAt      time.Time `bson:"createAt,omitempty" json:"createAt,omitempty"`
		Score_        float64   `bson:"_score,omitempty" json:"_score,omitempty"`
	}
	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) ICouponMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Coupon, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)

	filter := MakeBsonFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Coupon
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

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Coupon, int64, error) {
	var (
		coupons         []*Coupon
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
		coupons, err2 = m.FindMany(ctx, fopts, popts, sorter)
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	return coupons, total, nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *Coupon) error {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
	}

	key := prefixCouponCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return err
}

func (m *MongoMapper) FindOne(ctx context.Context, fopts *FilterOptions) (*Coupon, error) {
	filter := MakeBsonFilter(fopts)
	var data Coupon
	if fopts.OnlyCouponId == nil {
		return nil, consts.ErrInvalidId
	}
	key := prefixCouponCacheKey + *fopts.OnlyCouponId
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

func (m *MongoMapper) Update(ctx context.Context, data *Coupon) error {
	key := prefixCouponCacheKey + data.ID.Hex()
	_, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	return err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return consts.ErrInvalidId
	}
	key := prefixCouponCacheKey + id
	_, err = m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return err
}
