package user

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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const CollectionName = "user"

var PrefixUserCacheKey = "cache:userinfo:"

var _ IUserMongoMapper = (*MongoMapper)(nil)

type (
	IUserMongoMapper interface {
		Insert(ctx context.Context, data *User) (string, error)
		FindOne(ctx context.Context, id string) (*User, error)
		Update(ctx context.Context, data *User) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*User, error)
		FindManyByIds(ctx context.Context, ids []string) ([]*User, error)
	}
	User struct {
		ID            primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		Name          string             `bson:"name,omitempty" json:"name,omitempty"`
		Sex           int64              `bson:"sex,omitempty" json:"sex,omitempty"`
		FullName      string             `bson:"fullName,omitempty" json:"fullName,omitempty"`
		IdCard        string             `bson:"idCard,omitempty" json:"idCard,omitempty"`
		Description   string             `bson:"description,omitempty" json:"description,omitempty"`
		Url           string             `bson:"url,omitempty" json:"url,omitempty"`
		BackgroundUrl string             `bson:"backgroundUrl,omitempty" json:"backgroundUrl,omitempty"`
		UpdateAt      time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		CreateAt      time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		Labels        []string           `bson:"labels,omitempty" json:"labels,omitempty"`
		Score_        float64            `bson:"_score,omitempty" json:"_score,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IUserMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindManyByIds(ctx context.Context, ids []string) ([]*User, error) {
	var (
		users []*User
		err   error
	)

	fopts := &FilterOptions{OnlyUserIds: ids}
	filter := MakeBsonFilter(fopts)
	if err = m.conn.Find(ctx, &users, filter, &options.FindOptions{
		Limit: lo.ToPtr(int64(len(ids))),
	}); err != nil {
		return nil, err
	}
	return users, nil
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*User, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := MakeBsonFilter(fopts)
	var data []*User
	if err := m.conn.Find(ctx, &data, filter, &options.FindOptions{
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
		err := p.StoreCursor(ctx, data[0], data[len(data)-1])
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *User) (string, error) {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
	}
	data.CreateAt = time.Now()
	data.UpdateAt = time.Now()

	key := PrefixUserCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		return "", err
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*User, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}
	var data User
	key := PrefixUserCacheKey + id
	err = m.conn.FindOne(ctx, key, &data, bson.M{consts.ID: oid})
	switch {
	case err == nil:
		return &data, nil
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) Update(ctx context.Context, data *User) (*mongo.UpdateResult, error) {
	data.UpdateAt = time.Now()
	key := PrefixUserCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{"_id": data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return 0, err
	}
	key := PrefixUserCacheKey + id
	res, err := m.conn.DeleteOne(ctx, key, bson.M{"_id": oid})
	return res, err
}
