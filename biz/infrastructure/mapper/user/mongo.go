package user

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const CollectionName = "user"

var PrefixUserCacheKey = "cache:user:"

var _ IUserMongoMapper = (*MongoMapper)(nil)

type (
	IUserMongoMapper interface {
		Insert(ctx context.Context, data *User) (string, error)
		FindOne(ctx context.Context, id string) (*User, error)
		Update(ctx context.Context, data *User) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
	}
	User struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		Name        string             `bson:"name,omitempty" json:"name,omitempty"`
		Sex         int64              `bson:"sex,omitempty" json:"sex,omitempty"`
		FullName    string             `bson:"fullName,omitempty" json:"fullName,omitempty"`
		IdCard      string             `bson:"idCard,omitempty" json:"idCard,omitempty"`
		Description string             `bson:"description,omitempty" json:"description,omitempty"`
		Url         string             `bson:"url,omitempty" json:"url,omitempty"`
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		Score_      float64            `bson:"_score,omitempty" json:"_score,omitempty"`
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
	err = m.conn.FindOne(ctx, key, &data, bson.M{"_id": oid})
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
