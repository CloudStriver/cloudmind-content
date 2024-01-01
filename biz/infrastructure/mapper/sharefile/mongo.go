package sharefile

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

const CollectionName = "ShareFile"

var prefixPublicFileCacheKey = "cache:shareFile:"

var _ IMongoMapper = (*MongoMapper)(nil)

type (
	IMongoMapper interface {
		Insert(ctx context.Context, data *PublicFile) (string, error)
		FindOne(ctx context.Context, id string) (*PublicFile, error)
		Update(ctx context.Context, data *PublicFile) (*mongo.UpdateResult, error)
		Delete(ctx context.Context, id string) (int64, error)
		GetConn() *monc.Model
	}

	PublicFile struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
		UserId      string             `bson:"userId,omitempty"`
		Name        string             `bson:"name,omitempty"`
		Type        int32              `bson:"type,omitempty"`
		Path        string             `bson:"path,omitempty"`
		FatherId    string             `bson:"fatherId,omitempty"`
		Size        *int64             `bson:"size,omitempty"`
		FileMd5     string             `bson:"fileMd5,omitempty"`
		Tag         string             `bson:"tag,omitempty"`
		Description string             `bson:"description,omitempty"`
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
	}

	MongoMapper struct {
		conn *monc.Model
	}
)

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.Cache)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) Insert(ctx context.Context, data *PublicFile) (string, error) {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixPublicFileCacheKey + data.ID.Hex()
	ID, err := m.conn.InsertOne(ctx, key, data)
	if err != nil {
		return "", err
	}
	return ID.InsertedID.(primitive.ObjectID).Hex(), err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*PublicFile, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidObjectId
	}
	var data PublicFile
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

func (m *MongoMapper) Update(ctx context.Context, data *PublicFile) (*mongo.UpdateResult, error) {
	data.UpdateAt = time.Now()
	key := prefixPublicFileCacheKey + data.ID.Hex()
	res, err := m.conn.UpdateOne(ctx, key, bson.M{"_id": data.ID}, bson.M{"$set": data})
	return res, err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) (int64, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return 0, consts.ErrInvalidObjectId
	}
	key := prefixPublicFileCacheKey + id
	res, err := m.conn.DeleteOne(ctx, key, bson.M{"_id": oid})
	return res, err
}

func (m *MongoMapper) GetConn() *monc.Model {
	return m.conn
}
