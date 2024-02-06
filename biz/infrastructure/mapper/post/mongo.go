package post

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"

	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CollectionName = "post"
const prefixPostCacheKey = "cache:post:"

type (
	IPostMongoMapper interface {
		Insert(ctx context.Context, data *Post) error
		FindOne(ctx context.Context, id string) (*Post, error)
		Update(ctx context.Context, data *Post) error
		Delete(ctx context.Context, id string) error
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error)
		Count(ctx context.Context, fopts *FilterOptions) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, int64, error)
	}

	MongoMapper struct {
		conn *monc.Model
	}

	Post struct {
		ID       primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
		Title    string             `bson:"title,omitempty" `
		Text     string             `bson:"text,omitempty"`
		Url      string             `bson:"url,omitempty"`
		Tags     []string           `bson:"tags,omitempty"`
		UserId   string             `bson:"userId,omitempty"`
		UpdateAt time.Time          `bson:"updateAt,omitempty"`
		CreateAt time.Time          `bson:"createAt,omitempty"`
		Status   int64              `bson:"status,omitempty"`
		// 仅ES查询时使用
		Score_ float64 `bson:"_score,omitempty" json:"_score,omitempty"`
	}
)

func NewMongoMapper(config *config.Config) IPostMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)

	filter := MakeBsonFilter(fopts)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Post
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

func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, int64, error) {
	var (
		posts           []*Post
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
		posts, err2 = m.FindMany(ctx, fopts, popts, sorter)
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	return posts, total, nil
}

func (m *MongoMapper) Insert(ctx context.Context, data *Post) error {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixPostCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*Post, error) {
	var data Post
	key := prefixPostCacheKey + id
	oid, _ := primitive.ObjectIDFromHex(id)
	err := m.conn.FindOne(ctx, key, &data, bson.M{
		consts.ID: oid,
	})
	switch {
	case err == nil:
		return &data, nil
	case errors.Is(err, monc.ErrNotFound):
		return nil, consts.ErrNotFound
	default:
		return nil, err
	}
}

func (m *MongoMapper) Update(ctx context.Context, data *Post) error {
	data.UpdateAt = time.Now()
	key := prefixPostCacheKey + data.ID.Hex()
	_, err := m.conn.UpdateOne(ctx, key, bson.M{consts.ID: data.ID}, bson.M{"$set": data})
	return err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return consts.ErrInvalidId
	}
	key := prefixPostCacheKey + id
	_, err = m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return err
}
