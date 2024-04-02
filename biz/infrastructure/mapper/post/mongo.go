package post

import (
	"context"
	"errors"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/go-pkg/utils/pagination"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"go.mongodb.org/mongo-driver/mongo"
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
		Insert(ctx context.Context, data *Post) (string, error)
		FindOne(ctx context.Context, id string) (*Post, error)
		Update(ctx context.Context, data *Post) error
		Delete(ctx context.Context, id []string) error
		FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error)
		Count(ctx context.Context, fopts *FilterOptions) (int64, error)
		FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, int64, error)
		FindManyByIds(ctx context.Context, ids []string) ([]*Post, error)
	}

	MongoMapper struct {
		conn *monc.Model
	}

	Post struct {
		ID       primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
		Title    string             `bson:"title,omitempty" json:"title,omitempty"`
		Text     string             `bson:"text,omitempty" json:"text,omitempty"`
		Url      string             `bson:"url,omitempty" json:"url,omitempty"`
		Tags     []*content.Tag     `bson:"tags,omitempty" json:"tags,omitempty"`
		UserId   string             `bson:"userId,omitempty" json:"userId,omitempty"`
		UpdateAt time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`
		CreateAt time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`
		Status   int64              `bson:"status,omitempty" json:"status,omitempty"`
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

func (m *MongoMapper) FindManyByIds(ctx context.Context, ids []string) ([]*Post, error) {
	var (
		posts []*Post
		err   error
	)

	fopts := &FilterOptions{OnlyPostIds: ids}
	filter := MakeBsonFilter(fopts)
	// 创建聚合管道
	pipeline := mongo.Pipeline{
		{{"$match", filter}}, // 应用筛选条件
		{{"$addFields", bson.M{
			"text": bson.M{"$substrCP": []interface{}{"$text", 0, 200}},
		}}},
	}

	// 使用聚合管道执行查询
	if err = m.conn.Aggregate(ctx, &posts, pipeline); err != nil {
		return nil, err
	}

	return posts, nil
}

func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error) {
	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
	filter := MakeBsonFilter(fopts)
	fmt.Println(filter)
	sort, err := p.MakeSortOptions(ctx, filter)
	if err != nil {
		return nil, err
	}

	var data []*Post
	// 创建聚合管道
	pipeline := mongo.Pipeline{
		{{"$match", filter}}, // 应用筛选条件
		{{"$addFields", bson.M{
			"text": bson.M{"$substrCP": []interface{}{"$text", 0, 200}},
		}}},
	}

	// 考虑到排序和分页
	pipeline = append(pipeline, bson.D{{"$sort", sort}})
	if popts.Limit != nil {
		pipeline = append(pipeline, bson.D{{"$limit", *popts.Limit}})
	}
	if popts.Offset != nil {
		pipeline = append(pipeline, bson.D{{"$skip", *popts.Offset}})
	}

	// 使用聚合管道执行查询
	if err = m.conn.Aggregate(ctx, &data, pipeline); err != nil {
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

func (m *MongoMapper) Insert(ctx context.Context, data *Post) (string, error) {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixPostCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return data.ID.Hex(), err
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

func (m *MongoMapper) Delete(ctx context.Context, id []string) error {
	oids := lo.Map[string, primitive.ObjectID](id, func(item string, _ int) primitive.ObjectID {
		oid, _ := primitive.ObjectIDFromHex(item)
		return oid
	})
	keys := lo.Map(id, func(item string, index int) string {
		return prefixPostCacheKey + item
	})
	if _, err := m.conn.DeleteMany(ctx, bson.M{
		consts.ID: bson.M{
			"$in": oids,
		},
	}); err != nil {
		return err
	}

	if err := m.conn.DelCache(ctx, keys...); err != nil {
		return err
	}

	return nil
}
