package post

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"time"

	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"

	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CollectionName = "post"
const prefixPostCacheKey = "cache:post:"

type (
	IMongoMapper interface {
		Insert(ctx context.Context, data *Post) error
		FindOne(ctx context.Context, id string) (*Post, error)
		Update(ctx context.Context, data *Post) error
		Delete(ctx context.Context, id string) error
		//FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error)
		//Count(ctx context.Context, fopts *FilterOptions) (int64, error)
		//FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, int64, error)
		//UpdateFlags(ctx context.Context, id string, flags map[Flag]bool) error
	}

	MongoMapper struct {
		conn *monc.Model
	}

	Post struct {
		ID       primitive.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
		Title    string             `bson:"title,omitempty" `
		Text     string             `bson:"text,omitempty"`
		CoverUrl string             `bson:"coverUrl,omitempty"`
		Tags     []string           `bson:"tags,omitempty"`
		UserId   string             `bson:"userId,omitempty"`
		Flags    *Flag              `bson:"flags,omitempty"`
		UpdateAt time.Time          `bson:"updateAt,omitempty"`
		CreateAt time.Time          `bson:"createAt,omitempty"`
		// 仅ES查询时使用
		Score_ float64 `bson:"_score,omitempty" json:"_score,omitempty"`
	}

	Flag int64
)

const (
	OfficialFlag = 1 << 0
)

func (f *Flag) SetFlag(flag Flag, b bool) *Flag {
	if f == nil {
		f = new(Flag)
	}
	if b {
		*f |= flag
	} else {
		*f &= ^flag
	}
	return f
}

func (f *Flag) GetFlag(flag Flag) bool {
	return f != nil && (*f&flag) > 0
}

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.CacheConf)
	return &MongoMapper{
		conn: conn,
	}
}

//func (m *MongoMapper) UpdateFlags(ctx context.Context, id string, flags map[Flag]bool) error {
//	var or, and Flag
//	for flag, v := range flags {
//		if v {
//			or += flag
//		} else {
//			and += flag
//		}
//	}
//	oid, err := primitive.ObjectIDFromHex(id)
//	if err != nil {
//		return consts.ErrInvalidId
//	}
//	_, err = m.conn.UpdateOne(ctx, prefixPostCacheKey+id, bson.M{consts.ID: oid}, bson.M{
//		"$bit": bson.M{
//			consts.Flags: bson.M{
//				"and": ^and,
//				"or":  or,
//			},
//		},
//	})
//	if err != nil {
//		return err
//	}
//	return nil
//}
//
//func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, error) {
//	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
//
//	filter := MakeBsonFilter(fopts)
//	sort, err := p.MakeSortOptions(ctx, filter)
//	if err != nil {
//		return nil, err
//	}
//
//	var data []*Post
//	if err := m.conn.Find(ctx, &data, filter, &options.FindOptions{
//		Sort:  sort,
//		Limit: popts.Limit,
//		Skip:  popts.Offset,
//	}); err != nil {
//		return nil, err
//	}
//
//	// 如果是反向查询，反转数据
//	if *popts.Backward {
//		for i := 0; i < len(data)/2; i++ {
//			data[i], data[len(data)-i-1] = data[len(data)-i-1], data[i]
//		}
//	}
//	if len(data) > 0 {
//		err = p.StoreCursor(ctx, data[0], data[len(data)-1])
//		if err != nil {
//			return nil, err
//		}
//	}
//	return data, nil
//}
//
//func (m *MongoMapper) Count(ctx context.Context, filter *FilterOptions) (int64, error) {
//	f := MakeBsonFilter(filter)
//	return m.conn.CountDocuments(ctx, f)
//}
//
//func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Post, int64, error) {
//	var posts []*Post
//	var total int64
//	wg := sync.WaitGroup{}
//	wg.Add(2)
//	c := make(chan error)
//	ctx, cancel := context.WithCancel(ctx)
//	defer cancel()
//	go func() {
//		defer wg.Done()
//		var err error
//		posts, err = m.FindMany(ctx, fopts, popts, sorter)
//		if err != nil {
//			c <- err
//			return
//		}
//	}()
//	go func() {
//		defer wg.Done()
//		var err error
//		total, err = m.Count(ctx, fopts)
//		if err != nil {
//			c <- err
//			return
//		}
//	}()
//	go func() {
//		wg.Wait()
//		defer close(c)
//	}()
//	if err := <-c; err != nil {
//		return nil, 0, err
//	}
//	return posts, total, nil
//}

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
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidId
	}

	var data Post
	key := prefixPostCacheKey + id
	err = m.conn.FindOne(ctx, key, &data, bson.M{consts.ID: oid})
	switch err {
	case nil:
		return &data, nil
	case monc.ErrNotFound:
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
