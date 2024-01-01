package comment

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

const prefixCommentCacheKey = "cache:comment:"
const CollectionName = "comment"

var _ IMongoMapper = (*MongoMapper)(nil)

/*
	一级评论 -> root = parent = "0"
	二级评论 -> root = parent != "0"
	多级评论 -> root != parent
*/

type (
	IMongoMapper interface {
		Insert(ctx context.Context, data *Comment) error
		FindOne(ctx context.Context, id string) (*Comment, error)
		Update(ctx context.Context, data *Comment) error
		Delete(ctx context.Context, id string) error
	}

	MongoMapper struct {
		conn *monc.Model
	}

	Comment struct {
		ID          primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty,omitempty"`        // 评论ID
		ObjectId    string             `bson:"objectId,omitempty" json:"objectId,omitempty"`       // 评论对象ID
		CommunityId string             `bson:"communityId,omitempty" json:"communityId,omitempty"` // 社区ID
		UserId      string             `bson:"userId,omitempty" json:"userId,omitempty"`           // 写评论用户ID
		AtUserId    string             `bson:"atUserId,omitempty" json:"atUserId,omitempty"`       // 被回复用户ID
		Root        string             `bson:"root,omitempty" json:"root,omitempty"`               // 根评论ID
		Parent      string             `bson:"parent,omitempty" json:"parent,omitempty"`           // 父评论ID
		Count       *int64             `bson:"count,omitempty" json:"count,omitempty"`             // 评论数(该字段只对一级评论有意义)
		State       int32              `bson:"state,omitempty" json:"state,omitempty"`             // 当前状态 1:正常, 2:隐藏...
		Attrs       int32              `bson:"attrs,omitempty" json:"attrs,omitempty"`             // 置顶状态 1:运营置顶, 2:作者置顶, 3:无置顶...
		Content     string             `bson:"content,omitempty" json:"content,omitempty"`         // 评论内容
		Meta        string             `bson:"meta,omitempty" json:"meta,omitempty"`               // 评论元数据(字体格式,颜色,皮肤...)
		CreateAt    time.Time          `bson:"createAt,omitempty" json:"createAt,omitempty"`       // 创建时间
		UpdateAt    time.Time          `bson:"updateAt,omitempty" json:"updateAt,omitempty"`       // 修改时间
	}
)

func NewMongoMapper(config *config.Config) IMongoMapper {
	conn := monc.MustNewModel(config.Mongo.URL, config.Mongo.DB, CollectionName, config.Cache)
	return &MongoMapper{
		conn: conn,
	}
}

//func (m *MongoMapper) FindMany(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Moment, error) {
//	p := mongop.NewMongoPaginator(pagination.NewRawStore(sorter), popts)
//
//	filter := makeMongoFilter(fopts)
//	sort, err := p.MakeSortOptions(ctx, filter)
//	if err != nil {
//		return nil, err
//	}
//
//	var data []*Moment
//	if err = m.conn.Find(ctx, &data, filter, &options.FindOptions{
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

//func (m *MongoMapper) Count(ctx context.Context, filter *FilterOptions) (int64, error) {
//	f := makeMongoFilter(filter)
//	return m.conn.CountDocuments(ctx, f)
//}

//func (m *MongoMapper) FindManyAndCount(ctx context.Context, fopts *FilterOptions, popts *pagination.PaginationOptions, sorter mongop.MongoCursor) ([]*Moment, int64, error) {
//	var data []*Moment
//	var total int64
//	wg := sync.WaitGroup{}
//	wg.Add(2)
//	c := make(chan error)
//	ctx, cancel := context.WithCancel(ctx)
//	defer cancel()
//	go func() {
//		defer wg.Done()
//		var err error
//		data, err = m.FindMany(ctx, fopts, popts, sorter)
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
//	return data, total, nil
//}

func (m *MongoMapper) Insert(ctx context.Context, data *Comment) error {
	if data.ID.IsZero() {
		data.ID = primitive.NewObjectID()
		data.CreateAt = time.Now()
		data.UpdateAt = time.Now()
	}

	key := prefixCommentCacheKey + data.ID.Hex()
	_, err := m.conn.InsertOne(ctx, key, data)
	return err
}

func (m *MongoMapper) FindOne(ctx context.Context, id string) (*Comment, error) {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, consts.ErrInvalidObjectId
	}

	var data Comment
	key := prefixCommentCacheKey + id
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

func (m *MongoMapper) Update(ctx context.Context, data *Comment) error {
	data.UpdateAt = time.Now()
	key := prefixCommentCacheKey + data.ID.Hex()
	_, err := m.conn.UpdateByID(ctx, key, data.ID, bson.M{"$set": data})
	return err
}

func (m *MongoMapper) Delete(ctx context.Context, id string) error {
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return consts.ErrInvalidObjectId
	}
	key := prefixCommentCacheKey + id
	_, err = m.conn.DeleteOne(ctx, key, bson.M{consts.ID: oid})
	return err
}
