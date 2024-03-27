package mongop

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type (
	CreateAtDescCursor struct {
		ID string `json:"_id"`
	}
	UpdateAtDescCursor struct {
		UpdateAt time.Time `json:"updateAt"`
	}
	NameDescCursor struct {
		Name string `json:"name"`
	}
	TypeDescCursor struct {
		Type string `json:"type"`
	}
	CreateAtAscCursor struct {
		ID string `json:"_id"`
	}
	UpdateAtAscCursor struct {
		UpdateAt time.Time `json:"updateAt"`
	}
	NameAscCursor struct {
		Name string `json:"name"`
	}
	TypeAscCursor struct {
		Type string `json:"type"`
	}
)

var (
	CreateAtDescCursorType = (*CreateAtDescCursor)(nil)
	CreateAtAscCursorType  = (*CreateAtAscCursor)(nil)
	UpdateAtDescCursorType = (*UpdateAtDescCursor)(nil)
	UpdateAtAscCursorType  = (*UpdateAtAscCursor)(nil)
	NameDescCursorType     = (*NameDescCursor)(nil)
	NameAscCursorType      = (*NameAscCursor)(nil)
	TypeDescCursorType     = (*TypeDescCursor)(nil)
	TypeAscCursorType      = (*TypeAscCursor)(nil)
)

// 降序
func (s *CreateAtDescCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	var id primitive.ObjectID
	var err error
	if s == nil {
		if backward {
			id = primitive.NewObjectIDFromTimestamp(time.Unix(0, 0))
		} else {
			id = primitive.NewObjectIDFromTimestamp(time.Unix(math.MaxInt64, 0))
		}
	} else {
		id, err = primitive.ObjectIDFromHex(s.ID)
		if err != nil {
			return nil, err
		}
	}

	var sort bson.M
	if backward {
		filter["_id"] = bson.M{"$gt": id}
		sort = bson.M{"_id": 1}
	} else {
		filter["_id"] = bson.M{"$lt": id}
		sort = bson.M{"_id": -1}
	}
	return sort, err
}

func (s *CreateAtAscCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	var id primitive.ObjectID
	var err error
	if s == nil {
		if !backward {
			id = primitive.NewObjectIDFromTimestamp(time.Unix(0, 0))
		} else {
			id = primitive.NewObjectIDFromTimestamp(time.Unix(math.MaxInt64, 0))
		}
	} else {
		id, err = primitive.ObjectIDFromHex(s.ID)
		if err != nil {
			return nil, err
		}
	}

	var sort bson.M
	if !backward {
		filter["_id"] = bson.M{"$gt": id}
		sort = bson.M{"_id": 1}
	} else {
		filter["_id"] = bson.M{"$lt": id}
		sort = bson.M{"_id": -1}
	}
	return sort, err
}

func (s *UpdateAtDescCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	//构造lastId
	var id time.Time
	var err error
	if s == nil {
		if backward {
			id = time.Unix(0, 0)
		} else {
			id = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
		}
	} else {
		id = s.UpdateAt
	}

	var sort bson.M
	if backward {
		filter["updateAt"] = bson.M{"$gt": id}
		sort = bson.M{"updateAt": 1}
	} else {
		filter["updateAt"] = bson.M{"$lt": id}
		sort = bson.M{"updateAt": -1}
	}
	return sort, err
}

func (s *UpdateAtAscCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	//构造lastId
	var id time.Time
	var err error
	if s == nil {
		if !backward {
			id = time.Unix(0, 0)
		} else {
			id = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
		}
	} else {
		id = s.UpdateAt
	}

	var sort bson.M
	if !backward {
		filter["updateAt"] = bson.M{"$gt": id}
		sort = bson.M{"updateAt": 1}
	} else {
		filter["updateAt"] = bson.M{"$lt": id}
		sort = bson.M{"updateAt": -1}
	}
	return sort, err
}

func (s *NameDescCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	var data string
	var err error
	if s == nil {
		var builder strings.Builder
		if backward {
			for i := 0; i <= 100; i++ {
				builder.WriteString(" ")
			}
		} else {
			for i := 0; i <= 100; i++ {
				builder.WriteString("\U0010FFFF")
			}
		}
		data = builder.String()
		builder.Reset()
	} else {
		data = s.Name
	}

	var sort bson.M
	if backward {
		filter[consts.Name] = bson.M{"$gt": data}
		sort = bson.M{consts.Name: 1}
	} else {
		filter[consts.Name] = bson.M{"$lt": data}
		sort = bson.M{consts.Name: -1}
	}
	return sort, err
}

func (s *NameAscCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	var data string
	var err error
	if s == nil {
		var builder strings.Builder
		if !backward {
			for i := 0; i <= 100; i++ {
				builder.WriteString(" ")
			}
		} else {
			for i := 0; i <= 100; i++ {
				builder.WriteString("\U0010FFFF")
			}
		}
		data = builder.String()
		builder.Reset()
	} else {
		data = s.Name
	}

	var sort bson.M
	if !backward {
		filter[consts.Name] = bson.M{"$gt": data}
		sort = bson.M{consts.Name: 1}
	} else {
		filter[consts.Name] = bson.M{"$lt": data}
		sort = bson.M{consts.Name: -1}
	}
	return sort, err
}

func (s *TypeDescCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	//构造lastId
	var data string
	var err error
	if s == nil {
		var builder strings.Builder
		if backward {
			for i := 0; i <= 100; i++ {
				builder.WriteString(" ")
			}
		} else {
			for i := 0; i <= 100; i++ {
				builder.WriteString("\U0010FFFF")
			}
		}
		data = builder.String()
		builder.Reset()
	} else {
		data = s.Type
	}

	var sort bson.M
	if backward {
		filter[consts.Type] = bson.M{"$gt": data}
		sort = bson.M{consts.Type: 1}
	} else {
		filter[consts.Type] = bson.M{"$lt": data}
		sort = bson.M{consts.Type: -1}
	}
	return sort, err
}

func (s *TypeAscCursor) MakeSortOptions(filter bson.M, backward bool) (bson.M, error) {
	//构造lastId
	var data string
	var err error
	if s == nil {
		var builder strings.Builder
		if !backward {
			for i := 0; i <= 100; i++ {
				builder.WriteString(" ")
			}
		} else {
			for i := 0; i <= 100; i++ {
				builder.WriteString("\U0010FFFF")
			}
		}
		data = builder.String()
		builder.Reset()
	} else {
		data = s.Type
	}

	var sort bson.M
	if !backward {
		filter[consts.Type] = bson.M{"$gt": data}
		sort = bson.M{consts.Type: 1}
	} else {
		filter[consts.Type] = bson.M{"$lt": data}
		sort = bson.M{consts.Type: -1}
	}
	return sort, err
}
