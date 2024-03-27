package pagination

import (
	"context"
	"encoding/json"
	"github.com/jinzhu/copier"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"reflect"
)

//const (
//	suffixFront   = ":front"
//	suffixBack    = ":back"
//	defaultExpire = time.Minute * 5
//)

var copierOpt = &copier.Option{Converters: []copier.TypeConverter{{
	SrcType: primitive.ObjectID{},
	DstType: copier.String,
	Fn: func(src interface{}) (interface{}, error) {
		return src.(primitive.ObjectID).Hex(), nil
	},
}}}

type Store interface {
	GetCursor() any
	LoadCursor(ctx context.Context, lastToken string, backward bool) error
	StoreCursor(ctx context.Context, lastToken *string, first, last any) (*string, error)
}

type RawStore struct {
	cursor     any
	cursorType reflect.Type
}

func NewRawStore(cursor any) *RawStore {
	t := reflect.TypeOf(cursor)
	for t.Kind() == reflect.Interface || t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return &RawStore{
		cursor:     cursor,
		cursorType: t,
	}
}

// GetCursor 获取游标
func (s *RawStore) GetCursor() any {
	return s.cursor
}

// LoadCursor 加载游标
func (s *RawStore) LoadCursor(_ context.Context, lastToken string, backward bool) error {
	cursors := reflect.New(reflect.ArrayOf(2, reflect.PointerTo(s.cursorType)))
	err := json.Unmarshal([]byte(lastToken), cursors.Interface())
	if backward {
		s.cursor = cursors.Elem().Index(0).Interface()
	} else {
		s.cursor = cursors.Elem().Index(1).Interface()
	}
	if err != nil {
		return err
	}
	return nil
}

// StoreCursor 生成游标
func (s *RawStore) StoreCursor(_ context.Context, lastToken *string, first, last any) (*string, error) {
	front := reflect.New(s.cursorType).Interface()
	err := copier.CopyWithOption(front, first, *copierOpt)
	if err != nil {
		return nil, err
	}
	back := reflect.New(s.cursorType).Interface()
	err = copier.CopyWithOption(back, last, *copierOpt)
	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal([2]any{front, back})
	if err != nil {
		return nil, err
	}
	if lastToken == nil {
		lastToken = new(string)
	}
	*lastToken = string(bytes)
	return lastToken, nil
}
