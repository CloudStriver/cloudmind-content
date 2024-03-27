package mongop

import (
	"github.com/CloudStriver/go-pkg/utils/pagination"
)

type (
	MongoPaginator struct {
		opts  *pagination.PaginationOptions
		store pagination.Store
	}
)

func NewMongoPaginator(store pagination.Store, opts *pagination.PaginationOptions) *MongoPaginator {
	opts.EnsureSafe()
	return &MongoPaginator{
		store: store,
		opts:  opts,
	}
}

// MakeSortOptions 生成ID分页查询选项，并将filter在原地更新
//func (p *MongoPaginator) MakeSortOptions(ctx context.Context, filter bson.M) (bson.M, error) {
//	if p.opts.LastToken != nil {
//		err := p.store.LoadCursor(ctx, *p.opts.LastToken, *p.opts.Backward)
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	cursor := p.store.GetCursor()
//	sort, err := cursor.(MongoCursor).MakeSortOptions(filter, *p.opts.Backward)
//	if err != nil {
//		return nil, err
//	}
//	return sort, nil
//}
//
//func (p *MongoPaginator) StoreCursor(ctx context.Context, first, last any) error {
//	token, err := p.store.StoreCursor(ctx, p.opts.LastToken, first, last)
//	p.opts.LastToken = token
//	return err
//}
