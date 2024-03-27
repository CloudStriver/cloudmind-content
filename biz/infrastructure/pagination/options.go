package pagination

var (
	defaultPageSize = int64(10)
)

type PaginationOptions struct {
	Limit     *int64
	Offset    *int64
	Backward  *bool
	LastToken *string
}

func (p *PaginationOptions) EnsureSafe() {
	if p.Backward == nil {
		p.Backward = new(bool)
	}
	if p.Limit == nil {
		p.Limit = &defaultPageSize
	}
	if p.Offset == nil {
		p.Offset = new(int64)
	}
}
