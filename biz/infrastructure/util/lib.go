package util

import (
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/basic"
	"github.com/bytedance/sonic"
	"github.com/xh-polaris/gopkg/pagination"
	"github.com/xh-polaris/gopkg/util/log"
	"sync"

	"github.com/bytedance/gopkg/util/gopool"
)

func JSONF(v any) string {
	data, err := sonic.Marshal(v)
	if err != nil {
		log.Error("JSONF fail, v=%v, err=%v", v, err)
	}
	return string(data)
}

func ParsePagination(opts *basic.PaginationOptions) (p *pagination.PaginationOptions) {
	if opts == nil {
		p = &pagination.PaginationOptions{}
	} else {
		p = &pagination.PaginationOptions{
			Limit:     opts.Limit,
			Offset:    opts.Offset,
			Backward:  opts.Backward,
			LastToken: opts.LastToken,
		}
	}
	return
}

func ParallelRun(fns []func()) {
	wg := sync.WaitGroup{}
	wg.Add(len(fns))
	for _, fn := range fns {
		fn := fn
		gopool.Go(func() {
			defer wg.Done()
			fn()
		})
	}
	wg.Wait()
}
