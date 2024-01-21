package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	ordermapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/order"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type IOrderService interface {
	GetOrder(ctx context.Context, req *gencontent.GetOrderReq) (resp *gencontent.GetOrderResp, err error)
	GetOrders(ctx context.Context, req *gencontent.GetOrdersReq) (resp *gencontent.GetOrdersResp, err error)
	CreateOrder(ctx context.Context, req *gencontent.CreateOrderReq) (resp *gencontent.CreateOrderResp, err error)
	UpdateOrder(ctx context.Context, req *gencontent.UpdateOrderReq) (resp *gencontent.UpdateOrderResp, err error)
	DeleteOrder(ctx context.Context, req *gencontent.DeleteOrderReq) (resp *gencontent.DeleteOrderResp, err error)
}

var OrderSet = wire.NewSet(
	wire.Struct(new(OrderService), "*"),
	wire.Bind(new(IOrderService), new(*OrderService)),
)

type OrderService struct {
	Config           *config.Config
	OrderMongoMapper ordermapper.IOrderMongoMapper
	OrderEsMapper    ordermapper.IEsMapper
	Redis            *redis.Redis
}

func (s *OrderService) GetOrder(ctx context.Context, req *gencontent.GetOrderReq) (resp *gencontent.GetOrderResp, err error) {
	resp = new(gencontent.GetOrderResp)
	order, err := s.OrderMongoMapper.FindOne(ctx, convertor.OrderFilterOptionsToFilterOptions(req.OrderFilterOptions))
	if err != nil {
		return resp, err
	}
	resp.Order = convertor.OrderMapperToOrder(order)
	return resp, nil
}

func (s *OrderService) GetOrders(ctx context.Context, req *gencontent.GetOrdersReq) (resp *gencontent.GetOrdersResp, err error) {
	resp = new(gencontent.GetOrdersResp)
	var (
		total  int64
		orders []*ordermapper.Order
	)
	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	filter := convertor.OrderFilterOptionsToFilterOptions(req.OrderFilterOptions)
	if req.SearchOptions != nil {
		switch o := req.SearchOptions.Type.(type) {
		case *gencontent.SearchOptions_AllFieldsKey:
			orders, total, err = s.OrderEsMapper.Search(ctx, convertor.ConvertOrderAllFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		case *gencontent.SearchOptions_MultiFieldsKey:
			orders, total, err = s.OrderEsMapper.Search(ctx, convertor.ConvertOrderMultiFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		}
	} else {
		orders, total, err = s.OrderMongoMapper.FindManyAndCount(ctx, convertor.OrderFilterOptionsToFilterOptions(req.OrderFilterOptions),
			p, mongop.IdCursorType)
	}
	if err != nil {
		return resp, err
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Orders = lo.Map[*ordermapper.Order, *gencontent.Order](orders, func(item *ordermapper.Order, _ int) *gencontent.Order {
		return convertor.OrderMapperToOrder(item)
	})
	return resp, nil
}

func (s *OrderService) CreateOrder(ctx context.Context, req *gencontent.CreateOrderReq) (resp *gencontent.CreateOrderResp, err error) {
	resp = new(gencontent.CreateOrderResp)
	if err = s.OrderMongoMapper.Insert(ctx, convertor.OrderToOrderMapper(req.Order)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *OrderService) UpdateOrder(ctx context.Context, req *gencontent.UpdateOrderReq) (resp *gencontent.UpdateOrderResp, err error) {
	resp = new(gencontent.UpdateOrderResp)
	if err = s.OrderMongoMapper.Update(ctx, convertor.OrderToOrderMapper(req.Order)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *OrderService) DeleteOrder(ctx context.Context, req *gencontent.DeleteOrderReq) (resp *gencontent.DeleteOrderResp, err error) {
	resp = new(gencontent.DeleteOrderResp)
	if err = s.OrderMongoMapper.Delete(ctx, req.OrderId); err != nil {
		return resp, err
	}
	return resp, nil
}
