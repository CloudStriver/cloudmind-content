package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	productmapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/product"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type IProductService interface {
	GetProduct(ctx context.Context, req *gencontent.GetProductReq) (resp *gencontent.GetProductResp, err error)
	GetProducts(ctx context.Context, req *gencontent.GetProductsReq) (resp *gencontent.GetProductsResp, err error)
	CreateProduct(ctx context.Context, req *gencontent.CreateProductReq) (resp *gencontent.CreateProductResp, err error)
	UpdateProduct(ctx context.Context, req *gencontent.UpdateProductReq) (resp *gencontent.UpdateProductResp, err error)
	DeleteProduct(ctx context.Context, req *gencontent.DeleteProductReq) (resp *gencontent.DeleteProductResp, err error)
}

type ProductService struct {
	Config             *config.Config
	ProductMongoMapper productmapper.IProductMongoMapper
	ProductEsMapper    productmapper.IEsMapper
	Redis              *redis.Redis
}

func (s *ProductService) GetProduct(ctx context.Context, req *gencontent.GetProductReq) (resp *gencontent.GetProductResp, err error) {
	resp = new(gencontent.GetProductResp)
	product, err := s.ProductMongoMapper.FindOne(ctx, convertor.ProductFilterOptionsToFilterOptions(req.ProductFilterOptions))
	if err != nil {
		return resp, err
	}
	resp.Product = convertor.ProductMapperToProduct(product)
	return resp, nil
}

func (s *ProductService) GetProducts(ctx context.Context, req *gencontent.GetProductsReq) (resp *gencontent.GetProductsResp, err error) {
	resp = new(gencontent.GetProductsResp)
	var (
		total    int64
		products []*productmapper.Product
	)
	p := pconvertor.PaginationOptionsToModelPaginationOptions(req.PaginationOptions)
	filter := convertor.ProductFilterOptionsToFilterOptions(req.ProductFilterOptions)
	if req.SearchOptions != nil {
		switch o := req.SearchOptions.Type.(type) {
		case *gencontent.SearchOptions_AllFieldsKey:
			products, total, err = s.ProductEsMapper.Search(ctx, convertor.ConvertProductAllFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		case *gencontent.SearchOptions_MultiFieldsKey:
			products, total, err = s.ProductEsMapper.Search(ctx, convertor.ConvertProductMultiFieldsSearchQuery(o),
				filter, p, esp.ScoreCursorType)
		}
	} else {
		products, total, err = s.ProductMongoMapper.FindManyAndCount(ctx, convertor.ProductFilterOptionsToFilterOptions(req.ProductFilterOptions),
			p, mongop.IdCursorType)
	}
	if err != nil {
		return resp, err
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Products = lo.Map[*productmapper.Product, *gencontent.Product](products, func(item *productmapper.Product, _ int) *gencontent.Product {
		return convertor.ProductMapperToProduct(item)
	})
	return resp, nil
}

func (s *ProductService) CreateProduct(ctx context.Context, req *gencontent.CreateProductReq) (resp *gencontent.CreateProductResp, err error) {
	resp = new(gencontent.CreateProductResp)
	if err = s.ProductMongoMapper.Insert(ctx, convertor.ProductToProductMapper(req.Product)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *ProductService) UpdateProduct(ctx context.Context, req *gencontent.UpdateProductReq) (resp *gencontent.UpdateProductResp, err error) {
	resp = new(gencontent.UpdateProductResp)
	if err = s.ProductMongoMapper.Update(ctx, convertor.ProductToProductMapper(req.Product)); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *ProductService) DeleteProduct(ctx context.Context, req *gencontent.DeleteProductReq) (resp *gencontent.DeleteProductResp, err error) {
	resp = new(gencontent.DeleteProductResp)
	if err = s.ProductMongoMapper.Delete(ctx, req.ProductId); err != nil {
		return resp, err
	}
	return resp, nil
}

var ProductSet = wire.NewSet(
	wire.Struct(new(ProductService), "*"),
	wire.Bind(new(IProductService), new(*ProductService)),
)
