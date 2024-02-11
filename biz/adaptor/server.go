package adaptor

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/application/service"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
)

type ContentServerImpl struct {
	*config.Config
	FileService      service.IFileService
	PostService      service.IPostService
	ZoneService      service.IZoneService
	UserService      service.IUserService
	ProductService   service.IProductService
	CouponService    service.ICouponService
	OrderService     service.IOrderService
	RecommendService service.IRecommendService
}

func (s *ContentServerImpl) GetRecycleBinFiles(ctx context.Context, req *content.GetRecycleBinFilesReq) (res *content.GetRecycleBinFilesResp, err error) {
	return s.FileService.GetRecycleBinFiles(ctx, req)
}

func (s *ContentServerImpl) CreateFeedBacks(ctx context.Context, req *content.CreateFeedBacksReq) (res *content.CreateFeedBacksResp, err error) {
	return s.RecommendService.CreateFeedBacks(ctx, req)
}

func (s *ContentServerImpl) GetLatestRecommend(ctx context.Context, req *content.GetLatestRecommendReq) (res *content.GetLatestRecommendResp, err error) {
	return s.RecommendService.GetLatestRecommend(ctx, req)
}

func (s *ContentServerImpl) CreateItems(ctx context.Context, req *content.CreateItemsReq) (res *content.CreateItemsResp, err error) {
	return s.RecommendService.CreateItems(ctx, req)
}

func (s *ContentServerImpl) UpdateItem(ctx context.Context, req *content.UpdateItemReq) (res *content.UpdateItemResp, err error) {
	return s.RecommendService.UpdateItem(ctx, req)
}

func (s *ContentServerImpl) DeleteItem(ctx context.Context, req *content.DeleteItemReq) (res *content.DeleteItemResp, err error) {
	return s.RecommendService.DeleteItem(ctx, req)
}

func (s *ContentServerImpl) GetRecommendByUser(ctx context.Context, req *content.GetRecommendByUserReq) (res *content.GetRecommendByUserResp, err error) {
	return s.RecommendService.GetRecommendByUser(ctx, req)
}

func (s *ContentServerImpl) GetRecommendByItem(ctx context.Context, req *content.GetRecommendByItemReq) (res *content.GetRecommendByItemResp, err error) {
	return s.RecommendService.GetRecommendByItem(ctx, req)
}

func (s *ContentServerImpl) GetPopularRecommend(ctx context.Context, req *content.GetPopularRecommendReq) (res *content.GetPopularRecommendResp, err error) {
	return s.RecommendService.GetPopularRecommend(ctx, req)

}

func (s *ContentServerImpl) GetFilesByIds(ctx context.Context, req *content.GetFilesByIdsReq) (res *content.GetFilesByIdsResp, err error) {
	return s.FileService.GetFilesByIds(ctx, req)
}

func (s *ContentServerImpl) CompletelyRemoveFile(ctx context.Context, req *content.CompletelyRemoveFileReq) (res *content.CompletelyRemoveFileResp, err error) {
	return s.FileService.CompletelyRemoveFile(ctx, req)
}

func (s *ContentServerImpl) CreateProduct(ctx context.Context, req *content.CreateProductReq) (res *content.CreateProductResp, err error) {
	return s.ProductService.CreateProduct(ctx, req)
}

func (s *ContentServerImpl) DeleteProduct(ctx context.Context, req *content.DeleteProductReq) (res *content.DeleteProductResp, err error) {
	return s.ProductService.DeleteProduct(ctx, req)
}

func (s *ContentServerImpl) UpdateProduct(ctx context.Context, req *content.UpdateProductReq) (res *content.UpdateProductResp, err error) {
	return s.ProductService.UpdateProduct(ctx, req)
}

func (s *ContentServerImpl) GetProduct(ctx context.Context, req *content.GetProductReq) (res *content.GetProductResp, err error) {
	return s.ProductService.GetProduct(ctx, req)
}

func (s *ContentServerImpl) GetProducts(ctx context.Context, req *content.GetProductsReq) (res *content.GetProductsResp, err error) {
	return s.ProductService.GetProducts(ctx, req)
}

func (s *ContentServerImpl) CreateCoupon(ctx context.Context, req *content.CreateCouponReq) (res *content.CreateCouponResp, err error) {
	return s.CouponService.CreateCoupon(ctx, req)
}

func (s *ContentServerImpl) DeleteCoupon(ctx context.Context, req *content.DeleteCouponReq) (res *content.DeleteCouponResp, err error) {
	return s.CouponService.DeleteCoupon(ctx, req)
}

func (s *ContentServerImpl) UpdateCoupon(ctx context.Context, req *content.UpdateCouponReq) (res *content.UpdateCouponResp, err error) {
	return s.CouponService.UpdateCoupon(ctx, req)
}

func (s *ContentServerImpl) GetCoupon(ctx context.Context, req *content.GetCouponReq) (res *content.GetCouponResp, err error) {
	return s.CouponService.GetCoupon(ctx, req)
}

func (s *ContentServerImpl) GetCoupons(ctx context.Context, req *content.GetCouponsReq) (res *content.GetCouponsResp, err error) {
	return s.CouponService.GetCoupons(ctx, req)
}

func (s *ContentServerImpl) CreateOrder(ctx context.Context, req *content.CreateOrderReq) (res *content.CreateOrderResp, err error) {
	return s.OrderService.CreateOrder(ctx, req)
}

func (s *ContentServerImpl) DeleteOrder(ctx context.Context, req *content.DeleteOrderReq) (res *content.DeleteOrderResp, err error) {
	return s.OrderService.DeleteOrder(ctx, req)
}

func (s *ContentServerImpl) UpdateOrder(ctx context.Context, req *content.UpdateOrderReq) (res *content.UpdateOrderResp, err error) {
	return s.OrderService.UpdateOrder(ctx, req)
}

func (s *ContentServerImpl) GetOrder(ctx context.Context, req *content.GetOrderReq) (res *content.GetOrderResp, err error) {
	return s.OrderService.GetOrder(ctx, req)
}

func (s *ContentServerImpl) GetOrders(ctx context.Context, req *content.GetOrdersReq) (res *content.GetOrdersResp, err error) {
	return s.OrderService.GetOrders(ctx, req)
}

func (s *ContentServerImpl) CreatePost(ctx context.Context, req *content.CreatePostReq) (res *content.CreatePostResp, err error) {
	return s.PostService.CreatePost(ctx, req)
}

func (s *ContentServerImpl) DeletePost(ctx context.Context, req *content.DeletePostReq) (res *content.DeletePostResp, err error) {
	return s.PostService.DeletePost(ctx, req)
}

func (s *ContentServerImpl) UpdatePost(ctx context.Context, req *content.UpdatePostReq) (res *content.UpdatePostResp, err error) {
	return s.PostService.UpdatePost(ctx, req)
}

func (s *ContentServerImpl) GetPost(ctx context.Context, req *content.GetPostReq) (res *content.GetPostResp, err error) {
	return s.PostService.GetPost(ctx, req)
}

func (s *ContentServerImpl) GetPosts(ctx context.Context, req *content.GetPostsReq) (res *content.GetPostsResp, err error) {
	return s.PostService.GetPosts(ctx, req)
}

func (s *ContentServerImpl) DeleteUser(ctx context.Context, req *content.DeleteUserReq) (resp *content.DeleteUserResp, err error) {
	return s.UserService.DeleteUser(ctx, req)
}

func (s *ContentServerImpl) UpdateUser(ctx context.Context, req *content.UpdateUserReq) (resp *content.UpdateUserResp, err error) {
	return s.UserService.UpdateUser(ctx, req)
}

func (s *ContentServerImpl) GetUser(ctx context.Context, req *content.GetUserReq) (resp *content.GetUserResp, err error) {
	return s.UserService.GetUser(ctx, req)
}

func (s *ContentServerImpl) SearchUser(ctx context.Context, req *content.SearchUserReq) (resp *content.SearchUserResp, err error) {
	return s.UserService.SearchUser(ctx, req)
}

func (s *ContentServerImpl) CreateUser(ctx context.Context, req *content.CreateUserReq) (resp *content.CreateUserResp, err error) {
	return s.UserService.CreateUser(ctx, req)
}

func (s *ContentServerImpl) GetFileIsExist(ctx context.Context, req *content.GetFileIsExistReq) (*content.GetFileIsExistResp, error) {
	return s.FileService.GetFileIsExist(ctx, req)
}

func (s *ContentServerImpl) GetFile(ctx context.Context, req *content.GetFileReq) (*content.GetFileResp, error) {
	return s.FileService.GetFile(ctx, req)
}

func (s *ContentServerImpl) GetFileList(ctx context.Context, req *content.GetFileListReq) (*content.GetFileListResp, error) {
	return s.FileService.GetFileList(ctx, req)
}

func (s *ContentServerImpl) DeleteFile(ctx context.Context, req *content.DeleteFileReq) (res *content.DeleteFileResp, err error) {
	return s.FileService.DeleteFile(ctx, req)
}

func (s *ContentServerImpl) UpdateFile(ctx context.Context, req *content.UpdateFileReq) (*content.UpdateFileResp, error) {
	return s.FileService.UpdateFile(ctx, req)
}

func (s *ContentServerImpl) MoveFile(ctx context.Context, req *content.MoveFileReq) (*content.MoveFileResp, error) {
	return s.FileService.MoveFile(ctx, req)
}

func (s *ContentServerImpl) GetFileBySharingCode(ctx context.Context, req *content.GetFileBySharingCodeReq) (*content.GetFileBySharingCodeResp, error) {
	return s.FileService.GetFileBySharingCode(ctx, req)
}

func (s *ContentServerImpl) CreateFile(ctx context.Context, req *content.CreateFileReq) (*content.CreateFileResp, error) {
	return s.FileService.CreateFile(ctx, req)
}

func (s *ContentServerImpl) GetZone(ctx context.Context, req *content.GetZoneReq) (res *content.GetZoneResp, err error) {
	return s.ZoneService.GetZone(ctx, req)
}

func (s *ContentServerImpl) GetZones(ctx context.Context, req *content.GetZonesReq) (res *content.GetZonesResp, err error) {
	return s.ZoneService.GetZones(ctx, req)
}

func (s *ContentServerImpl) CreateZone(ctx context.Context, req *content.CreateZoneReq) (*content.CreateZoneResp, error) {
	return s.ZoneService.CreateZone(ctx, req)
}

func (s *ContentServerImpl) UpdateZone(ctx context.Context, req *content.UpdateZoneReq) (*content.UpdateZoneResp, error) {
	return s.ZoneService.UpdateZone(ctx, req)
}

func (s *ContentServerImpl) DeleteZone(ctx context.Context, req *content.DeleteZoneReq) (*content.DeleteZoneResp, error) {
	return s.ZoneService.DeleteZone(ctx, req)
}

func (s *ContentServerImpl) GetShareList(ctx context.Context, req *content.GetShareListReq) (*content.GetShareListResp, error) {
	return s.FileService.GetShareList(ctx, req)
}

func (s *ContentServerImpl) CreateShareCode(ctx context.Context, req *content.CreateShareCodeReq) (*content.CreateShareCodeResp, error) {
	return s.FileService.CreateShareCode(ctx, req)
}

func (s *ContentServerImpl) UpdateShareCode(ctx context.Context, req *content.UpdateShareCodeReq) (*content.UpdateShareCodeResp, error) {
	return s.FileService.UpdateShareCode(ctx, req)
}

func (s *ContentServerImpl) DeleteShareCode(ctx context.Context, req *content.DeleteShareCodeReq) (*content.DeleteShareCodeResp, error) {
	return s.FileService.DeleteShareCode(ctx, req)
}

func (s *ContentServerImpl) ParsingShareCode(ctx context.Context, req *content.ParsingShareCodeReq) (*content.ParsingShareCodeResp, error) {
	return s.FileService.ParsingShareCode(ctx, req)
}

func (s *ContentServerImpl) SaveFileToPrivateSpace(ctx context.Context, req *content.SaveFileToPrivateSpaceReq) (res *content.SaveFileToPrivateSpaceResp, err error) {
	return s.FileService.SaveFileToPrivateSpace(ctx, req)
}

func (s *ContentServerImpl) AddFileToPublicSpace(ctx context.Context, req *content.AddFileToPublicSpaceReq) (*content.AddFileToPublicSpaceResp, error) {
	return s.FileService.AddFileToPublicSpace(ctx, req)
}

func (s *ContentServerImpl) RecoverRecycleBinFile(ctx context.Context, req *content.RecoverRecycleBinFileReq) (*content.RecoverRecycleBinFileResp, error) {
	return s.FileService.RecoverRecycleBinFile(ctx, req)
}
