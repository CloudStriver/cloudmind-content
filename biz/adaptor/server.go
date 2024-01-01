package adaptor

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/application/service"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
)

type ContentServerImpl struct {
	*config.Config
	FileService    service.IFileService
	PostService    service.IPostService
	CommentService service.ICommentService
	LabelService   service.ILabelService
	UserService    service.UserService
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

func (s *ContentServerImpl) GetUserDetail(ctx context.Context, req *content.GetUserDetailReq) (resp *content.GetUserDetailResp, err error) {
	return s.UserService.GetUserDetail(ctx, req)
}

func (s *ContentServerImpl) GetFolderSize(ctx context.Context, req *content.GetFolderSizeReq) (*content.GetFolderSizeResp, error) {
	resp := new(content.GetFolderSizeResp)
	res, err := s.FileService.GetFile(ctx, &content.GetFileReq{FilterOptions: req.FilterOptions, IsGetSize: true})
	if err != nil {
		return resp, err
	}
	resp.SpaceSize = res.File.SpaceSize
	return resp, nil
}

func (s *ContentServerImpl) GetFileIsExist(ctx context.Context, req *content.GetFileIsExistReq) (res *content.GetFileIsExistResp, err error) {
	return s.FileService.GetFileIsExist(ctx, req)
}

func (s *ContentServerImpl) GetFile(ctx context.Context, req *content.GetFileReq) (res *content.GetFileResp, err error) {
	return s.FileService.GetFile(ctx, req)
}

func (s *ContentServerImpl) GetFileList(ctx context.Context, req *content.GetFileListReq) (res *content.GetFileListResp, err error) {
	return s.FileService.GetFileList(ctx, req)
}

func (s *ContentServerImpl) GetFileCount(ctx context.Context, req *content.GetFileCountReq) (res *content.GetFileCountResp, err error) {
	return s.FileService.GetFileCount(ctx, req)
}

func (s *ContentServerImpl) UpdateFile(ctx context.Context, req *content.UpdateFileReq) (res *content.UpdateFileResp, err error) {
	return s.FileService.UpdateFile(ctx, req)
}

func (s *ContentServerImpl) MoveFile(ctx context.Context, req *content.MoveFileReq) (res *content.MoveFileResp, err error) {
	return s.FileService.MoveFile(ctx, req)
}

func (s *ContentServerImpl) GetFileBySharingCode(ctx context.Context, req *content.GetFileBySharingCodeReq) (res *content.GetFileBySharingCodeResp, err error) {
	return s.FileService.GetFileBySharingCode(ctx, req)
}

func (s *ContentServerImpl) CreateFolder(ctx context.Context, req *content.CreateFolderReq) (*content.CreateFolderResp, error) {
	return s.FileService.CreateFolder(ctx, req)
}

//func (s *ContentServerImpl) UploadFile(ctx context.Context, req *content.UploadFileReq) (res *content.UploadFileResp, err error) {
//	return s.FileService.UploadFile(ctx, req)
//}

func (s *ContentServerImpl) AskUploadFile(ctx context.Context, req *content.AskUploadFileReq) (res *content.AskUploadFileResp, err error) {
	return s.FileService.AskUploadFile(ctx, req)
}

func (s *ContentServerImpl) AskUploadFileRollback(ctx context.Context, req *content.AskUploadFileReq) (res *content.AskUploadFileResp, err error) {
	return s.FileService.AskUploadFileRollback(ctx, req)
}

func (s *ContentServerImpl) DeleteFile(ctx context.Context, req *content.DeleteFileReq) (res *content.DeleteFileResp, err error) {
	return s.FileService.DeleteFile(ctx, req)
}

func (s *ContentServerImpl) DeleteShareFile(ctx context.Context, req *content.DeleteShareFileReq) (res *content.DeleteShareFileResp, err error) {
	return s.FileService.DeleteShareFile(ctx, req)
}

func (s *ContentServerImpl) DeleteExpiredFiles(ctx context.Context, req *content.DeleteExpiredFilesReq) (res *content.DeleteExpiredFilesResp, err error) {
	return s.FileService.DeleteExpiredFiles(ctx, req)
}

func (s *ContentServerImpl) DeleteExpiredShareCodes(ctx context.Context, req *content.DeleteExpiredShareCodesReq) (res *content.DeleteExpiredShareCodesResp, err error) {
	return s.FileService.DeleteExpiredShareCodes(ctx, req)
}

func (s *ContentServerImpl) GetLabel(ctx context.Context, req *content.GetLabelReq) (res *content.GetLabelResp, err error) {
	return s.LabelService.GetLabel(ctx, req)
}

func (s *ContentServerImpl) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (res *content.CreateLabelResp, err error) {
	return s.LabelService.CreateLabel(ctx, req)
}

func (s *ContentServerImpl) UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (res *content.UpdateLabelResp, err error) {
	return s.LabelService.UpdateLabel(ctx, req)
}

func (s *ContentServerImpl) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (res *content.DeleteLabelResp, err error) {
	return s.LabelService.DeleteLabel(ctx, req)
}
