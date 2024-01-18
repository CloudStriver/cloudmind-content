package adaptor

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/application/service"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
)

type ContentServerImpl struct {
	*config.Config
	FileService  service.IFileService
	PostService  service.IPostService
	LabelService service.ILabelService
	UserService  service.UserService
}

func (s *ContentServerImpl) CreatePost(ctx context.Context, req *content.CreatePostReq) (res *content.CreatePostResp, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *ContentServerImpl) DeletePost(ctx context.Context, req *content.DeletePostReq) (res *content.DeletePostResp, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *ContentServerImpl) UpdatePost(ctx context.Context, req *content.UpdatePostReq) (res *content.UpdatePostResp, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *ContentServerImpl) GetPost(ctx context.Context, req *content.GetPostReq) (res *content.GetPostResp, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *ContentServerImpl) GetPosts(ctx context.Context, req *content.GetPostsReq) (res *content.GetPostsResp, err error) {
	//TODO implement me
	panic("implement me")
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

func (s *ContentServerImpl) GetFileCount(ctx context.Context, req *content.GetFileCountReq) (*content.GetFileCountResp, error) {
	return s.FileService.GetFileCount(ctx, req)
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

func (s *ContentServerImpl) CreateFolder(ctx context.Context, req *content.CreateFolderReq) (*content.CreateFolderResp, error) {
	return s.FileService.CreateFolder(ctx, req)
}

func (s *ContentServerImpl) GetLabel(ctx context.Context, req *content.GetLabelReq) (res *content.GetLabelResp, err error) {
	return s.LabelService.GetLabel(ctx, req)
}

func (s *ContentServerImpl) CreateLabel(ctx context.Context, req *content.CreateLabelReq) (*content.CreateLabelResp, error) {
	return s.LabelService.CreateLabel(ctx, req)
}

func (s *ContentServerImpl) UpdateLabel(ctx context.Context, req *content.UpdateLabelReq) (*content.UpdateLabelResp, error) {
	return s.LabelService.UpdateLabel(ctx, req)
}

func (s *ContentServerImpl) DeleteLabel(ctx context.Context, req *content.DeleteLabelReq) (*content.DeleteLabelResp, error) {
	return s.LabelService.DeleteLabel(ctx, req)
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
