package service

import (
	"context"
	errorx "errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/util/convertor"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type IFileService interface {
	GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (res *gencontent.GetFileIsExistResp, err error)
	GetFile(ctx context.Context, req *gencontent.GetFileReq) (res *gencontent.GetFileResp, err error)
	GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (res *gencontent.GetFileListResp, err error)
	GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (res *gencontent.GetFileCountResp, err error)
	GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (res *gencontent.GetFileBySharingCodeResp, err error)
	GetFolderSize(ctx context.Context, path string) (res *gencontent.GetFolderSizeResp, err error)
	CreateFolder(ctx context.Context, req *gencontent.CreateFolderReq) (res *gencontent.CreateFolderResp, err error)
	UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (res *gencontent.UpdateFileResp, err error)
	MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (res *gencontent.MoveFileResp, err error)
	AskUploadFile(ctx context.Context, req *gencontent.AskUploadFileReq) (res *gencontent.AskUploadFileResp, err error)
	AskUploadFileRollback(ctx context.Context, req *gencontent.AskUploadFileReq) (res *gencontent.AskUploadFileResp, err error)
	DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (res *gencontent.DeleteFileResp, err error)
	DeleteShareFile(ctx context.Context, req *gencontent.DeleteShareFileReq) (res *gencontent.DeleteShareFileResp, err error)
	DeleteExpiredFiles(ctx context.Context, req *gencontent.DeleteExpiredFilesReq) (res *gencontent.DeleteExpiredFilesResp, err error)
	DeleteExpiredShareCodes(ctx context.Context, req *gencontent.DeleteExpiredShareCodesReq) (res *gencontent.DeleteExpiredShareCodesResp, err error)
}

type FileService struct {
	FileMongoMapper filemapper.IMongoMapper
}

var FileSet = wire.NewSet(
	wire.Struct(new(FileService), "*"),
	wire.Bind(new(IFileService), new(*FileService)),
)

func (s *FileService) GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (*gencontent.GetFileIsExistResp, error) {
	resp := new(gencontent.GetFileIsExistResp)
	File, err := s.FileMongoMapper.FindByMd5(ctx, req.Md5)
	if err != nil {
		resp.Ok = false
		log.CtxError(ctx, "GetFileIsExist", err)
		return resp, err
	} else if File == nil {
		resp.Ok = false
		return resp, nil
	} else {
		resp.Ok = true
		return resp, nil
	}
}

func (s *FileService) GetFile(ctx context.Context, req *gencontent.GetFileReq) (*gencontent.GetFileResp, error) {
	resp := new(gencontent.GetFileResp)

	filter := convertor.ParseFileFilter(req.FilterOptions)
	File, err := s.FileMongoMapper.FindOne(ctx, filter)
	if errorx.Is(err, consts.ErrNotFound) {

		return resp, consts.ErrNoSuchFile
	} else if err != nil {
		log.CtxError(ctx, "GetFile", err)
		return resp, err
	}

	resp.File = convertor.ConvertFile(File)
	if req.IsGetSize && File.Type == int32(gencontent.Type_Type_folder) {
		res, err := s.GetFolderSize(ctx, File.Path)
		if err != nil {
			log.CtxError(ctx, "GetFile", err)
			return resp, consts.ErrCalFileSize
		}
		resp.File.SpaceSize = res.SpaceSize
	}

	return resp, nil
}

func (s *FileService) GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (*gencontent.GetFileListResp, error) {
	resp := new(gencontent.GetFileListResp)
	var files []*filemapper.File
	var total int64
	var err error

	filter := convertor.ParseFileFilter(req.FilterOptions)
	p := convertor.ParsePagination(req.PaginationOptions)

	if req.SearchOptions == nil {
		files, total, err = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType)
		if err != nil {
			log.CtxError(ctx, "GetFileList", err)
			return nil, err
		}
	} else {

	}

	resp.Total = total
	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Files = make([]*gencontent.FileInfo, 0, len(files))
	for _, file := range files {
		resp.Files = append(resp.Files, convertor.ConvertFile(file))
	}

	return resp, nil
}

func (s *FileService) GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (res *gencontent.GetFileCountResp, err error) {
	resp := new(gencontent.GetFileCountResp)
	filter := convertor.ParseFileFilter(req.FilterOptions)
	total, err := s.FileMongoMapper.Count(ctx, filter)
	if err != nil {
		log.CtxError(ctx, "GetFileCount", err)
		return resp, err
	}
	resp.Count = total
	return resp, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (res *gencontent.GetFileBySharingCodeResp, err error) {
	return nil, nil
}

func (s *FileService) GetFolderSize(ctx context.Context, path string) (*gencontent.GetFolderSizeResp, error) {
	resp := new(gencontent.GetFolderSizeResp)
	size, err := s.FileMongoMapper.FindFolderSize(ctx, path)
	if err != nil {
		log.CtxError(ctx, "GetFolderSize", err)
		return resp, err
	}

	resp.SpaceSize = size
	return resp, nil
}

func (s *FileService) CreateFolder(ctx context.Context, req *gencontent.CreateFolderReq) (*gencontent.CreateFolderResp, error) {
	resp := new(gencontent.CreateFolderResp)

	var path string
	if req.File.UserId == req.File.FatherId {
		path = req.File.UserId
	} else {
		filter := convertor.ParseFileFilter(&gencontent.FileFilterOptions{
			OnlyUserId:   &req.File.UserId,
			OnlyFileId:   &req.File.FatherId,
			IsDel:        int32(gencontent.IsDel_Is_no),
			DocumentType: int32(gencontent.DocumentType_DocumentType_personal),
		})
		fatherFile, err := s.FileMongoMapper.FindOne(ctx, filter)
		if err != nil {
			log.CtxError(ctx, "CreateFolder", err)
			return resp, err
		}
		path = fatherFile.Path
	}

	data := &filemapper.File{
		ID:       primitive.NewObjectID(),
		UserId:   req.File.UserId,
		Name:     req.File.Name,
		Type:     int32(gencontent.Type_Type_folder),
		FatherId: req.File.FatherId,
		Size:     lo.ToPtr(int64(0)),
		IsDel:    int32(gencontent.IsDel_Is_no),
		CreateAt: time.Now(),
		UpdateAt: time.Now(),
	}
	data.Path = path + "/" + data.ID.Hex()
	id, err := s.FileMongoMapper.Insert(ctx, data)
	if err != nil {
		log.CtxError(ctx, "CreateFolder", err)
		return resp, err
	}

	resp.FileId = id
	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (*gencontent.UpdateFileResp, error) {
	resp := new(gencontent.UpdateFileResp)

	oid, err := primitive.ObjectIDFromHex(req.File.FileId)
	if err != nil {
		log.CtxError(ctx, "UpdateFile", err)
		return resp, consts.ErrInvalidId
	}

	_, err = s.FileMongoMapper.Update(ctx, &filemapper.File{
		ID:          oid,
		UserId:      req.File.UserId,
		Name:        req.File.Name,
		Type:        int32(req.File.Type),
		Path:        req.File.Path,
		FatherId:    req.File.FatherId,
		Size:        req.File.SpaceSize,
		FileMd5:     req.File.Md5,
		IsDel:       req.File.IsDel,
		Tag:         req.File.Tag,
		Description: req.File.Description,
		UpdateAt:    time.Now(),
	})
	if err != nil {
		log.CtxError(ctx, "UpdateFile", err)
		return resp, err
	}

	return resp, nil
}

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (*gencontent.MoveFileResp, error) {
	resp := new(gencontent.MoveFileResp)
	var file *filemapper.File
	var objectfile *filemapper.File
	var err, err1, err2 error

	err = mr.Finish(func() error {
		file, err1 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId:   req.FilterOptions.OnlyUserId,
			OnlyFileId:   req.FilterOptions.OnlyFileId,
			IsDel:        int32(gencontent.IsDel_Is_no),
			DocumentType: int32(gencontent.DocumentType_DocumentType_personal),
		})
		if err1 != nil {
			return err1
		}
		return nil
	}, func() error {
		objectfile, err2 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId:   req.FilterOptions.OnlyUserId,
			OnlyFileId:   req.FilterOptions.OnlyFatherId,
			IsDel:        int32(gencontent.IsDel_Is_no),
			DocumentType: int32(gencontent.DocumentType_DocumentType_personal),
		})
		if err2 != nil {
			return err2
		}
		return nil
	})

	if err != nil {
		return resp, err
	}

	if objectfile.Type != int32(gencontent.Type_Type_folder) {
		return resp, consts.ErrFileIsNotDir
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if file.Type == int32(gencontent.Type_Type_folder) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter)
			if err != nil {
				return err
			}

			for _, v := range data {
				_, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:   v.ID,
					Path: objectfile.Path + v.Path[len(file.Path):],
				})
				if err != nil {
					if rbErr := sessionContext.AbortTransaction(ctx); rbErr != nil {
						log.CtxError(ctx, "MoveFile", rbErr)
						return rbErr
					}
				}
			}
		}

		file.Path = objectfile.Path + "/" + file.ID.Hex()
		file.FatherId = *req.FilterOptions.OnlyFatherId
		file.UpdateAt = time.Now()
		_, err = s.FileMongoMapper.Update(sessionContext, file)
		if err != nil {
			if rbErr := sessionContext.AbortTransaction(ctx); rbErr != nil {
				log.CtxError(ctx, "MoveFile", rbErr)
				return rbErr
			}
		}

		if err = sessionContext.CommitTransaction(ctx); err != nil {
			log.CtxError(ctx, "MoveFile", err)
			return err
		}

		return nil
	})

	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) AskUploadFile(ctx context.Context, req *gencontent.AskUploadFileReq) (res *gencontent.AskUploadFileResp, err error) {
	return nil, nil
}

func (s *FileService) AskUploadFileRollback(ctx context.Context, req *gencontent.AskUploadFileReq) (res *gencontent.AskUploadFileResp, err error) {
	return nil, nil
}

func (s *FileService) DeleteExpiredFiles(ctx context.Context, req *gencontent.DeleteExpiredFilesReq) (res *gencontent.DeleteExpiredFilesResp, err error) {
	return nil, nil
}

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (*gencontent.DeleteFileResp, error) {
	resp := new(gencontent.DeleteFileResp)

	oid, err := primitive.ObjectIDFromHex(req.FileId)
	if err != nil {
		log.CtxError(ctx, "DeleteFile", err)
		return resp, err
	}

	_, err = s.FileMongoMapper.Update(ctx, &filemapper.File{
		ID:    oid,
		IsDel: int32(gencontent.IsDel_Is_soft),
		Tag:   []string{},
	})
	if err != nil {
		log.CtxError(ctx, "DeleteFile", err)
		return resp, err
	}

	return resp, nil
}

func (s *FileService) DeleteShareFile(ctx context.Context, req *gencontent.DeleteShareFileReq) (*gencontent.DeleteShareFileResp, error) {
	resp := new(gencontent.DeleteShareFileResp)
	return resp, nil
}

func (s *FileService) DeleteExpiredShareCodes(ctx context.Context, req *gencontent.DeleteExpiredShareCodesReq) (res *gencontent.DeleteExpiredShareCodesResp, err error) {
	return nil, nil
}
