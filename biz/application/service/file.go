package service

import (
	"context"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	sharefilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/util/convertor"
	"github.com/CloudStriver/go-pkg/utils/pagination/esp"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"sync"
	"time"
)

type IFileService interface {
	GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (*gencontent.GetFileIsExistResp, error)
	GetFile(ctx context.Context, req *gencontent.GetFileReq) (*gencontent.GetFileResp, error)
	GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (*gencontent.GetFileListResp, error)
	GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (*gencontent.GetFileCountResp, error)
	GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (*gencontent.GetFileBySharingCodeResp, error)
	GetFolderSize(ctx context.Context, path string) (*gencontent.GetFolderSizeResp, error)
	CreateFolder(ctx context.Context, req *gencontent.CreateFolderReq) (*gencontent.CreateFolderResp, error)
	UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (*gencontent.UpdateFileResp, error)
	MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (*gencontent.MoveFileResp, error)
	DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (*gencontent.DeleteFileResp, error)
	RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (*gencontent.RecoverRecycleBinFileResp, error)
	GetShareList(ctx context.Context, req *gencontent.GetShareListReq) (*gencontent.GetShareListResp, error)
	CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (*gencontent.CreateShareCodeResp, error)
	UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (*gencontent.UpdateShareCodeResp, error)
	DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (*gencontent.DeleteShareCodeResp, error)
	ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (*gencontent.ParsingShareCodeResp, error)
	SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (*gencontent.SaveFileToPrivateSpaceResp, error)
	AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (*gencontent.AddFileToPublicSpaceResp, error)
}

type FileService struct {
	FileMongoMapper      filemapper.IMongoMapper
	FileEsMapper         filemapper.IFileEsMapper
	ShareFileMongoMapper sharefilemapper.IMongoMapper
}

var FileSet = wire.NewSet(
	wire.Struct(new(FileService), "*"),
	wire.Bind(new(IFileService), new(*FileService)),
)

func (s *FileService) GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (*gencontent.GetFileIsExistResp, error) {
	resp := new(gencontent.GetFileIsExistResp)
	file, err := s.FileMongoMapper.FindByMd5(ctx, req.Md5)
	if err != nil {
		resp.Ok = false
		log.CtxError(ctx, "查询文件md5值是否存在: 发生异常[%v]\n", err)
		return resp, err
	} else if file == nil {
		resp.Ok = false
		return resp, nil
	} else {
		resp.Ok = true
		return resp, nil
	}
}

func (s *FileService) GetFile(ctx context.Context, req *gencontent.GetFileReq) (*gencontent.GetFileResp, error) {
	resp := new(gencontent.GetFileResp)
	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	file, err := s.FileMongoMapper.FindOne(ctx, filter)
	if err != nil {
		log.CtxError(ctx, "查询文件详细信息: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.File = convertor.FileMapperToFile(file)
	if req.IsGetSize && file.Type == int64(gencontent.Type_Type_folder) {
		res, err := s.GetFolderSize(ctx, file.Path)
		if err != nil {
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

	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	p := convertor.ParsePagination(req.PaginationOptions)
	if req.SearchOptions == nil {
		if files, total, err = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType); err != nil {
			log.CtxError(ctx, "查询文件列表: 发生异常[%v]\n", err)
			return resp, err
		}
	} else {
		switch o := req.SearchOptions.Type.(type) {
		case *gencontent.SearchOptions_AllFieldsKey:
			files, total, err = s.FileEsMapper.Search(ctx, convertor.ConvertPostAllFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
		case *gencontent.SearchOptions_MultiFieldsKey:
			files, total, err = s.FileEsMapper.Search(ctx, convertor.ConvertPostMultiFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
		}
		if err != nil {
			log.CtxError(ctx, "搜索用户信息异常[%v]\n", err)
			return resp, err
		}

		if p.LastToken != nil {
			resp.Token = *p.LastToken
		}
		resp.Total = total
		resp.Files = make([]*gencontent.FileInfo, 0, len(files))
		for _, v := range files {
			resp.Files = append(resp.Files, convertor.FileMapperToFile(v))
		}
	}

	resp.Total = total
	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Files = make([]*gencontent.FileInfo, 0, len(files))
	for _, file := range files {
		resp.Files = append(resp.Files, convertor.FileMapperToFile(file))
	}

	return resp, nil
}

func (s *FileService) GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (*gencontent.GetFileCountResp, error) {
	resp := new(gencontent.GetFileCountResp)
	var total int64
	var err error
	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	if total, err = s.FileMongoMapper.Count(ctx, filter); err != nil {
		log.CtxError(ctx, "查询文件总数: 发生异常[%v]\n", err)
		return resp, err
	}
	resp.Count = total
	return resp, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (*gencontent.GetFileBySharingCodeResp, error) {
	resp := new(gencontent.GetFileBySharingCodeResp)
	var err error
	var isTrue bool
	var res *gencontent.GetFileResp
	var shareFile *gencontent.ParsingShareCodeResp

	if shareFile, err = s.ParsingShareCode(ctx, &gencontent.ParsingShareCodeReq{Code: req.SharingCode}); err != nil {
		return resp, err
	}
	if res, err = s.GetFile(ctx, &gencontent.GetFileReq{
		FilterOptions: &gencontent.FileFilterOptions{
			OnlyFileId:   req.FilterOptions.OnlyFatherId,
			IsDel:        1,
			DocumentType: 1,
		},
		IsGetSize: false,
	}); err != nil {
		return resp, err
	}

	mu := sync.Mutex{}
	wait := sync.WaitGroup{}
	wait.Add(len(shareFile.ShareFile.FileList))
	errChan := make(chan error)
	for _, v := range shareFile.ShareFile.FileList {
		go func(id string, errChan chan error) {
			defer wait.Done()
			if id == res.File.FileId {
				mu.Lock()
				isTrue = true
				mu.Unlock()
			} else {
				x, err1 := s.GetFile(ctx, &gencontent.GetFileReq{
					FilterOptions: &gencontent.FileFilterOptions{
						OnlyFileId:   &id,
						IsDel:        1,
						DocumentType: 1,
					},
					IsGetSize: false,
				})
				if err1 != nil {
					errChan <- err1
				}

				mu.Lock()
				if strings.HasPrefix(x.File.Path, res.File.Path) {
					isTrue = true
				}
				mu.Unlock()
			}
		}(v, errChan)
	}

	go func() {
		wait.Wait()
		close(errChan)
	}()
	if err = <-errChan; err != nil {
		return resp, err
	}

	if isTrue {
		data, err1 := s.GetFileList(ctx, &gencontent.GetFileListReq{
			FilterOptions:     req.FilterOptions,
			PaginationOptions: req.PaginationOptions,
		})
		if err1 != nil {
			return resp, err1
		}
		resp.Files = data.Files
		resp.Total = data.Total
		resp.Token = data.Token
	}

	return resp, nil
}

func (s *FileService) GetFolderSize(ctx context.Context, path string) (*gencontent.GetFolderSizeResp, error) {
	resp := new(gencontent.GetFolderSizeResp)
	var size int64
	var err error
	if size, err = s.FileMongoMapper.FindFolderSize(ctx, path); err != nil {
		log.CtxError(ctx, "查询文件夹空间大小: 发生异常[%v]\n", err)
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
		filter := convertor.FileFilterOptionsToFilterOptions(&gencontent.FileFilterOptions{
			OnlyUserId:   &req.File.UserId,
			OnlyFileId:   &req.File.FatherId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		})
		fatherFile, err := s.FileMongoMapper.FindOne(ctx, filter)
		if err != nil {
			log.CtxError(ctx, "查询目标文件夹: 发生异常[%v]\n", err)
			return resp, err
		} else if fatherFile.Type != int64(gencontent.Type_Type_folder) {
			log.CtxError(ctx, "目标文件[%v]不是文件夹\n", req.File.FatherId)
			return resp, consts.ErrFileIsNotDir
		}
		path = fatherFile.Path
	}

	data, err := convertor.FileToFileMapper(req.File)
	if err != nil {
		return resp, err
	}
	data.CreateAt = time.Now()
	data.UpdateAt = time.Now()
	data.Path = path + "/" + data.ID.Hex()
	id, err := s.FileMongoMapper.Insert(ctx, data)
	if err != nil {
		log.CtxError(ctx, "创建文件夹: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.FileId = id
	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (*gencontent.UpdateFileResp, error) {
	resp := new(gencontent.UpdateFileResp)
	data, err := convertor.FileToFileMapper(req.File)
	if err != nil {
		return resp, err
	}
	if _, err = s.FileMongoMapper.Update(ctx, data); err != nil {
		log.CtxError(ctx, "更新文件信息: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (*gencontent.MoveFileResp, error) {
	resp := new(gencontent.MoveFileResp)
	var file *filemapper.File
	var objectfile *filemapper.File
	var err, err1, err2 error

	if err = mr.Finish(func() error {
		file, err1 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId:   &req.UserId,
			OnlyFileId:   &req.FileId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		})

		if err1 != nil {
			return err1
		}
		return nil
	}, func() error {
		objectfile, err2 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId:   &req.UserId,
			OnlyFileId:   &req.FatherId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		})
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		log.CtxError(ctx, "移动文件: 发生异常[%v]\n", err)
		return resp, err
	}

	if objectfile.Type != int64(gencontent.Type_Type_folder) {
		return resp, consts.ErrFileIsNotDir
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if file.Type == int64(gencontent.Type_Type_folder) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}

			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:   v.ID,
					Path: objectfile.Path + v.Path[len(file.Path)-len(file.ID.Hex())-1:],
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}

		file.Path = objectfile.Path + "/" + file.ID.Hex()
		file.FatherId = req.FatherId
		if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return err
			}
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "移动文件: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})

	return resp, err
}

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (*gencontent.DeleteFileResp, error) {
	resp := new(gencontent.DeleteFileResp)
	var file *filemapper.File
	var err error
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "删除文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if req.DeleteType == gencontent.IsDel_Is_soft {
		if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyFileId:   &req.FileId,
			OnlyUserId:   &req.UserId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		}); err != nil {
			return resp, err
		}

		tx := s.FileMongoMapper.StartClient()
		err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
			if err = sessionContext.StartTransaction(); err != nil {
				return err
			}
			if file.Type == int64(gencontent.Type_Type_folder) {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_soft)
					data[i].DeletedAt = time.Now()
					if req.ClearCommunity {
						data[i].Tag = nil
					}
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			file.IsDel = int64(gencontent.IsDel_Is_soft)
			file.DeletedAt = time.Now()
			if req.ClearCommunity {
				file.Tag = nil
			}
			if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
					log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					return err
				}
			}
			if err = sessionContext.CommitTransaction(sessionContext); err != nil {
				log.CtxError(ctx, "移动文件: 提交事务异常[%v]\n", err)
				return err
			}
			return nil
		})
	} else if req.DeleteType == gencontent.IsDel_Is_hard {
		if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyFileId:   &req.FileId,
			OnlyUserId:   &req.UserId,
			IsDel:        int64(gencontent.IsDel_Is_soft),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		}); err != nil {
			return resp, err
		}

		tx := s.FileMongoMapper.StartClient()
		err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
			if err = sessionContext.StartTransaction(); err != nil {
				return err
			}
			if file.Type == int64(gencontent.Type_Type_folder) {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_hard)
					data[i].Tag = nil
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			file.IsDel = int64(gencontent.IsDel_Is_hard)
			file.Tag = nil
			if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
					log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					return err
				}
			}
			if err = sessionContext.CommitTransaction(sessionContext); err != nil {
				log.CtxError(ctx, "移动文件: 提交事务异常[%v]\n", err)
				return err
			}
			return nil
		})
	} else {
		return resp, consts.ErrInvalidDeleteType
	}

	return resp, nil
}

func (s *FileService) RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (*gencontent.RecoverRecycleBinFileResp, error) {
	resp := new(gencontent.RecoverRecycleBinFileResp)
	var file *filemapper.File
	var err error
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "恢复文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
		OnlyFileId:   &req.FileId,
		OnlyUserId:   &req.UserId,
		IsDel:        int64(gencontent.IsDel_Is_soft),
		DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
	}); err != nil {
		return resp, err
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if file.Type == int64(gencontent.Type_Type_folder) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}

			for i := 0; i < len(data); i++ {
				data[i].IsDel = int64(gencontent.IsDel_Is_no)
				data[i].DeletedAt = time.Time{}
				if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}

		file.IsDel = int64(gencontent.IsDel_Is_no)
		file.DeletedAt = time.Time{}
		if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return err
			}
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "移动文件: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})

	return resp, nil
}

func (s *FileService) GetShareList(ctx context.Context, req *gencontent.GetShareListReq) (*gencontent.GetShareListResp, error) {
	resp := new(gencontent.GetShareListResp)
	var shareCodes []*sharefilemapper.ShareFile
	var total int64
	var err error

	filter := convertor.ShareFileFilterOptionsToShareCodeOptions(req.ShareFileFilterOptions)
	p := convertor.ParsePagination(req.PaginationOptions)

	if shareCodes, total, err = s.ShareFileMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType); err != nil {
		log.CtxError(ctx, "查询文件分享链接列表: 发生异常[%v]\n", err)
		return nil, err
	}

	resp.Total = total
	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.ShareCodes = make([]*gencontent.ShareCode, 0, len(shareCodes))
	for _, v := range shareCodes {
		resp.ShareCodes = append(resp.ShareCodes, convertor.ShareFileToShareCode(v))
	}

	return resp, nil
}

func (s *FileService) CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (*gencontent.CreateShareCodeResp, error) {
	resp := new(gencontent.CreateShareCodeResp)
	var id string
	data, err := convertor.ShareFileToShareFileMapper(req.ShareFile)
	if err != nil {
		return resp, err
	}
	data.CreateAt = time.Now()
	data.DeletedAt = data.CreateAt.Add(time.Duration(req.ShareFile.EffectiveTime)*time.Second + 720*time.Hour)
	if id, err = s.ShareFileMongoMapper.Insert(ctx, data); err != nil {
		log.CtxError(ctx, "创建文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.Code = id
	return resp, nil
}

func (s *FileService) UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (*gencontent.UpdateShareCodeResp, error) {
	resp := new(gencontent.UpdateShareCodeResp)
	data, err := convertor.ShareFileToShareFileMapper(req.ShareFile)
	if err != nil {
		return resp, err
	}
	if _, err = s.ShareFileMongoMapper.Update(ctx, data); err != nil {
		log.CtxError(ctx, "修改文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (*gencontent.DeleteShareCodeResp, error) {
	resp := new(gencontent.DeleteShareCodeResp)
	filter := convertor.ShareFileFilterOptionsToShareCodeOptions(req.ShareFileFilterOptions)
	if _, err := s.ShareFileMongoMapper.Delete(ctx, filter); err != nil {
		log.CtxError(ctx, "删除文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (*gencontent.ParsingShareCodeResp, error) {
	resp := new(gencontent.ParsingShareCodeResp)
	var shareFile *sharefilemapper.ShareFile
	var err error
	if shareFile, err = s.ShareFileMongoMapper.FindOne(ctx, req.Code); err != nil {
		log.CtxError(ctx, "提取文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	res := convertor.ShareFileMapperToShareFile(shareFile)
	if res.Status == int64(2) {
		return resp, nil
	}
	resp.ShareFile = res
	return resp, nil
}

func (s *FileService) SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (*gencontent.SaveFileToPrivateSpaceResp, error) {
	resp := new(gencontent.SaveFileToPrivateSpaceResp)
	var path string
	var file, objectfile *filemapper.File
	var err, err1, err2 error
	type kv struct {
		id   string
		path string
	}

	if err = mr.Finish(func() error {
		_, err = primitive.ObjectIDFromHex(req.FileId)
		if err != nil {
			return err
		}
		file, err1 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyFileId:   &req.FileId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(req.DocumentType),
		})
		if err1 != nil {
			return err1
		}
		return nil
	}, func() error {
		_, err = primitive.ObjectIDFromHex(req.FatherId)
		if err != nil {
			return err
		}
		objectfile, err2 = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId:   &req.UserId,
			OnlyFileId:   &req.FatherId,
			IsDel:        int64(gencontent.IsDel_Is_no),
			DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
		})
		if err2 != nil {
			return err2
		}
		return nil
	}); err != nil {
		log.CtxError(ctx, "保存文件: 发生异常[%v]\n", err)
		return resp, err
	}
	if objectfile.Type != int64(gencontent.Type_Type_folder) {
		return resp, consts.ErrFileIsNotDir
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error { // 队列+协程
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		oid := primitive.NewObjectID()
		path = objectfile.Path + "/" + oid.Hex()
		rootFile := &filemapper.File{
			ID:       oid,
			UserId:   req.UserId,
			Name:     file.Name,
			Type:     file.Type,
			Path:     path,
			FatherId: req.FatherId,
			Size:     file.Size,
			FileMd5:  file.FileMd5,
			IsDel:    int64(gencontent.IsDel_Is_no),
			CreateAt: time.Now(),
			UpdateAt: time.Now(),
		}

		if file.Type == int64(gencontent.Type_Type_folder) {
			err = mr.Finish(func() error {
				_, err = s.FileMongoMapper.Insert(sessionContext, rootFile)
				return err
			}, func() error {
				var front kv
				var sonFile *filemapper.File
				queue := make([]kv, 0, 100)
				queue = append(queue, kv{id: file.ID.Hex(), path: rootFile.Path})
				for len(queue) > 0 {
					front = queue[0]
					queue = queue[1:]
					var data []*filemapper.File
					filter := bson.M{"fatherId": front.id, "tag": bson.M{"$ne": nil}}
					if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter, &options.FindOptions{BatchSize: lo.ToPtr(int32(100))}); err != nil {
						return err
					}
					for _, v := range data {
						oid = primitive.NewObjectID()
						path = front.path + "/" + oid.Hex()
						sonFile = &filemapper.File{
							ID:       oid,
							UserId:   req.UserId,
							Name:     v.Name,
							Type:     v.Type,
							Path:     path,
							FatherId: front.id,
							Size:     v.Size,
							FileMd5:  v.FileMd5,
							IsDel:    int64(gencontent.IsDel_Is_no),
							CreateAt: time.Now(),
							UpdateAt: time.Now(),
						}
						if _, err = s.FileMongoMapper.Insert(sessionContext, sonFile); err != nil {
							return err
						}
						if v.Type == int64(gencontent.Type_Type_folder) {
							queue = append(queue, kv{id: v.ID.Hex(), path: sonFile.Path})
						}
					}
				}
				return nil
			})
		} else {
			_, err = s.FileMongoMapper.Insert(sessionContext, rootFile)
		}

		if err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return rbErr
			}
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "保存文件: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})

	return resp, err
}

func (s *FileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (*gencontent.AddFileToPublicSpaceResp, error) {
	resp := new(gencontent.AddFileToPublicSpaceResp)
	var file *filemapper.File
	oid, err := primitive.ObjectIDFromHex(req.File.FileId)
	if err != nil {
		return resp, err
	}
	file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
		OnlyUserId:   &req.File.UserId,
		OnlyFileId:   &req.File.FileId,
		IsDel:        int64(gencontent.IsDel_Is_no),
		DocumentType: int64(gencontent.DocumentType_DocumentType_personal),
	})
	if err != nil {
		log.CtxError(ctx, "保存文件: 发生异常[%v]\n", err)
		return resp, err
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if req.File.Tag == nil {
			req.File.Tag = []string{}
		}
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if req.File.Type == gencontent.Type_Type_folder {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter)
			if err != nil {
				return err
			}
			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:  v.ID,
					Tag: req.File.Tag,
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}
		if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
			ID:  oid,
			Tag: req.File.Tag,
		}); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return err
			}
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "上传文件到社区: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})
	return resp, err
}
