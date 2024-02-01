package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	sharefilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
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
	"time"
)

type IFileService interface {
	GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (resp *gencontent.GetFileIsExistResp, err error)
	GetFile(ctx context.Context, req *gencontent.GetFileReq) (resp *gencontent.GetFileResp, err error)
	GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error)
	GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (resp *gencontent.GetFileCountResp, err error)
	GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error)
	GetFolderSize(ctx context.Context, path string) (resp *gencontent.GetFolderSizeResp, err error)
	CreateFile(ctx context.Context, req *gencontent.CreateFileReq) (resp *gencontent.CreateFileResp, err error)
	UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error)
	MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error)
	DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error)
	CompletelyRemoveFile(ctx context.Context, req *gencontent.CompletelyRemoveFileReq) (resp *gencontent.CompletelyRemoveFileResp, err error)
	RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error)
	GetShareList(ctx context.Context, req *gencontent.GetShareListReq) (resp *gencontent.GetShareListResp, err error)
	CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (resp *gencontent.CreateShareCodeResp, err error)
	UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error)
	DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (resp *gencontent.DeleteShareCodeResp, err error)
	ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (resp *gencontent.ParsingShareCodeResp, err error)
	SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (resp *gencontent.SaveFileToPrivateSpaceResp, err error)
	AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error)
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

func (s *FileService) GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (resp *gencontent.GetFileIsExistResp, err error) {
	resp = new(gencontent.GetFileIsExistResp)
	var ok bool
	ok, err = s.FileMongoMapper.FindFileIsExist(ctx, req.Md5)
	if err != nil {
		log.CtxError(ctx, "查询文件md5值是否存在: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.Ok = ok
	return resp, nil
}

func (s *FileService) GetFile(ctx context.Context, req *gencontent.GetFileReq) (resp *gencontent.GetFileResp, err error) {
	resp = new(gencontent.GetFileResp)
	file, err := s.FileMongoMapper.FindOne(ctx, convertor.FileFilterOptionsToFilterOptions(req.FilterOptions))
	if err != nil {
		log.CtxError(ctx, "查询文件详细信息: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.File = convertor.FileMapperToFile(file)
	if req.IsGetSize && resp.File.SpaceSize == consts.FolderSize {
		res, err := s.GetFolderSize(ctx, resp.File.Path)
		if err != nil {
			return resp, consts.ErrCalFileSize
		}
		resp.File.SpaceSize = res.SpaceSize
	}

	return resp, nil
}

func (s *FileService) GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error) {
	resp = new(gencontent.GetFileListResp)
	resp.FatherPath = "CloudMind"
	var (
		files  []*filemapper.File
		total  int64
		cursor mongop.MongoCursor
		err2   error
	)

	if err = mr.Finish(func() error {
		getFileResp, err1 := s.GetFile(ctx, &gencontent.GetFileReq{
			FilterOptions: &gencontent.FileFilterOptions{
				OnlyFileId: lo.ToPtr(req.GetFilterOptions().GetOnlyFatherId()),
			},
		})
		fmt.Println(getFileResp)
		if errors.Is(err1, consts.ErrNotFound) {
			return nil
		}
		if err1 != nil {
			return err1
		}
		paths := strings.Split(getFileResp.File.Path, "/")
		if len(paths) > 1 {
			filelist, err1 := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
				OnlyFileIds: paths[1:],
			})
			if err1 != nil {
				return err1
			}
			lo.ForEach(filelist, func(item *filemapper.File, _ int) {
				resp.FatherPath += "/" + item.Name
			})
		}
		return nil
	}, func() error {
		switch req.GetSortOptions() {
		case gencontent.SortOptions_SortOptions_createAtAsc:
			cursor = filemapper.CreateAtAscCursorType
		case gencontent.SortOptions_SortOptions_createAtDesc:
			cursor = filemapper.CreateAtDescCursorType
		case gencontent.SortOptions_SortOptions_updateAtAsc:
			cursor = filemapper.UpdateAtAscCursorType
		case gencontent.SortOptions_SortOptions_updateAtDesc:
			cursor = filemapper.UpdateAtDescCursorType
		}

		filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
		p := convertor.ParsePagination(req.PaginationOptions)
		if req.SearchOptions == nil {
			if files, total, err2 = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, cursor); err != nil {
				log.CtxError(ctx, "查询文件列表: 发生异常[%v]\n", err2)
				return err2
			}
		} else {
			switch o := req.SearchOptions.Type.(type) {
			case *gencontent.SearchOptions_AllFieldsKey:
				files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileAllFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
			case *gencontent.SearchOptions_MultiFieldsKey:
				files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileMultiFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
			}
			if err2 != nil {
				log.CtxError(ctx, "搜索文件列表异常[%v]\n", err2)
				return err2
			}
		}
		if p.LastToken != nil {
			resp.Token = *p.LastToken
		}
		resp.Total = total
		resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](files, func(item *filemapper.File, _ int) *gencontent.FileInfo {
			return convertor.FileMapperToFile(item)
		})
		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) GetFileCount(ctx context.Context, req *gencontent.GetFileCountReq) (resp *gencontent.GetFileCountResp, err error) {
	resp = new(gencontent.GetFileCountResp)
	var total int64
	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	if total, err = s.FileMongoMapper.Count(ctx, filter); err != nil {
		log.CtxError(ctx, "查询文件总数: 发生异常[%v]\n", err)
		return resp, err
	}
	resp.Count = total
	return resp, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error) {
	resp = new(gencontent.GetFileBySharingCodeResp)
	var (
		isTrue     bool
		res        *gencontent.GetFileResp
		shareFile  *gencontent.ParsingShareCodeResp
		shareFiles []*filemapper.File
	)

	if shareFile, err = s.ParsingShareCode(ctx, &gencontent.ParsingShareCodeReq{Code: req.SharingCode}); err != nil {
		return resp, err
	}
	if res, err = s.GetFile(ctx, &gencontent.GetFileReq{
		FilterOptions: &gencontent.FileFilterOptions{
			OnlyFileId: req.FilterOptions.OnlyFileId,
		},
		IsGetSize: false,
	}); err != nil {
		return resp, err
	}

	shareFiles, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyFileIds: shareFile.ShareFile.FileList,
		OnlyIsDel:   lo.ToPtr(int64(gencontent.IsDel_Is_no)),
	})
	if err != nil {
		return resp, err
	}
	for _, file := range shareFiles {
		if strings.HasPrefix(file.Path, res.File.Path) {
			isTrue = true
			break
		}
	}

	if isTrue {
		data, err := s.GetFileList(ctx, &gencontent.GetFileListReq{
			FilterOptions:     req.FilterOptions,
			PaginationOptions: req.PaginationOptions,
		})
		if err != nil {
			return resp, err
		}
		resp.Files = data.Files
		resp.Total = data.Total
		resp.Token = data.Token
	}

	return resp, nil
}

func (s *FileService) GetFolderSize(ctx context.Context, path string) (resp *gencontent.GetFolderSizeResp, err error) {
	resp = new(gencontent.GetFolderSizeResp)
	if resp.SpaceSize, err = s.FileMongoMapper.FindFolderSize(ctx, path); err != nil {
		log.CtxError(ctx, "查询文件夹空间大小: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) CreateFile(ctx context.Context, req *gencontent.CreateFileReq) (resp *gencontent.CreateFileResp, err error) {
	resp = new(gencontent.CreateFileResp)
	var path string
	if req.File.UserId == req.File.FatherId {
		path = req.File.UserId
	} else {
		fatherFile, err := s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyUserId: lo.ToPtr(req.File.UserId),
			OnlyFileId: lo.ToPtr(req.File.FatherId),
		})
		if err != nil {
			log.CtxError(ctx, "查询目标文件夹: 发生异常[%v]\n", err)
			return resp, err
		}
		if *fatherFile.Size != consts.FolderSize {
			log.CtxError(ctx, "目标文件[%v]不是文件夹\n", req.File.FatherId)
			return resp, consts.ErrFileIsNotDir
		}
		path = fatherFile.Path
	}

	req.File.Path = path
	data := convertor.FileToFileMapper(req.File)
	resp.FileId, err = s.FileMongoMapper.Insert(ctx, data)
	if err != nil {
		log.CtxError(ctx, "创建文件: 发生异常[%v]\n", err)
		return resp, err
	}

	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error) {
	resp = new(gencontent.UpdateFileResp)
	data := convertor.FileToFileMapper(req.File)
	if _, err = s.FileMongoMapper.Update(ctx, data); err != nil {
		log.CtxError(ctx, "更新文件信息: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error) {
	resp = new(gencontent.MoveFileResp)
	var (
		file       *filemapper.File
		fatherFile *filemapper.File
	)

	files, err := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyUserId:  lo.ToPtr(req.UserId),
		OnlyFileIds: []string{req.FileId, req.FatherId},
		OnlyIsDel:   lo.ToPtr(int64(gencontent.IsDel_Is_no)),
	})
	if err != nil {
		return resp, err
	}

	if req.FatherId == req.UserId {
		files = append(files, &filemapper.File{
			Path: req.FatherId,
			Size: lo.ToPtr(int64(-1)),
		})
	}

	if len(files) != 2 {
		return resp, consts.ErrIllegalOperation
	}

	if files[0].ID.Hex() != req.FileId {
		file = files[1]
		fatherFile = files[0]
	} else {
		file = files[0]
		fatherFile = files[1]
	}

	if *fatherFile.Size != consts.FolderSize {
		return resp, consts.ErrFileIsNotDir
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if *file.Size == consts.FolderSize {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}

			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:   v.ID,
					Path: fatherFile.Path + v.Path[len(file.Path)-len(file.ID.Hex())-1:],
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}

		file.Path = fatherFile.Path + "/" + file.ID.Hex()
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

func (s *FileService) CompletelyRemoveFile(ctx context.Context, req *gencontent.CompletelyRemoveFileReq) (resp *gencontent.CompletelyRemoveFileResp, err error) {
	resp = new(gencontent.CompletelyRemoveFileResp)
	if _, err = s.FileMongoMapper.Delete(ctx, req.FileId, req.UserId); err != nil {
		log.CtxError(ctx, "删除文件: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error) {
	resp = new(gencontent.DeleteFileResp)
	var file *filemapper.File
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "删除文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if req.DeleteType == gencontent.IsDel_Is_soft {
		if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyFileId:       lo.ToPtr(req.FileId),
			OnlyUserId:       lo.ToPtr(req.UserId),
			OnlyIsDel:        lo.ToPtr(int64(gencontent.IsDel_Is_no)),
			OnlyDocumentType: lo.ToPtr(int64(gencontent.DocumentType_DocumentType_personal)),
		}); err != nil {
			return resp, err
		}

		tx := s.FileMongoMapper.StartClient()
		err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
			if err = sessionContext.StartTransaction(); err != nil {
				return err
			}
			if *file.Size == consts.FolderSize {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_soft)
					data[i].DeletedAt = time.Now()
					if req.ClearCommunity {
						data[i].Zone = ""
						data[i].SubZone = ""
					}
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			file.IsDel = int64(gencontent.IsDel_Is_soft)
			file.DeletedAt = time.Now()
			if req.ClearCommunity {
				file.Zone = ""
				file.SubZone = ""
			}
			if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
					log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					return err
				}
			}
			if err = sessionContext.CommitTransaction(sessionContext); err != nil {
				log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
				return err
			}
			return nil
		})
	} else if req.DeleteType == gencontent.IsDel_Is_hard {
		if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
			OnlyFileId:       lo.ToPtr(req.FileId),
			OnlyUserId:       lo.ToPtr(req.UserId),
			OnlyIsDel:        lo.ToPtr(int64(gencontent.IsDel_Is_soft)),
			OnlyDocumentType: lo.ToPtr(int64(gencontent.DocumentType_DocumentType_personal)),
		}); err != nil {
			return resp, err
		}

		tx := s.FileMongoMapper.StartClient()
		err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
			if err = sessionContext.StartTransaction(); err != nil {
				return err
			}
			if *file.Size == consts.FolderSize {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_hard)
					data[i].Zone = ""
					data[i].SubZone = ""
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			file.IsDel = int64(gencontent.IsDel_Is_hard)
			file.Zone = ""
			file.SubZone = ""
			if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
					log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					return err
				}
			}
			if err = sessionContext.CommitTransaction(sessionContext); err != nil {
				log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
				return err
			}
			return nil
		})
	} else {
		return resp, consts.ErrInvalidDeleteType
	}

	return resp, nil
}

func (s *FileService) RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error) {
	resp = new(gencontent.RecoverRecycleBinFileResp)
	var file *filemapper.File
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "恢复文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
		OnlyFileId:       lo.ToPtr(req.FileId),
		OnlyUserId:       lo.ToPtr(req.UserId),
		OnlyIsDel:        lo.ToPtr(int64(gencontent.IsDel_Is_soft)),
		OnlyDocumentType: lo.ToPtr(int64(gencontent.DocumentType_DocumentType_personal)),
	}); err != nil {
		return resp, err
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if *file.Size == consts.FolderSize {
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
						log.CtxError(ctx, "恢复文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}

		path := strings.Split(file.Path, "/")
		if err = mr.Finish(lo.Map(path, func(id string, _ int) func() error {
			return func() error {
				if id == req.UserId {
					return nil
				}
				oid, _ := primitive.ObjectIDFromHex(id)
				_, err = s.FileMongoMapper.Update(ctx, &filemapper.File{ID: oid, UserId: req.UserId, IsDel: consts.NotDel})
				return err
			}
		})...); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "恢复文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return err
			}
		}

		file.IsDel = int64(gencontent.IsDel_Is_no)
		file.DeletedAt = time.Time{}
		if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "恢复文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				return err
			}
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "恢复文件: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})

	return resp, nil
}

func (s *FileService) GetShareList(ctx context.Context, req *gencontent.GetShareListReq) (resp *gencontent.GetShareListResp, err error) {
	resp = new(gencontent.GetShareListResp)
	var (
		shareCodes []*sharefilemapper.ShareFile
		total      int64
	)
	p := convertor.ParsePagination(req.PaginationOptions)
	if shareCodes, total, err = s.ShareFileMongoMapper.FindManyAndCount(ctx, convertor.ShareFileFilterOptionsToShareCodeOptions(req.ShareFileFilterOptions),
		p, mongop.IdCursorType); err != nil {
		log.CtxError(ctx, "查询文件分享链接列表: 发生异常[%v]\n", err)
		return nil, err
	}

	resp.Total = total
	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.ShareCodes = lo.Map[*sharefilemapper.ShareFile, *gencontent.ShareCode](shareCodes, func(item *sharefilemapper.ShareFile, _ int) *gencontent.ShareCode {
		return convertor.ShareFileToShareCode(item)
	})
	return resp, nil
}

func (s *FileService) CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (resp *gencontent.CreateShareCodeResp, err error) {
	resp = new(gencontent.CreateShareCodeResp)
	var id, key string
	data := convertor.ShareFileToShareFileMapper(req.ShareFile)
	data.CreateAt = time.Now()
	if req.ShareFile.EffectiveTime >= 0 {
		data.DeletedAt = data.CreateAt.Add(time.Duration(req.ShareFile.EffectiveTime)*time.Second + 720*time.Hour)
	}
	if id, key, err = s.ShareFileMongoMapper.Insert(ctx, data); err != nil {
		log.CtxError(ctx, "创建文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}

	resp.Code = id
	resp.Key = key
	return resp, nil
}

func (s *FileService) UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error) {
	resp = new(gencontent.UpdateShareCodeResp)
	data := convertor.ShareFileToShareFileMapper(req.ShareFile)
	if _, err = s.ShareFileMongoMapper.Update(ctx, data); err != nil {
		log.CtxError(ctx, "修改文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (resp *gencontent.DeleteShareCodeResp, err error) {
	resp = new(gencontent.DeleteShareCodeResp)
	filter := convertor.ShareFileFilterOptionsToShareCodeOptions(req.ShareFileFilterOptions)
	if _, err := s.ShareFileMongoMapper.Delete(ctx, filter); err != nil {
		log.CtxError(ctx, "删除文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (resp *gencontent.ParsingShareCodeResp, err error) {
	resp = new(gencontent.ParsingShareCodeResp)
	var shareFile *sharefilemapper.ShareFile
	if shareFile, err = s.ShareFileMongoMapper.FindOne(ctx, req.Code); err != nil {
		log.CtxError(ctx, "提取文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	res := convertor.ShareFileMapperToShareFile(shareFile)
	if res.Status == int64(consts.Invalid) {
		return resp, nil
	}
	resp.ShareFile = res
	return resp, nil
}

func (s *FileService) SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (resp *gencontent.SaveFileToPrivateSpaceResp, err error) {
	resp = new(gencontent.SaveFileToPrivateSpaceResp)
	var (
		path       string
		files      []*filemapper.File
		file       *filemapper.File
		objectfile *filemapper.File
		err1, err2 error
	)
	type kv struct {
		id   string
		path string
	}

	if req.FileId == req.FatherId {
		return resp, consts.ErrIllegalOperation
	}

	// 查看目标文件夹和要保存的文件
	if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyFileIds: []string{req.FileId, req.FatherId},
		OnlyIsDel:   lo.ToPtr(int64(gencontent.IsDel_Is_no)),
	}); err != nil {
		return resp, err
	}

	// 判断目标文件夹和要保存的文件是否存在
	if len(files) != 2 {
		return resp, consts.ErrIllegalOperation
	}

	if files[0].ID.Hex() != req.FileId {
		file = files[1]
		objectfile = files[0]
	} else {
		file = files[0]
		objectfile = files[1]
	}

	if *objectfile.Size != consts.FolderSize {
		return resp, consts.ErrFileIsNotDir
	} // 如果目标文件不是文件夹，则返回错误
	if file.UserId == objectfile.UserId {
		return resp, consts.ErrIllegalOperation
	} // 如果目标文件和要保存的文件是同一个用户的，则返回错误
	if req.DocumentType == gencontent.DocumentType_DocumentType_public && (objectfile.Zone == "" || objectfile.SubZone == "") { // 如果要保存的文件不是社区文件，则返回错误
		return resp, consts.ErrIllegalOperation
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error { // 队列+协程
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		oid := primitive.NewObjectID()
		resp.FileId = oid.Hex()
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

		if *file.Size == consts.FolderSize {
			err = mr.Finish(func() error {
				_, err1 = s.FileMongoMapper.Insert(sessionContext, rootFile)
				return err1
			}, func() error {
				var front kv
				var sonFile *filemapper.File
				queue := make([]kv, 0, 100)
				queue = append(queue, kv{id: file.ID.Hex(), path: rootFile.Path})
				for len(queue) > 0 {
					front = queue[0]
					queue = queue[1:]
					var data []*filemapper.File
					var filter bson.M
					if req.DocumentType == gencontent.DocumentType_DocumentType_public {
						filter = bson.M{"fatherId": front.id, "tag": bson.M{"$ne": nil}}
					} else if req.DocumentType == gencontent.DocumentType_DocumentType_personal {
						filter = bson.M{"fatherId": front.id}
					} else {
						return consts.ErrIllegalOperation
					}

					if err2 = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter, &options.FindOptions{BatchSize: lo.ToPtr(int32(100))}); err2 != nil {
						return err2
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
						if _, err2 = s.FileMongoMapper.Insert(sessionContext, sonFile); err != nil {
							return err2
						}
						if *v.Size == consts.FolderSize {
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

func (s *FileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error) {
	resp = new(gencontent.AddFileToPublicSpaceResp)
	var file *filemapper.File
	oid, err := primitive.ObjectIDFromHex(req.File.FileId)
	if err != nil {
		return resp, err
	}
	if file, err = s.FileMongoMapper.FindOne(ctx, &filemapper.FilterOptions{
		OnlyUserId:       lo.ToPtr(req.File.UserId),
		OnlyFileId:       lo.ToPtr(req.File.FileId),
		OnlyIsDel:        lo.ToPtr(int64(gencontent.IsDel_Is_no)),
		OnlyDocumentType: lo.ToPtr(int64(gencontent.DocumentType_DocumentType_personal)),
	}); err != nil {
		log.CtxError(ctx, "上传文件到社区过程中: 发生异常[%v]\n", err)
		return resp, err
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if *req.File.SpaceSize == consts.FolderSize {
			var data []*filemapper.File
			filter := bson.M{consts.Path: bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}
			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:      v.ID,
					UserId:  v.UserId,
					Zone:    req.File.Zone,
					SubZone: req.File.SubZone,
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}
		if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
			ID:      oid,
			UserId:  req.File.UserId,
			Zone:    req.File.Zone,
			SubZone: req.File.SubZone,
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
