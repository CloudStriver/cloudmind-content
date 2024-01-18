package service

import (
	"context"
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
	CreateFolder(ctx context.Context, req *gencontent.CreateFolderReq) (resp *gencontent.CreateFolderResp, err error)
	UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error)
	MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error)
	DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error)
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
	file, err := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyMd5: lo.ToPtr(req.Md5),
	})
	if err != nil {
		log.CtxError(ctx, "查询文件md5值是否存在: 发生异常[%v]\n", err)
		return resp, err
	}
	if len(file) != 0 {
		resp.Ok = true
	}
	return resp, nil
}

func (s *FileService) GetFile(ctx context.Context, req *gencontent.GetFileReq) (resp *gencontent.GetFileResp, err error) {
	resp = new(gencontent.GetFileResp)
	file, err := s.FileMongoMapper.FindManyNotPagination(ctx, convertor.FileFilterOptionsToFilterOptions(req.FilterOptions))
	if err != nil {
		log.CtxError(ctx, "查询文件详细信息: 发生异常[%v]\n", err)
		return resp, err
	}
	if len(file) != 1 {
		log.CtxError(ctx, "查询文件详细信息: 文件数量不为1\n")
		return resp, consts.ErrNotFound
	}

	resp.File = convertor.FileMapperToFile(file[0])
	if req.IsGetSize && resp.File.Type == gencontent.Type_Type_folder {
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
	var (
		files []*filemapper.File
		total int64
	)

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
	}

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Total = total
	resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](files, func(item *filemapper.File, _ int) *gencontent.FileInfo {
		return convertor.FileMapperToFile(item)
	})

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
	})
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

func (s *FileService) CreateFolder(ctx context.Context, req *gencontent.CreateFolderReq) (resp *gencontent.CreateFolderResp, err error) {
	resp = new(gencontent.CreateFolderResp)
	var path string
	if req.File.UserId == req.File.FatherId {
		path = req.File.UserId
	} else {
		fatherFile, err := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
			OnlyUserId:   lo.ToPtr(req.File.UserId),
			OnlyFatherId: lo.ToPtr(req.File.FatherId),
		})
		if err != nil {
			log.CtxError(ctx, "查询目标文件夹: 发生异常[%v]\n", err)
			return resp, err
		}
		if fatherFile[0].Type != int64(gencontent.Type_Type_folder) {
			log.CtxError(ctx, "目标文件[%v]不是文件夹\n", req.File.FatherId)
			return resp, consts.ErrFileIsNotDir
		}
		path = fatherFile[0].Path
	}

	data, err := convertor.FileToFileMapper(req.File)
	if err != nil {
		return resp, err
	}
	data.Path = path + "/" + data.ID.Hex()

	resp.FileId, err = s.FileMongoMapper.Insert(ctx, data)
	if err != nil {
		log.CtxError(ctx, "创建文件夹: 发生异常[%v]\n", err)
		return resp, err
	}

	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error) {
	resp = new(gencontent.UpdateFileResp)
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

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error) {
	resp = new(gencontent.MoveFileResp)
	var (
		file       *filemapper.File
		fatherFile *filemapper.File
	)

	files, err := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyUserId:  &req.UserId,
		OnlyFileIds: []string{req.FileId, req.FatherId},
	})
	if err != nil {
		return resp, err
	}

	if files[0].ID.Hex() != req.FileId {
		file = files[1]
		fatherFile = files[0]
	} else {
		file = files[0]
		fatherFile = files[1]
	}

	if fatherFile.Type != int64(gencontent.Type_Type_folder) {
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

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error) {
	resp = new(gencontent.DeleteFileResp)
	var files []*filemapper.File
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "删除文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if req.DeleteType == gencontent.IsDel_Is_soft {
		if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
			OnlyFileIds:      []string{req.FileId},
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
			if files[0].Type == int64(gencontent.Type_Type_folder) {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + files[0].Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_soft)
					data[i].DeletedAt = time.Now()
					if req.ClearCommunity {
						data[i].Tags = nil
					}
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			files[0].IsDel = int64(gencontent.IsDel_Is_soft)
			files[0].DeletedAt = time.Now()
			if req.ClearCommunity {
				files[0].Tags = nil
			}
			if _, err = s.FileMongoMapper.Update(sessionContext, files[0]); err != nil {
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
		if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
			OnlyFileIds:      []string{req.FileId},
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
			if files[0].Type == int64(gencontent.Type_Type_folder) {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + files[0].Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				for i := 0; i < len(data); i++ {
					data[i].IsDel = int64(gencontent.IsDel_Is_hard)
					data[i].Tags = nil
					if _, err = s.FileMongoMapper.Update(sessionContext, data[i]); err != nil {
						if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
							log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
							return err
						}
					}
				}
			}

			files[0].IsDel = int64(gencontent.IsDel_Is_hard)
			files[0].Tags = nil
			if _, err = s.FileMongoMapper.Update(sessionContext, files[0]); err != nil {
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

func (s *FileService) RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error) {
	resp = new(gencontent.RecoverRecycleBinFileResp)
	var files []*filemapper.File
	if _, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		log.CtxError(ctx, "恢复文件: 发生异常[%v]\n", err)
		return resp, consts.ErrInvalidId
	}

	if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyFileIds:      []string{req.FileId},
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
		if files[0].Type == int64(gencontent.Type_Type_folder) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + files[0].Path + "/"}}
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

		files[0].IsDel = int64(gencontent.IsDel_Is_no)
		files[0].DeletedAt = time.Time{}
		if _, err = s.FileMongoMapper.Update(sessionContext, files[0]); err != nil {
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
	resp.ShareCodes = make([]*gencontent.ShareCode, 0, len(shareCodes))
	for _, v := range shareCodes {
		resp.ShareCodes = append(resp.ShareCodes, convertor.ShareFileToShareCode(v))
	}

	return resp, nil
}

func (s *FileService) CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (resp *gencontent.CreateShareCodeResp, err error) {
	resp = new(gencontent.CreateShareCodeResp)
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

func (s *FileService) UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error) {
	resp = new(gencontent.UpdateShareCodeResp)
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
	if res.Status == int64(2) {
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
	)
	type kv struct {
		id   string
		path string
	}

	if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyFileIds: []string{req.FileId, req.FatherId},
		OnlyIsDel:   lo.ToPtr(int64(gencontent.IsDel_Is_no)),
	}); err != nil {
		return resp, err
	}

	if files[0].ID.Hex() != req.FileId {
		file = files[1]
		objectfile = files[0]
	} else {
		file = files[0]
		objectfile = files[1]
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
			Md5:      file.Md5,
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
					filter := bson.M{consts.FatherId: front.id, consts.Tags: bson.M{"$ne": nil}}
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
							Md5:      v.Md5,
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

func (s *FileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error) {
	resp = new(gencontent.AddFileToPublicSpaceResp)
	var files []*filemapper.File
	oid, err := primitive.ObjectIDFromHex(req.File.FileId)
	if err != nil {
		return resp, err
	}
	if files, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyUserId:       lo.ToPtr(req.File.UserId),
		OnlyFileIds:      []string{req.File.FileId},
		OnlyIsDel:        lo.ToPtr(int64(gencontent.IsDel_Is_no)),
		OnlyDocumentType: lo.ToPtr(int64(gencontent.DocumentType_DocumentType_personal)),
	}); err != nil {
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
			filter := bson.M{consts.Path: bson.M{"$regex": "^" + files[0].Path + "/"}}
			err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter)
			if err != nil {
				return err
			}
			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:   v.ID,
					Tags: req.File.Tag,
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
						return err
					}
				}
			}
		}
		if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
			ID:   oid,
			Tags: req.File.Tag,
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
