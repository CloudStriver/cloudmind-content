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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"time"
)

type IFileService interface {
	GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (resp *gencontent.GetFileIsExistResp, err error)
	GetFile(ctx context.Context, req *gencontent.GetFileReq) (resp *gencontent.GetFileResp, err error)
	GetFilesByIds(ctx context.Context, req *gencontent.GetFilesByIdsReq) (resp *gencontent.GetFilesByIdsResp, err error)
	GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error)
	GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error)
	GetRecycleBinFiles(ctx context.Context, req *gencontent.GetRecycleBinFilesReq) (resp *gencontent.GetRecycleBinFilesResp, err error)
	GetFolderSize(ctx context.Context, path string) (resp int64, err error)
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
	var file *filemapper.File
	if file, err = s.FileMongoMapper.FindOne(ctx, req.FileId); err != nil {
		log.CtxError(ctx, "查询文件详细信息: 发生异常[%v]\n", err)
		return resp, err
	}
	resp.File = convertor.FileMapperToFile(file)
	if req.IsGetSize && resp.File.SpaceSize == int64(gencontent.Folder_Folder_Size) {
		if resp.File.SpaceSize, err = s.GetFolderSize(ctx, resp.File.Path); err != nil {
			return resp, consts.ErrCalFileSize
		}
	}
	return resp, nil
}

func (s *FileService) GetFilesByIds(ctx context.Context, req *gencontent.GetFilesByIdsReq) (resp *gencontent.GetFilesByIdsResp, err error) {
	resp = new(gencontent.GetFilesByIdsResp)
	var files []*filemapper.File
	if files, err = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err != nil {
		log.CtxError(ctx, "获取标签集 失败[%v]\n", err)
		return resp, err
	}

	// 创建映射：文件ID到文件
	fileMap := make(map[string]*filemapper.File)
	for _, file := range files {
		fileMap[file.ID.Hex()] = file
	}

	// 按req.FileIds中的ID顺序映射和转换
	resp.Files = lo.Map(req.FileIds, func(id string, _ int) *gencontent.FileInfo {
		if file, ok := fileMap[id]; ok {
			return convertor.FileMapperToFile(file)
		}
		return nil // 或者处理找不到文件的情况
	})
	return resp, nil
}
func (s *FileService) GetRecycleBinFiles(ctx context.Context, req *gencontent.GetRecycleBinFilesReq) (resp *gencontent.GetRecycleBinFilesResp, err error) {
	resp = new(gencontent.GetRecycleBinFilesResp)
	var total int64
	var files []*filemapper.File
	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	p := convertor.ParsePagination(req.PaginationOptions)
	if files, total, err = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType); err != nil {
		log.CtxError(ctx, "查询回收站文件列表: 发生异常[%v]\n", err)
		return resp, err
	}
	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](files, func(item *filemapper.File, _ int) *gencontent.FileInfo {
		return convertor.FileMapperToFile(item)
	})
	resp.Total = total
	return resp, nil
}

func (s *FileService) GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error) {
	resp = new(gencontent.GetFileListResp)
	resp.FatherNamePath = "CloudMind"
	var (
		files  []*filemapper.File
		total  int64
		cursor mongop.MongoCursor
		err2   error
	)

	if err = mr.Finish(func() error {
		getFileResp, err1 := s.GetFile(ctx, &gencontent.GetFileReq{
			FileId: req.GetFilterOptions().GetOnlyFatherId(),
		})
		if errors.Is(err1, consts.ErrNotFound) {
			resp.FatherIdPath = req.GetFilterOptions().GetOnlyFatherId()
			return nil
		}
		if err1 != nil {
			return err1
		}
		resp.FatherIdPath = getFileResp.File.Path
		paths := strings.Split(getFileResp.File.Path, "/")
		if len(paths) > 1 {
			filelist, err1 := s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
				OnlyFileIds: paths[1:],
			})
			if err1 != nil {
				return err1
			}
			lo.ForEach(filelist, func(item *filemapper.File, _ int) {
				resp.FatherNamePath += "/" + item.Name
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

func (s *FileService) CheckShareFile(ctx context.Context, shareFiles []*filemapper.File, fileId *string) (*gencontent.FileInfo, bool, error) {
	var (
		err error
		ok  bool
		res *gencontent.GetFileResp
	)

	if res, err = s.GetFile(ctx, &gencontent.GetFileReq{
		FileId:    *fileId,
		IsGetSize: false,
	}); err != nil {
		return nil, ok, err
	}

	for _, file := range shareFiles {
		if strings.HasPrefix(file.Path, res.File.Path) {
			ok = true
			break
		}
	}

	return res.File, ok, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error) {
	resp = new(gencontent.GetFileBySharingCodeResp)
	var (
		ok         bool
		shareFiles []*filemapper.File
		res        *gencontent.FileInfo
		data       *gencontent.GetFileListResp
	)

	if shareFiles, err = s.FileMongoMapper.FindManyNotPagination(ctx, &filemapper.FilterOptions{
		OnlyFileIds: req.FileIds,
		OnlyIsDel:   lo.ToPtr(int64(gencontent.Deletion_Deletion_notDel)),
	}); err != nil {
		return resp, err
	}

	fmt.Printf("\n[%v]\n", req)
	if req.OnlyFileId != nil {
		if res, ok, err = s.CheckShareFile(ctx, shareFiles, req.OnlyFileId); err != nil {
			return resp, err
		}
		if ok {
			resp.Files = []*gencontent.FileInfo{res}
		}
	} else if req.OnlyFatherId != nil {
		if _, ok, err = s.CheckShareFile(ctx, shareFiles, req.OnlyFatherId); err != nil {
			return resp, err
		}
		if ok {
			if data, err = s.GetFileList(ctx, &gencontent.GetFileListReq{
				FilterOptions:     &gencontent.FileFilterOptions{OnlyFatherId: req.OnlyFatherId, OnlyIsDel: lo.ToPtr(int64(gencontent.Deletion_Deletion_notDel))},
				PaginationOptions: req.PaginationOptions,
				SortOptions:       req.SortOptions,
			}); err != nil {
				return resp, err
			}
			resp.Files = data.Files
			resp.Total = data.Total
			resp.Token = data.Token
			resp.FatherIdPath = data.FatherIdPath
			resp.FatherNamePath = data.FatherNamePath
		}
	} else {
		resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](shareFiles, func(item *filemapper.File, _ int) *gencontent.FileInfo {
			return convertor.FileMapperToFile(item)
		})
		resp.Total = int64(len(shareFiles))
		resp.FatherIdPath = data.FatherIdPath
		resp.FatherNamePath = data.FatherNamePath
	}

	return resp, nil
}

func (s *FileService) GetFolderSize(ctx context.Context, path string) (resp int64, err error) {
	if resp, err = s.FileMongoMapper.FindFolderSize(ctx, path); err != nil {
		log.CtxError(ctx, "查询文件夹空间大小: 发生异常[%v]\n", err)
		return 0, err
	}
	return resp, nil
}

func (s *FileService) CreateFile(ctx context.Context, req *gencontent.CreateFileReq) (resp *gencontent.CreateFileResp, err error) {
	resp = new(gencontent.CreateFileResp)
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
	file := convertor.FileInfoToFileMapper(req.File)
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if *file.Size == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}

			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{
					ID:   v.ID,
					Path: req.NewPath + v.Path[len(file.Path)-len(file.ID.Hex())-1:],
				}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					}
					return err
				}
			}
		}

		file.Path = req.NewPath + "/" + file.ID.Hex()
		file.FatherId = req.FatherId
		if _, err = s.FileMongoMapper.Update(sessionContext, file); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
			}
			return err
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
	file := convertor.FileInfoToFileMapper(req.File)

	update := bson.M{}
	switch req.DeleteType {
	case int64(gencontent.Deletion_Deletion_softDel):
		update["$set"] = bson.M{
			consts.IsDel:     int64(gencontent.Deletion_Deletion_softDel),
			consts.DeletedAt: time.Now(),
		}
	case int64(gencontent.Deletion_Deletion_hardDel):
		update["$set"] = bson.M{
			consts.IsDel: int64(gencontent.Deletion_Deletion_hardDel),
		}
	default:
		return resp, consts.ErrInvalidDeleteType
	}

	if req.ClearCommunity || req.DeleteType == int64(gencontent.Deletion_Deletion_hardDel) {
		update["$unset"] = bson.M{
			consts.Zone:        "",
			consts.SubZone:     "",
			consts.Description: "",
			consts.Labels:      "",
		}
	}

	ids := make([]string, 0, 20)
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}

		ids = append(ids, file.ID.Hex())
		if *file.Size == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}
			for _, v := range data {
				ids = append(ids, v.ID.Hex())
			}
		}

		if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, file.UserId, update); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
			}
			return err
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})

	return resp, nil
}

func (s *FileService) RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error) {
	resp = new(gencontent.RecoverRecycleBinFileResp)
	file := convertor.FileInfoToFileMapper(req.File)
	update := bson.M{
		"$set":   bson.M{consts.IsDel: int64(gencontent.Deletion_Deletion_notDel)},
		"$unset": bson.M{consts.DeletedAt: ""},
	}

	ids := make([]string, 0, 20)
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if *file.Size == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}
			for _, v := range data {
				ids = append(ids, v.ID.Hex())
			}
		}

		paths := strings.Split(file.Path, "/")
		for _, id := range paths {
			if id == file.UserId {
				continue
			}
			ids = append(ids, id)
		}

		if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, file.UserId, update); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "恢复文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
			}
			return err
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

	if p.LastToken != nil {
		resp.Token = *p.LastToken
	}
	resp.ShareCodes = lo.Map[*sharefilemapper.ShareFile, *gencontent.ShareCode](shareCodes, func(item *sharefilemapper.ShareFile, _ int) *gencontent.ShareCode {
		return convertor.ShareFileToShareCode(item)
	})
	resp.Total = total
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
	if shareFile, err = s.ShareFileMongoMapper.FindOne(ctx, req.Code, req.Key); err != nil {
		log.CtxError(ctx, "提取文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	res := convertor.ShareFileMapperToShareFile(shareFile)
	if res.Status == int64(gencontent.Validity_Validity_expired) {
		return resp, nil
	}
	resp.ShareFile = res
	return resp, nil
}

func (s *FileService) SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (resp *gencontent.SaveFileToPrivateSpaceResp, err error) {
	resp = new(gencontent.SaveFileToPrivateSpaceResp)
	type kv struct {
		id   string
		path string
	}
	var err1 error
	file := convertor.FileInfoToFileMapper(req.File)
	tx := s.FileMongoMapper.StartClient()

	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err1 = sessionContext.StartTransaction(); err1 != nil {
			return err1
		}
		if resp.FileId, err1 = s.FileMongoMapper.FindAndInsert(sessionContext, &filemapper.File{ // 创建根文件
			UserId:   req.UserId,
			Name:     file.Name,
			Type:     file.Type,
			Path:     req.NewPath,
			FatherId: req.FatherId,
			Size:     file.Size,
			FileMd5:  file.FileMd5,
			IsDel:    int64(gencontent.Deletion_Deletion_notDel),
		}); err1 != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
			}
			return err1
		}
		if *file.Size == int64(gencontent.Folder_Folder_Size) { // 若是文件夹，开始根据原文件夹层层创建
			var front kv
			queue := make([]kv, 0, 20)
			queue = append(queue, kv{id: file.ID.Hex(), path: req.NewPath + "/" + resp.FileId})
			for len(queue) > 0 {
				front = queue[0]
				queue = queue[1:]
				var ids []string
				var data []*filemapper.File
				var filter bson.M
				if req.DocumentType == int64(gencontent.Space_Space_public) {
					filter = bson.M{consts.FatherId: front.id, consts.Zone: bson.M{"$exists": true, "$ne": ""}, consts.SubZone: bson.M{"$exists": true, "$ne": ""}}
				} else if req.DocumentType == int64(gencontent.Space_Space_private) {
					filter = bson.M{consts.FatherId: front.id}
				} else {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", "访问未知空间", rbErr)
					}
					return consts.ErrIllegalOperation
				}

				if err1 = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter, &options.FindOptions{BatchSize: lo.ToPtr(int32(100))}); err1 != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
						return rbErr
					}
				}

				if len(data) <= 0 {
					continue
				}

				sonFiles := lo.Map(data, func(item *filemapper.File, _ int) *filemapper.File {
					return &filemapper.File{
						UserId:   file.UserId,
						Name:     item.Name,
						Type:     item.Type,
						Path:     front.path,
						FatherId: front.path[len(front.path)-len(front.id):],
						Size:     item.Size,
						FileMd5:  item.FileMd5,
						IsDel:    int64(gencontent.Deletion_Deletion_notDel),
					}
				})

				if ids, err1 = s.FileMongoMapper.FindAndInsertMany(sessionContext, sonFiles); err1 != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
					}
					return err1
				}

				for i, v := range data {
					if *v.Size == int64(gencontent.Folder_Folder_Size) {
						queue = append(queue, kv{id: v.ID.Hex(), path: front.path + "/" + ids[i]})
					}
				}
			}
		}

		if err1 = sessionContext.CommitTransaction(sessionContext); err1 != nil {
			log.CtxError(ctx, "保存文件: 提交事务异常[%v]\n", err1)
			return err1
		}
		return nil
	})

	return resp, err
}

func (s *FileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error) {
	resp = new(gencontent.AddFileToPublicSpaceResp)
	file := convertor.FileInfoToFileMapper(req.File)
	update := bson.M{
		"$set": bson.M{
			consts.Zone:        req.File.Zone,
			consts.SubZone:     req.File.Zone,
			consts.Description: req.File.Description,
			consts.Labels:      req.File.Labels,
		},
	}
	ids := make([]string, 0, 20)
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		ids = append(ids, file.ID.Hex())
		if *file.Size == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{consts.Path: bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}
			for _, v := range data {
				ids = append(ids, v.ID.Hex())
			}
		}
		if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, req.File.UserId, update); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
			}
			return err
		}
		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "上传文件到社区: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})
	resp.FileIds = ids
	return resp, err
}
