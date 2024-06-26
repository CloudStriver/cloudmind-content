package service

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	publicfilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/publicfile"
	sharefilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
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
	GetFilesByIds(ctx context.Context, req *gencontent.GetFilesByIdsReq) (resp *gencontent.GetFilesByIdsResp, err error)
	GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error)
	GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error)
	GetRecycleBinFiles(ctx context.Context, req *gencontent.GetRecycleBinFilesReq) (resp *gencontent.GetRecycleBinFilesResp, err error)
	GetFolderSize(ctx context.Context, path string) (resp int64, err error)
	CreateFile(ctx context.Context, req *gencontent.CreateFileReq) (resp *gencontent.CreateFileResp, err error)
	UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error)
	MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error)
	DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error)
	EmptyRecycleBin(ctx context.Context, req *gencontent.EmptyRecycleBinReq) (resp *gencontent.EmptyRecycleBinResp, err error)
	CompletelyRemoveFile(ctx context.Context, req *gencontent.CompletelyRemoveFileReq) (resp *gencontent.CompletelyRemoveFileResp, err error)
	RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error)
	GetShareList(ctx context.Context, req *gencontent.GetShareListReq) (resp *gencontent.GetShareListResp, err error)
	CheckShareFile(ctx context.Context, req *gencontent.CheckShareFileReq) (resp *gencontent.CheckShareFileResp, err error)
	CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (resp *gencontent.CreateShareCodeResp, err error)
	UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error)
	DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (resp *gencontent.DeleteShareCodeResp, err error)
	ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (resp *gencontent.ParsingShareCodeResp, err error)
	SaveShareFileToPrivateSpace(ctx context.Context, req *gencontent.SaveShareFileToPrivateSpaceReq) (resp *gencontent.SaveShareFileToPrivateSpaceResp, err error)
	SavePublicFileToPrivateSpace(ctx context.Context, req *gencontent.SavePublicFileToPrivateSpaceReq) (resp *gencontent.SavePublicFileToPrivateSpaceResp, err error)
}

type FileService struct {
	Config                *config.Config
	FileMongoMapper       filemapper.IMongoMapper
	FileEsMapper          filemapper.IEsMapper
	PublicFileMongoMapper publicfilemapper.IMongoMapper
	ShareFileMongoMapper  sharefilemapper.IMongoMapper
}

var FileSet = wire.NewSet(
	wire.Struct(new(FileService), "*"),
	wire.Bind(new(IFileService), new(*FileService)),
)

func (s *FileService) GetFileIsExist(ctx context.Context, req *gencontent.GetFileIsExistReq) (resp *gencontent.GetFileIsExistResp, err error) {
	resp = new(gencontent.GetFileIsExistResp)
	resp.Ok, err = s.FileMongoMapper.FindFileIsExist(ctx, req.Md5)
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *FileService) GetFile(ctx context.Context, req *gencontent.GetFileReq) (resp *gencontent.GetFileResp, err error) {
	resp = new(gencontent.GetFileResp)
	var file *filemapper.File
	if file, err = s.FileMongoMapper.FindOne(ctx, req.FileId); err != nil {
		return resp, err
	}
	resp = &gencontent.GetFileResp{
		UserId:    file.UserId,
		Name:      file.Name,
		Type:      file.Type,
		Path:      file.Path,
		FatherId:  file.FatherId,
		SpaceSize: file.Size,
		Md5:       file.FileMd5,
		IsDel:     file.IsDel,
		CreateAt:  file.CreateAt.UnixMilli(),
		UpdateAt:  file.UpdateAt.UnixMilli(),
		DeleteAt:  file.DeletedAt.UnixMilli(),
	}

	// 如果是获取文件夹大小，需要计算文件夹大小
	if req.IsGetSize && resp.SpaceSize == int64(gencontent.Folder_Folder_Size) {
		if resp.SpaceSize, err = s.GetFolderSize(ctx, resp.Path); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (s *FileService) GetFilesByIds(ctx context.Context, req *gencontent.GetFilesByIdsReq) (resp *gencontent.GetFilesByIdsResp, err error) {
	resp = new(gencontent.GetFilesByIdsResp)
	var files []*filemapper.File
	if files, err = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err != nil {
		return resp, err
	}

	// 创建映射：文件ID到文件
	fileMap := make(map[string]*filemapper.File, len(req.FileIds))
	lo.ForEach(files, func(file *filemapper.File, _ int) {
		fileMap[file.ID.Hex()] = file
	})

	// 按req.FileIds中的ID顺序映射和转换
	resp.Files = lo.Map(req.FileIds, func(id string, _ int) *gencontent.File {
		if file, ok := fileMap[id]; ok {
			return convertor.FileMapperToFile(file)
		}
		return nil // 或者处理找不到文件的情况
	})
	return resp, nil
}
func (s *FileService) GetRecycleBinFiles(ctx context.Context, req *gencontent.GetRecycleBinFilesReq) (resp *gencontent.GetRecycleBinFilesResp, err error) {
	resp = new(gencontent.GetRecycleBinFilesResp)
	resp.FatherNamePath = "回收站"
	var (
		files []*filemapper.File
		total int64
		err2  error
	)

	if err = mr.Finish(func() error {
		getFileResp, err1 := s.GetFile(ctx, &gencontent.GetFileReq{
			FileId: req.GetFilterOptions().GetOnlyFatherId(),
		})
		if errors.Is(err1, consts.ErrNotFound) || errors.Is(err1, consts.ErrInvalidId) {
			resp.FatherIdPath = req.GetFilterOptions().GetOnlyFatherId()
			return nil
		}
		if err1 != nil {
			return err1
		}
		resp.FatherIdPath = getFileResp.Path
		paths := strings.Split(getFileResp.Path, "/")
		if len(paths) > 1 {
			var res *gencontent.GetFilesByIdsResp
			if res, err1 = s.GetFilesByIds(ctx, &gencontent.GetFilesByIdsReq{FileIds: paths[1:]}); err1 != nil {
				return err1
			}
			lo.ForEach(res.Files, func(item *gencontent.File, _ int) {
				resp.FatherNamePath += "/" + item.Name
			})
		}
		return nil
	}, func() error {
		filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
		p := convertor.ParsePagination(req.PaginationOptions)
		if req.SearchOption == nil {
			files, total, err2 = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, mongop.IdCursorType)
		} else {
			files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileAllFieldsSearchQuery(*req.SearchOption.SearchKeyword),
				filter, p, req.SearchOption)
		}
		if err2 != nil {
			return err
		}
		if p.LastToken != nil {
			resp.Token = *p.LastToken
		}
		resp.Total = total
		resp.Files = lo.Map[*filemapper.File, *gencontent.File](files, func(item *filemapper.File, _ int) *gencontent.File {
			return convertor.FileMapperToFile(item)
		})
		return nil
	}); err != nil {
		return resp, err
	}

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
		if errors.Is(err1, consts.ErrNotFound) || errors.Is(err1, consts.ErrInvalidId) {
			resp.FatherIdPath = req.GetFilterOptions().GetOnlyFatherId()
			return nil
		}
		if err1 != nil {
			return err1
		}
		resp.FatherIdPath = getFileResp.Path
		paths := strings.Split(getFileResp.Path, "/")
		if len(paths) > 1 {
			var res *gencontent.GetFilesByIdsResp
			if res, err1 = s.GetFilesByIds(ctx, &gencontent.GetFilesByIdsReq{FileIds: paths[1:]}); err1 != nil {
				return err1
			}
			lo.ForEach(res.Files, func(item *gencontent.File, _ int) {
				resp.FatherNamePath += "/" + item.Name
			})
		}
		return nil
	}, func() error {
		switch req.GetSortOptions() {
		case gencontent.SortOptions_SortOptions_createAtAsc:
			cursor = mongop.CreateAtAscCursorType
		case gencontent.SortOptions_SortOptions_createAtDesc:
			cursor = mongop.CreateAtDescCursorType
		case gencontent.SortOptions_SortOptions_updateAtAsc:
			cursor = mongop.UpdateAtAscCursorType
		case gencontent.SortOptions_SortOptions_updateAtDesc:
			cursor = mongop.UpdateAtDescCursorType
		case gencontent.SortOptions_SortOptions_NameDesc:
			cursor = mongop.NameDescCursorType
		case gencontent.SortOptions_SortOptions_NameAsc:
			cursor = mongop.NameAscCursorType
		case gencontent.SortOptions_SortOptions_TypeAsc:
			cursor = mongop.TypeAscCursorType
		case gencontent.SortOptions_SortOptions_TypeDesc:
			cursor = mongop.TypeDescCursorType
		}

		filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
		p := convertor.ParsePagination(req.PaginationOptions)
		if req.SearchOption == nil {
			files, total, err2 = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, cursor)
		} else {
			files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileAllFieldsSearchQuery(*req.SearchOption.SearchKeyword),
				filter, p, req.SearchOption)
		}
		if err2 != nil {
			return err
		}
		if p.LastToken != nil {
			resp.Token = *p.LastToken
		}
		resp.Total = total
		resp.Files = lo.Map[*filemapper.File, *gencontent.File](files, func(item *filemapper.File, _ int) *gencontent.File {
			return convertor.FileMapperToFile(item)
		})
		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) CheckShareFile(ctx context.Context, req *gencontent.CheckShareFileReq) (resp *gencontent.CheckShareFileResp, err error) {
	resp = new(gencontent.CheckShareFileResp)
	var (
		err1       error
		res        *gencontent.GetFileResp
		shareFiles []*filemapper.File
	)
	// 检查文件是否在分享文件夹中， 查询分享链接中的所有根文件
	if err = mr.Finish(func() error {
		if shareFiles, err1 = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err1 != nil {
			return err1
		}
		shareFiles = lo.Filter(shareFiles, func(item *filemapper.File, _ int) bool {
			switch item.IsDel {
			case int64(gencontent.Deletion_Deletion_notDel):
				return true
			default:
				return false
			}
		})
		return nil
	}, func() error {
		res, err1 = s.GetFile(ctx, &gencontent.GetFileReq{FileId: req.FileId, IsGetSize: false})
		return err1
	}); err != nil {
		return resp, err
	}
	for _, file := range shareFiles {
		if strings.HasPrefix(file.Path, res.Path) {
			resp.Ok = true
			break
		}
	}

	return resp, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error) {
	resp = new(gencontent.GetFileBySharingCodeResp)
	switch {
	case req.OnlyFatherId != nil:
		var res *gencontent.CheckShareFileResp
		var data *gencontent.GetFileListResp
		if res, err = s.CheckShareFile(ctx, &gencontent.CheckShareFileReq{FileIds: req.FileIds, FileId: *req.OnlyFatherId}); err != nil {
			return resp, err
		}
		if res.Ok {
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
	default:
		var shareFiles []*filemapper.File
		if shareFiles, err = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err != nil {
			return resp, err
		}
		resp.Files = lo.FilterMap[*filemapper.File, *gencontent.File](shareFiles, func(item *filemapper.File, _ int) (*gencontent.File, bool) {
			switch item.IsDel {
			case int64(gencontent.Deletion_Deletion_notDel):
				return convertor.FileMapperToFile(item), true
			default:
				return nil, false
			}
		})
		resp.Total = int64(len(shareFiles))
	}

	return resp, nil
}

func (s *FileService) GetFolderSize(ctx context.Context, path string) (resp int64, err error) {
	if resp, err = s.FileMongoMapper.FindFolderSize(ctx, path); err != nil {
		return 0, err
	}
	return resp, nil
}

func (s *FileService) CreateFile(ctx context.Context, req *gencontent.CreateFileReq) (resp *gencontent.CreateFileResp, err error) {
	resp = new(gencontent.CreateFileResp)
	resp.FileId, resp.Name, err = s.FileMongoMapper.Insert(ctx, &filemapper.File{
		ID:       primitive.NilObjectID,
		UserId:   req.UserId,
		Name:     req.Name,
		Category: req.Category,
		Type:     req.Type,
		Path:     req.Path,
		FatherId: req.FatherId,
		Size:     req.SpaceSize,
		FileMd5:  req.Md5,
		IsDel:    int64(gencontent.Deletion_Deletion_notDel),
	})
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error) {
	resp = new(gencontent.UpdateFileResp)
	var fileId primitive.ObjectID
	if fileId, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		return resp, err
	}
	data := &filemapper.File{
		ID:   fileId,
		Name: req.Name,
	}
	if _, err = s.FileMongoMapper.Update(ctx, data); err != nil {
		return resp, err
	}
	resp.Name = data.Name
	return resp, nil
}

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error) {
	resp = new(gencontent.MoveFileResp)
	var oid primitive.ObjectID
	if oid, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
		return resp, consts.ErrInvalidId
	}
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if req.SpaceSize == int64(gencontent.Folder_Folder_Size) { // 如果是文件夹
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + req.OldPath + "/"}} // 匹配该文件夹的所有子文件
			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
				return err
			}
			for _, v := range data {
				if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{ID: v.ID, Path: req.NewPath + v.Path[len(req.OldPath)-len(req.FileId)-1:]}); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					}
					return err
				}
			}
		}
		if _, err = s.FileMongoMapper.FindAndUpdate(sessionContext, &filemapper.File{ID: oid, Name: req.Name, Path: req.NewPath + "/" + oid.Hex(), FatherId: req.FatherId, IsDel: req.IsDel}); err != nil {
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
	ids := make([]string, 0, s.Config.InitialSliceLength)

	for _, file := range req.Files {
		ids = append(ids, file.FileId)
		if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(ctx, &data, filter); err != nil {
				return resp, err
			}
			for _, v := range data {
				ids = append(ids, v.ID.Hex())
			}
		}
	}

	if _, err = s.FileMongoMapper.DeleteMany(ctx, ids); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error) {
	resp = new(gencontent.DeleteFileResp)
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

	ids := make([]string, 0, s.Config.InitialSliceLength)
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		for _, file := range req.Files {
			ids = append(ids, file.FileId)
			if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
				var data []*filemapper.File
				filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}
				for _, v := range data {
					ids = append(ids, v.ID.Hex())
				}
			}
		}
		if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, update); err != nil {
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

func (s *FileService) EmptyRecycleBin(ctx context.Context, req *gencontent.EmptyRecycleBinReq) (resp *gencontent.EmptyRecycleBinResp, err error) {
	resp = new(gencontent.EmptyRecycleBinResp)
	var (
		err1, err2 error
		sortList   []*filemapper.File
		hardList   []*filemapper.File
	)

	if err = mr.Finish(func() error {
		sortList, err1 = s.FileMongoMapper.Find(ctx, bson.M{consts.UserId: req.UserId, consts.IsDel: int64(gencontent.Deletion_Deletion_softDel)})
		return err1
	}, func() error {
		hardList, err2 = s.FileMongoMapper.Find(ctx, bson.M{consts.UserId: req.UserId, consts.IsDel: int64(gencontent.Deletion_Deletion_hardDel)})
		return err2
	}); err != nil {
		return resp, err
	}

	ids := make([]string, len(sortList)+len(hardList))
	for i, v := range sortList {
		ids[i] = v.ID.Hex()
	}
	for i, v := range hardList {
		ids[i+len(sortList)] = v.ID.Hex()
	}

	if _, err = s.FileMongoMapper.DeleteMany(ctx, ids); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) RecoverRecycleBinFile(ctx context.Context, req *gencontent.RecoverRecycleBinFileReq) (resp *gencontent.RecoverRecycleBinFileResp, err error) {
	resp = new(gencontent.RecoverRecycleBinFileResp)
	update := bson.M{
		"$set":   bson.M{consts.IsDel: int64(gencontent.Deletion_Deletion_notDel)},
		"$unset": bson.M{consts.DeletedAt: ""},
	}

	ids := make([]string, 0, s.Config.InitialSliceLength)
	updates := make(map[string]bson.M)
	for _, file := range req.Files {
		paths := strings.Split(file.Path, "/")
		for i, id := range paths {
			if i == 0 {
				continue
			} else if i == 1 {
				var res *filemapper.File
				var old filemapper.File
				if res, err = s.FileMongoMapper.FindOne(ctx, id); err != nil {
					return resp, err
				}
				if err = s.FileMongoMapper.GetConn().FindOneNoCache(ctx, &old, bson.M{consts.FatherId: res.FatherId, consts.Name: res.Name, consts.IsDel: int64(gencontent.Deletion_Deletion_notDel)}); err != nil {
					if errors.Is(err, monc.ErrNotFound) {
						ids = append(ids, id)
						continue
					}
					return resp, err
				}

				s.FileMongoMapper.Rename(res)
				updates[id] = bson.M{
					"$set":   bson.M{consts.IsDel: int64(gencontent.Deletion_Deletion_notDel), consts.Name: res.Name},
					"$unset": bson.M{consts.DeletedAt: ""},
				}
				continue
			}
			ids = append(ids, id)
		}
	}

	for _, file := range req.Files {
		if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
			var data []*filemapper.File
			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
			if err = s.FileMongoMapper.GetConn().Find(ctx, &data, filter); err != nil {
				return resp, err
			}
			for _, v := range data {
				ids = append(ids, v.ID.Hex())
			}
		}
	}

	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		for key, v := range updates {
			oid, _ := primitive.ObjectIDFromHex(key)
			if _, err = s.FileMongoMapper.GetConn().UpdateOneNoCache(ctx, bson.M{consts.ID: oid}, v); err != nil {
				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
					log.CtxError(ctx, "恢复文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
				}
				return err
			}
		}

		if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, update); err != nil {
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
	if shareCodes, total, err = s.ShareFileMongoMapper.FindManyAndCount(ctx, convertor.ShareFileFilterOptionsToShareCodeOptions(req.FilterOptions),
		p, mongop.CreateAtDescCursorType); err != nil {
		return resp, err
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
	data := &sharefilemapper.ShareFile{
		ID:            primitive.NilObjectID,
		UserId:        req.UserId,
		Name:          req.Name,
		FileList:      req.FileList,
		EffectiveTime: req.EffectiveTime,
		BrowseNumber:  lo.ToPtr(int64(0)),
		CreateAt:      time.Now(),
	}
	if req.EffectiveTime >= 0 {
		data.DeletedAt = data.CreateAt.Add(time.Duration(req.EffectiveTime)*time.Second + time.Duration(s.Config.DeletionCoolingOffPeriod)*time.Hour)
	}
	if resp.Code, resp.Key, err = s.ShareFileMongoMapper.Insert(ctx, data); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *FileService) UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error) {
	var code primitive.ObjectID
	if code, err = primitive.ObjectIDFromHex(req.Code); err != nil {
		return resp, consts.ErrInvalidId
	}
	if _, err = s.ShareFileMongoMapper.Update(ctx, &sharefilemapper.ShareFile{
		ID:           code,
		Name:         req.Name,
		BrowseNumber: lo.ToPtr(req.BrowseNumber),
	}); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *FileService) DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (resp *gencontent.DeleteShareCodeResp, err error) {
	if _, err = s.ShareFileMongoMapper.Delete(ctx, req.Code); err != nil {
		log.CtxError(ctx, "删除文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (resp *gencontent.ParsingShareCodeResp, err error) {
	resp = new(gencontent.ParsingShareCodeResp)
	var shareFile *sharefilemapper.ShareFile
	if shareFile, err = s.ShareFileMongoMapper.FindOne(ctx, req.Code); err != nil {
		return resp, err
	}
	resp = &gencontent.ParsingShareCodeResp{
		UserId:        shareFile.UserId,
		Name:          shareFile.Name,
		Status:        convertor.IsExpired(shareFile.CreateAt, shareFile.EffectiveTime),
		EffectiveTime: shareFile.EffectiveTime,
		BrowseNumber:  *shareFile.BrowseNumber,
		CreateAt:      shareFile.CreateAt.UnixMilli(),
		FileList:      shareFile.FileList,
	}
	return resp, nil
}

func (s *FileService) SaveShareFileToPrivateSpace(ctx context.Context, req *gencontent.SaveShareFileToPrivateSpaceReq) (resp *gencontent.SaveShareFileToPrivateSpaceResp, err error) {
	resp = new(gencontent.SaveShareFileToPrivateSpaceResp)
	type kv struct {
		id   string
		path string
	}
	var err1 error
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err1 = sessionContext.StartTransaction(); err1 != nil {
			return err1
		}
		if resp.FileId, resp.Name, err1 = s.FileMongoMapper.FindAndInsert(sessionContext, &filemapper.File{ // 创建根文件
			UserId:   req.UserId,
			Name:     req.Name,
			Type:     req.Type,
			Path:     req.NewPath,
			FatherId: req.FatherId,
			Size:     req.SpaceSize,
			FileMd5:  req.FileMd5,
			IsDel:    int64(gencontent.Deletion_Deletion_notDel),
		}); err1 != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
			}
			return err1
		}

		if req.SpaceSize == int64(gencontent.Folder_Folder_Size) { // 若是文件夹，开始根据原文件夹层层创建
			var front kv
			queue := make([]kv, 0, s.Config.InitialSliceLength)
			queue = append(queue, kv{id: req.FileId, path: req.NewPath + "/" + resp.FileId})
			for len(queue) > 0 {
				front = queue[0]
				queue = queue[1:]
				var ids []string

				var (
					data   []*filemapper.File
					filter = bson.M{consts.FatherId: front.id}
				)

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
						ID:       primitive.NilObjectID,
						UserId:   req.UserId,
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
					if v.Size == int64(gencontent.Folder_Folder_Size) {
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

func (s *FileService) SavePublicFileToPrivateSpace(ctx context.Context, req *gencontent.SavePublicFileToPrivateSpaceReq) (resp *gencontent.SavePublicFileToPrivateSpaceResp, err error) {
	resp = new(gencontent.SavePublicFileToPrivateSpaceResp)
	type kv struct {
		id   string
		path string
	}
	var err1 error
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err1 = sessionContext.StartTransaction(); err1 != nil {
			return err1
		}
		if resp.FileId, resp.Name, err1 = s.FileMongoMapper.FindAndInsert(sessionContext, &filemapper.File{ // 创建根文件
			UserId:   req.UserId,
			Name:     req.Name,
			Type:     req.Type,
			Path:     req.NewPath,
			FatherId: req.FatherId,
			Size:     req.SpaceSize,
			FileMd5:  req.FileMd5,
			IsDel:    int64(gencontent.Deletion_Deletion_notDel),
		}); err1 != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
			}
			return err1
		}

		if req.SpaceSize == int64(gencontent.Folder_Folder_Size) { // 若是文件夹，开始根据原文件夹层层创建
			var front kv
			queue := make([]kv, 0, s.Config.InitialSliceLength)
			queue = append(queue, kv{id: req.FileId, path: req.NewPath + "/" + resp.FileId})
			for len(queue) > 0 {
				front = queue[0]
				queue = queue[1:]
				var ids []string
				var (
					data   []*publicfilemapper.PublicFile
					filter = bson.M{consts.Zone: front.id}
				)
				if err1 = s.PublicFileMongoMapper.GetConn().Find(sessionContext, &data, filter, &options.FindOptions{BatchSize: lo.ToPtr(int32(100))}); err1 != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
						return rbErr
					}
				}

				if len(data) <= 0 {
					continue
				}

				sonFiles := lo.Map(data, func(item *publicfilemapper.PublicFile, _ int) *filemapper.File {
					return &filemapper.File{
						ID:       primitive.NilObjectID,
						UserId:   req.UserId,
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
					if v.Size == int64(gencontent.Folder_Folder_Size) {
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
