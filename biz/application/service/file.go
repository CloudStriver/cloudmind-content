package service

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/kq"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	sharefilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/sharefile"
	"github.com/CloudStriver/cloudmind-mq/app/util/message"
	"github.com/CloudStriver/go-pkg/utils/pconvertor"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/bytedance/sonic"
	"github.com/google/wire"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/core/stores/monc"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
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
	SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (resp *gencontent.SaveFileToPrivateSpaceResp, err error)
	AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error)
	MakeFilePrivate(ctx context.Context, req *gencontent.MakeFilePrivateReq) (resp *gencontent.MakeFilePrivateResp, err error)
}

type FileService struct {
	Config               *config.Config
	FileMongoMapper      filemapper.IMongoMapper
	FileEsMapper         filemapper.IFileEsMapper
	ShareFileMongoMapper sharefilemapper.IMongoMapper
	DeleteFileRelationKq *kq.DeleteFileRelationKq
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
	//var file *filemapper.File
	//if file, err = s.FileMongoMapper.FindOne(ctx, req.FileId); err != nil {
	//	return resp, err
	//}
	//resp.File = convertor.FileMapperToFile(file)
	//
	//// 如果是获取文件夹大小，需要计算文件夹大小
	//if req.IsGetSize && resp.File.SpaceSize == int64(gencontent.Folder_Folder_Size) {
	//	if resp.File.SpaceSize, err = s.GetFolderSize(ctx, resp.File.Path); err != nil {
	//		return resp, err
	//	}
	//}
	return resp, nil
}

func (s *FileService) GetFilesByIds(ctx context.Context, req *gencontent.GetFilesByIdsReq) (resp *gencontent.GetFilesByIdsResp, err error) {
	resp = new(gencontent.GetFilesByIdsResp)
	//var files []*filemapper.File
	//if files, err = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err != nil {
	//	return resp, err
	//}
	//
	//// 创建映射：文件ID到文件
	//fileMap := make(map[string]*filemapper.File, len(req.FileIds))
	//lo.ForEach(files, func(file *filemapper.File, _ int) {
	//	fileMap[file.ID.Hex()] = file
	//})
	//
	//// 按req.FileIds中的ID顺序映射和转换
	//resp.Files = lo.Map(req.FileIds, func(id string, _ int) *gencontent.FileInfo {
	//	if file, ok := fileMap[id]; ok {
	//		return convertor.FileMapperToFile(file)
	//	}
	//	return nil // 或者处理找不到文件的情况
	//})
	return resp, nil
}
func (s *FileService) GetRecycleBinFiles(ctx context.Context, req *gencontent.GetRecycleBinFilesReq) (resp *gencontent.GetRecycleBinFilesResp, err error) {
	resp = new(gencontent.GetRecycleBinFilesResp)
	//var (
	//	total int64
	//	files []*filemapper.File
	//)
	//p := convertor.ParsePagination(req.PaginationOptions)
	//if files, total, err = s.FileMongoMapper.FindManyAndCount(ctx, convertor.FileFilterOptionsToFilterOptions(req.FilterOptions),
	//	p, mongop.CreateAtDescCursorType); err != nil {
	//	return resp, err
	//}
	//if p.LastToken != nil {
	//	resp.Token = *p.LastToken
	//}
	//resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](files, func(item *filemapper.File, _ int) *gencontent.FileInfo {
	//	return convertor.FileMapperToFile(item)
	//})
	//resp.Total = total
	return resp, nil
}

func (s *FileService) GetFileList(ctx context.Context, req *gencontent.GetFileListReq) (resp *gencontent.GetFileListResp, err error) {
	resp = new(gencontent.GetFileListResp)
	//resp.FatherNamePath = "CloudMind"
	//var (
	//	files  []*filemapper.File
	//	total  int64
	//	cursor mongop.MongoCursor
	//	err2   error
	//)
	//
	//if err = mr.Finish(func() error {
	//	getFileResp, err1 := s.GetFile(ctx, &gencontent.GetFileReq{
	//		//FileId: req.GetFilterOptions().GetOnlyFatherId(),
	//	})
	//	if errors.Is(err1, consts.ErrNotFound) || errors.Is(err1, consts.ErrInvalidId) {
	//		resp.FatherIdPath = req.GetFilterOptions().GetOnlyFatherId()
	//		return nil
	//	}
	//	if err1 != nil {
	//		return err1
	//	}
	//	//resp.FatherIdPath = getFileResp.File.Path
	//	//paths := strings.Split(getFileResp.File.Path, "/")
	//	if len(paths) > 1 {
	//		var res *gencontent.GetFilesByIdsResp
	//		if res, err1 = s.GetFilesByIds(ctx, &gencontent.GetFilesByIdsReq{FileIds: paths[1:]}); err1 != nil {
	//			return err1
	//		}
	//		lo.ForEach(res.Files, func(item *gencontent.FileInfo, _ int) {
	//			resp.FatherNamePath += "/" + item.Name
	//		})
	//	}
	//	return nil
	//}, func() error {
	//	switch req.GetSortOptions() {
	//	case gencontent.SortOptions_SortOptions_createAtAsc:
	//		cursor = mongop.CreateAtAscCursorType
	//	case gencontent.SortOptions_SortOptions_createAtDesc:
	//		cursor = mongop.CreateAtDescCursorType
	//	case gencontent.SortOptions_SortOptions_updateAtAsc:
	//		cursor = mongop.UpdateAtAscCursorType
	//	case gencontent.SortOptions_SortOptions_updateAtDesc:
	//		cursor = mongop.UpdateAtDescCursorType
	//	case gencontent.SortOptions_SortOptions_NameDesc:
	//		cursor = mongop.NameDescCursorType
	//	case gencontent.SortOptions_SortOptions_NameAsc:
	//		cursor = mongop.NameAscCursorType
	//	case gencontent.SortOptions_SortOptions_TypeAsc:
	//		cursor = mongop.TypeAscCursorType
	//	case gencontent.SortOptions_SortOptions_TypeDesc:
	//		cursor = mongop.TypeDescCursorType
	//	}
	//
	//	filter := convertor.FileFilterOptionsToFilterOptions(req.FilterOptions)
	//	p := convertor.ParsePagination(req.PaginationOptions)
	//	if req.SearchOptions == nil {
	//		if files, total, err2 = s.FileMongoMapper.FindManyAndCount(ctx, filter, p, cursor); err2 != nil {
	//			return err2
	//		}
	//	} else {
	//		switch o := req.SearchOptions.Type.(type) {
	//		case *gencontent.SearchOptions_AllFieldsKey:
	//			files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileAllFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
	//		case *gencontent.SearchOptions_MultiFieldsKey:
	//			files, total, err2 = s.FileEsMapper.Search(ctx, convertor.ConvertFileMultiFieldsSearchQuery(o), filter, p, esp.ScoreCursorType)
	//		}
	//		if err2 != nil {
	//			log.CtxError(ctx, "搜索文件列表异常[%v]\n", err2)
	//			return err2
	//		}
	//	}
	//
	//	if p.LastToken != nil {
	//		resp.Token = *p.LastToken
	//	}
	//	resp.Total = total
	//	resp.Files = lo.Map[*filemapper.File, *gencontent.FileInfo](files, func(item *filemapper.File, _ int) *gencontent.FileInfo {
	//		return convertor.FileMapperToFile(item)
	//	})
	//	return nil
	//}); err != nil {
	//	return resp, err
	//}

	return resp, nil
}

func (s *FileService) CheckShareFile(ctx context.Context, req *gencontent.CheckShareFileReq) (resp *gencontent.CheckShareFileResp, err error) {
	resp = new(gencontent.CheckShareFileResp)
	//var (
	//	err1       error
	//	res        *gencontent.GetFileResp
	//	shareFiles []*filemapper.File
	//)
	//// 检查文件是否在分享文件夹中， 查询分享链接中的所有根文件
	//if err = mr.Finish(func() error {
	//	if shareFiles, err1 = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err1 != nil {
	//		return err1
	//	}
	//	shareFiles = lo.Filter(shareFiles, func(item *filemapper.File, _ int) bool {
	//		switch item.IsDel {
	//		case int64(gencontent.Deletion_Deletion_notDel):
	//			return true
	//		default:
	//			return false
	//		}
	//	})
	//	return nil
	//}, func() error {
	//	res, err1 = s.GetFile(ctx, &gencontent.GetFileReq{FileId: req.FileId, IsGetSize: false})
	//	return err1
	//}); err != nil {
	//	return resp, err
	//}
	//for _, file := range shareFiles {
	//	if strings.HasPrefix(file.Path, res.File.Path) {
	//		resp.Ok = true
	//		break
	//	}
	//}

	return resp, nil
}

func (s *FileService) GetFileBySharingCode(ctx context.Context, req *gencontent.GetFileBySharingCodeReq) (resp *gencontent.GetFileBySharingCodeResp, err error) {
	resp = new(gencontent.GetFileBySharingCodeResp)
	//switch {
	//case req.OnlyFatherId != nil:
	//	var res *gencontent.CheckShareFileResp
	//	var data *gencontent.GetFileListResp
	//	if res, err = s.CheckShareFile(ctx, &gencontent.CheckShareFileReq{FileIds: req.FileIds, FileId: *req.OnlyFatherId}); err != nil {
	//		return resp, err
	//	}
	//	if res.Ok {
	//		if data, err = s.GetFileList(ctx, &gencontent.GetFileListReq{
	//			FilterOptions:     &gencontent.FileFilterOptions{OnlyFatherId: req.OnlyFatherId, OnlyIsDel: lo.ToPtr(int64(gencontent.Deletion_Deletion_notDel))},
	//			PaginationOptions: req.PaginationOptions,
	//			SortOptions:       req.SortOptions,
	//		}); err != nil {
	//			return resp, err
	//		}
	//		resp.Files = data.Files
	//		resp.Total = data.Total
	//		resp.Token = data.Token
	//		resp.FatherIdPath = data.FatherIdPath
	//		resp.FatherNamePath = data.FatherNamePath
	//	}
	//default:
	//	var shareFiles []*filemapper.File
	//	if shareFiles, err = s.FileMongoMapper.FindManyByIds(ctx, req.FileIds); err != nil {
	//		return resp, err
	//	}
	//	resp.Files = lo.FilterMap[*filemapper.File, *gencontent.FileInfo](shareFiles, func(item *filemapper.File, _ int) (*gencontent.FileInfo, bool) {
	//		switch item.IsDel {
	//		case int64(gencontent.Deletion_Deletion_notDel):
	//			return convertor.FileMapperToFile(item), true
	//		default:
	//			return nil, false
	//		}
	//	})
	//	resp.Total = int64(len(shareFiles))
	//}

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
	//resp.FileId, resp.Name, err = s.FileMongoMapper.Insert(ctx, convertor.FileToFileMapper(req.File))
	//if err != nil {
	//	return resp, err
	//}
	return resp, nil
}

func (s *FileService) UpdateFile(ctx context.Context, req *gencontent.UpdateFileReq) (resp *gencontent.UpdateFileResp, err error) {
	resp = new(gencontent.UpdateFileResp)
	//data := convertor.FileToFileMapper(req.File)
	//if _, err = s.FileMongoMapper.Update(ctx, data); err != nil {
	//	return resp, err
	//}
	//resp.Name = data.Name
	return resp, nil
}

func (s *FileService) MoveFile(ctx context.Context, req *gencontent.MoveFileReq) (resp *gencontent.MoveFileResp, err error) {
	resp = new(gencontent.MoveFileResp)
	//var oid primitive.ObjectID
	//if oid, err = primitive.ObjectIDFromHex(req.FileId); err != nil {
	//	return resp, consts.ErrInvalidId
	//}
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err = sessionContext.StartTransaction(); err != nil {
	//		return err
	//	}
	//	if req.SpaceSize == int64(gencontent.Folder_Folder_Size) { // 如果是文件夹
	//		var data []*filemapper.File
	//		filter := bson.M{"path": bson.M{"$regex": "^" + req.OldPath + "/"}} // 匹配该文件夹的所有子文件
	//		if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
	//			return err
	//		}
	//		for _, v := range data {
	//			if _, err = s.FileMongoMapper.Update(sessionContext, &filemapper.File{ID: v.ID, Path: req.NewPath + v.Path[len(req.OldPath)-len(req.FileId)-1:]}); err != nil {
	//				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//					log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//				}
	//				return err
	//			}
	//		}
	//	}
	//	if _, err = s.FileMongoMapper.FindAndUpdate(sessionContext, &filemapper.File{ID: oid, Name: req.Name, Path: req.NewPath + "/" + oid.Hex(), FatherId: req.FatherId, IsDel: req.IsDel}); err != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "移动文件中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//		}
	//		return err
	//	}
	//	if err = sessionContext.CommitTransaction(sessionContext); err != nil {
	//		log.CtxError(ctx, "移动文件: 提交事务异常[%v]\n", err)
	//		return err
	//	}
	//	return nil
	//})

	return resp, err
}

func (s *FileService) CompletelyRemoveFile(ctx context.Context, req *gencontent.CompletelyRemoveFileReq) (resp *gencontent.CompletelyRemoveFileResp, err error) {
	resp = new(gencontent.CompletelyRemoveFileResp)
	//ids := make([]string, 0, s.Config.InitialSliceLength)
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err = sessionContext.StartTransaction(); err != nil {
	//		return err
	//	}
	//	for _, file := range req.Files {
	//		ids = append(ids, file.FileId)
	//		if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
	//			var data []*filemapper.File
	//			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
	//			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
	//				return err
	//			}
	//			for _, v := range data {
	//				ids = append(ids, v.ID.Hex())
	//			}
	//		}
	//	}
	//	if _, err = s.FileMongoMapper.DeleteMany(sessionContext, ids); err != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//		}
	//		return err
	//	}
	//	if err = sessionContext.CommitTransaction(sessionContext); err != nil {
	//		log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
	//		return err
	//	}
	//	return nil
	//})
	//
	//data, _ := sonic.Marshal(&message.DeleteFileRelationsMessage{
	//	FromType: int64(gencontent.TargetType_UserType),
	//	FromIds:  ids,
	//})
	//
	//if err2 := s.DeleteFileRelationKq.Push(pconvertor.Bytes2String(data)); err2 != nil {
	//	return resp, err2
	//}

	return resp, nil
}

func (s *FileService) DeleteFile(ctx context.Context, req *gencontent.DeleteFileReq) (resp *gencontent.DeleteFileResp, err error) {
	resp = new(gencontent.DeleteFileResp)
	//update := bson.M{}
	//switch req.DeleteType {
	//case int64(gencontent.Deletion_Deletion_softDel):
	//	update["$set"] = bson.M{
	//		consts.IsDel:     int64(gencontent.Deletion_Deletion_softDel),
	//		consts.DeletedAt: time.Now(),
	//	}
	//case int64(gencontent.Deletion_Deletion_hardDel):
	//	update["$set"] = bson.M{
	//		consts.IsDel: int64(gencontent.Deletion_Deletion_hardDel),
	//	}
	//default:
	//	return resp, consts.ErrInvalidDeleteType
	//}
	//if req.ClearCommunity || req.DeleteType == int64(gencontent.Deletion_Deletion_hardDel) {
	//	update["$unset"] = bson.M{
	//		consts.Zone:        "",
	//		consts.SubZone:     "",
	//		consts.Description: "",
	//		consts.Labels:      "",
	//	}
	//}
	//ids := make([]string, 0, s.Config.InitialSliceLength)
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err = sessionContext.StartTransaction(); err != nil {
	//		return err
	//	}
	//	for _, file := range req.Files {
	//		ids = append(ids, file.FileId)
	//		if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
	//			var data []*filemapper.File
	//			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
	//			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
	//				return err
	//			}
	//			for _, v := range data {
	//				ids = append(ids, v.ID.Hex())
	//			}
	//		}
	//	}
	//	if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, update); err != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//		}
	//		return err
	//	}
	//	if err = sessionContext.CommitTransaction(sessionContext); err != nil {
	//		log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
	//		return err
	//	}
	//	return nil
	//})
	//
	//if req.DeleteType == int64(gencontent.Deletion_Deletion_hardDel) || req.ClearCommunity {
	//	data, _ := sonic.Marshal(&message.DeleteFileRelationsMessage{
	//		FromType: int64(gencontent.TargetType_UserType),
	//		FromIds:  ids,
	//	})
	//
	//	if err2 := s.DeleteFileRelationKq.Push(pconvertor.Bytes2String(data)); err2 != nil {
	//		return resp, err2
	//	}
	//}

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

	n := len(sortList)
	m := len(hardList)
	ids := make([]string, n+m)
	for i, v := range sortList {
		ids[i] = v.ID.Hex()
	}
	for i, v := range hardList {
		ids[i+n] = v.ID.Hex()
	}
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		if _, err = s.FileMongoMapper.DeleteMany(sessionContext, ids); err != nil {
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

	data, _ := sonic.Marshal(&message.DeleteFileRelationsMessage{
		FromType: int64(gencontent.TargetType_UserType),
		FromIds:  ids,
	})

	if err2 = s.DeleteFileRelationKq.Push(pconvertor.Bytes2String(data)); err2 != nil {
		return resp, err2
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
	tx := s.FileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}
		for _, file := range req.Files {
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
	//var (
	//	shareCodes []*sharefilemapper.ShareFile
	//	total      int64
	//)
	//p := convertor.ParsePagination(req.PaginationOptions)
	//if shareCodes, total, err = s.ShareFileMongoMapper.FindManyAndCount(ctx, convertor.ShareFileFilterOptionsToShareCodeOptions(req.ShareFileFilterOptions),
	//	p, mongop.CreateAtDescCursorType); err != nil {
	//	return resp, err
	//}
	//
	//if p.LastToken != nil {
	//	resp.Token = *p.LastToken
	//}
	//resp.ShareCodes = lo.Map[*sharefilemapper.ShareFile, *gencontent.ShareCode](shareCodes, func(item *sharefilemapper.ShareFile, _ int) *gencontent.ShareCode {
	//	return convertor.ShareFileToShareCode(item)
	//})
	//resp.Total = total
	return resp, nil
}

func (s *FileService) CreateShareCode(ctx context.Context, req *gencontent.CreateShareCodeReq) (resp *gencontent.CreateShareCodeResp, err error) {
	resp = new(gencontent.CreateShareCodeResp)
	//data := convertor.ShareFileToShareFileMapper(req.ShareFile)
	//data.CreateAt = time.Now()
	//if req.ShareFile.EffectiveTime >= 0 {
	//	data.DeletedAt = data.CreateAt.Add(time.Duration(req.ShareFile.EffectiveTime)*time.Second + time.Duration(s.Config.DeletionCoolingOffPeriod)*time.Hour)
	//}
	//if resp.Code, resp.Key, err = s.ShareFileMongoMapper.Insert(ctx, data); err != nil {
	//	return resp, err
	//}

	return resp, nil
}

func (s *FileService) UpdateShareCode(ctx context.Context, req *gencontent.UpdateShareCodeReq) (resp *gencontent.UpdateShareCodeResp, err error) {
	//data := convertor.ShareFileToShareFileMapper(req.ShareFile)
	//if _, err = s.ShareFileMongoMapper.Update(ctx, data); err != nil {
	//	return resp, err
	//}
	return resp, nil
}

func (s *FileService) DeleteShareCode(ctx context.Context, req *gencontent.DeleteShareCodeReq) (resp *gencontent.DeleteShareCodeResp, err error) {
	if _, err := s.ShareFileMongoMapper.Delete(ctx, req.Code); err != nil {
		log.CtxError(ctx, "删除文件分享链接: 发生异常[%v]\n", err)
		return resp, err
	}
	return resp, nil
}

func (s *FileService) ParsingShareCode(ctx context.Context, req *gencontent.ParsingShareCodeReq) (resp *gencontent.ParsingShareCodeResp, err error) {
	resp = new(gencontent.ParsingShareCodeResp)
	//var shareFile *sharefilemapper.ShareFile
	//if shareFile, err = s.ShareFileMongoMapper.FindOne(ctx, req.Code); err != nil {
	//	return resp, err
	//}
	//res := convertor.ShareFileMapperToShareFile(shareFile)
	//resp.ShareFile = res
	return resp, nil
}

func (s *FileService) SaveFileToPrivateSpace(ctx context.Context, req *gencontent.SaveFileToPrivateSpaceReq) (resp *gencontent.SaveFileToPrivateSpaceResp, err error) {
	resp = new(gencontent.SaveFileToPrivateSpaceResp)
	//type kv struct {
	//	id   string
	//	path string
	//}
	//var err1 error
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err1 = sessionContext.StartTransaction(); err1 != nil {
	//		return err1
	//	}
	//	if resp.FileId, resp.Name, err1 = s.FileMongoMapper.FindAndInsert(sessionContext, &filemapper.File{ // 创建根文件
	//		UserId:   req.UserId,
	//		Name:     req.Name,
	//		Type:     req.Type,
	//		Path:     req.NewPath,
	//		FatherId: req.FatherId,
	//		Size:     req.SpaceSize,
	//		FileMd5:  req.FileMd5,
	//		IsDel:    int64(gencontent.Deletion_Deletion_notDel),
	//	}); err1 != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
	//		}
	//		return err1
	//	}
	//	if req.SpaceSize == int64(gencontent.Folder_Folder_Size) { // 若是文件夹，开始根据原文件夹层层创建
	//		var front kv
	//		queue := make([]kv, 0, s.Config.InitialSliceLength)
	//		queue = append(queue, kv{id: req.FileId, path: req.NewPath + "/" + resp.FileId})
	//		for len(queue) > 0 {
	//			front = queue[0]
	//			queue = queue[1:]
	//			var ids []string
	//			var data []*filemapper.File
	//			var filter bson.M
	//			if req.DocumentType == int64(gencontent.Space_Space_public) {
	//				filter = bson.M{consts.FatherId: front.id, consts.Zone: bson.M{"$exists": true, "$ne": ""}, consts.SubZone: bson.M{"$exists": true, "$ne": ""}}
	//			} else if req.DocumentType == int64(gencontent.Space_Space_private) {
	//				filter = bson.M{consts.FatherId: front.id}
	//			} else {
	//				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//					log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", "访问未知空间", rbErr)
	//				}
	//				return consts.ErrIllegalOperation
	//			}
	//
	//			if err1 = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter, &options.FindOptions{BatchSize: lo.ToPtr(int32(100))}); err1 != nil {
	//				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//					log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
	//					return rbErr
	//				}
	//			}
	//
	//			if len(data) <= 0 {
	//				continue
	//			}
	//
	//			sonFiles := lo.Map(data, func(item *filemapper.File, _ int) *filemapper.File {
	//				return &filemapper.File{
	//					UserId:   req.UserId,
	//					Name:     item.Name,
	//					Type:     item.Type,
	//					Path:     front.path,
	//					FatherId: front.path[len(front.path)-len(front.id):],
	//					Size:     item.Size,
	//					FileMd5:  item.FileMd5,
	//					IsDel:    int64(gencontent.Deletion_Deletion_notDel),
	//				}
	//			})
	//
	//			if ids, err1 = s.FileMongoMapper.FindAndInsertMany(sessionContext, sonFiles); err1 != nil {
	//				if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//					log.CtxError(ctx, "保存文件中产生错误[%v]: 回滚异常[%v]\n", err1, rbErr)
	//				}
	//				return err1
	//			}
	//
	//			for i, v := range data {
	//				if v.Size == int64(gencontent.Folder_Folder_Size) {
	//					queue = append(queue, kv{id: v.ID.Hex(), path: front.path + "/" + ids[i]})
	//				}
	//			}
	//		}
	//	}
	//
	//	if err1 = sessionContext.CommitTransaction(sessionContext); err1 != nil {
	//		log.CtxError(ctx, "保存文件: 提交事务异常[%v]\n", err1)
	//		return err1
	//	}
	//	return nil
	//})

	return resp, err
}

func (s *FileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error) {
	resp = new(gencontent.AddFileToPublicSpaceResp)
	//update := bson.M{
	//	"$set": bson.M{
	//		consts.AuditStatus: int64(gencontent.AuditStatus_AuditStatus_wait),
	//		consts.Zone:        req.Zone,
	//		consts.SubZone:     req.SubZone,
	//		consts.Description: req.Description,
	//		consts.Labels:      req.Labels,
	//	},
	//}
	//ids := make([]string, 0, s.Config.InitialSliceLength)
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err = sessionContext.StartTransaction(); err != nil {
	//		return err
	//	}
	//	ids = append(ids, req.FileId)
	//	if req.SpaceSize == int64(gencontent.Folder_Folder_Size) {
	//		var data []*filemapper.File
	//		filter := bson.M{consts.Path: bson.M{"$regex": "^" + req.Path + "/"}}
	//		if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
	//			return err
	//		}
	//		for _, v := range data {
	//			ids = append(ids, v.ID.Hex())
	//		}
	//	}
	//	if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, update); err != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//		}
	//		return err
	//	}
	//	if err = sessionContext.CommitTransaction(sessionContext); err != nil {
	//		log.CtxError(ctx, "上传文件到社区: 提交事务异常[%v]\n", err)
	//		return err
	//	}
	//	return nil
	//})
	//resp.FileIds = ids
	return resp, err
}

func (s *FileService) MakeFilePrivate(ctx context.Context, req *gencontent.MakeFilePrivateReq) (resp *gencontent.MakeFilePrivateResp, err error) {
	resp = &gencontent.MakeFilePrivateResp{}
	//data := convertor.FileToFileMapper(&gencontent.File{FileId: req.FileId, AuditStatus: int64(gencontent.AuditStatus_AuditStatus_notStart)})
	//update := bson.M{
	//	consts.Zone:        "",
	//	consts.SubZone:     "",
	//	consts.Description: "",
	//	consts.Labels:      "",
	//}

	//ids := make([]string, 0, s.Config.InitialSliceLength)
	//tx := s.FileMongoMapper.StartClient()
	//err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
	//	if err = sessionContext.StartTransaction(); err != nil {
	//		return err
	//	}
	//	for _, file := range req.Files {
	//		ids = append(ids, file.FileId)
	//		if file.SpaceSize == int64(gencontent.Folder_Folder_Size) {
	//			var data []*filemapper.File
	//			filter := bson.M{"path": bson.M{"$regex": "^" + file.Path + "/"}}
	//			if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
	//				return err
	//			}
	//			for _, v := range data {
	//				ids = append(ids, v.ID.Hex())
	//			}
	//		}
	//	}
	//	if _, err = s.FileMongoMapper.UpdateMany(sessionContext, ids, update); err != nil {
	//		if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
	//			log.CtxError(ctx, "删除文件过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
	//		}
	//		return err
	//	}
	//	if err = sessionContext.CommitTransaction(sessionContext); err != nil {
	//		log.CtxError(ctx, "删除文件: 提交事务异常[%v]\n", err)
	//		return err
	//	}
	//	return nil
	//})
	//
	//if _, err = s.FileMongoMapper.UpdateUnset(ctx, data, update); err != nil {
	//	return resp, err
	//}
	//
	//res, _ := sonic.Marshal(&message.DeleteFileRelationsMessage{
	//	FromType: int64(gencontent.TargetType_UserType),
	//	FromId:   req.UserId,
	//	ToType:   int64(gencontent.TargetType_FileType),
	//})
	//if err2 := s.DeleteFileRelationKq.Push(pconvertor.Bytes2String(res)); err2 != nil {
	//	return resp, err2
	//}

	return resp, nil
}
