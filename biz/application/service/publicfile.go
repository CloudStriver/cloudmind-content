package service

import (
	"context"
	"errors"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/config"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/convertor"
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/kq"
	filemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/file"
	publicfilemapper "github.com/CloudStriver/cloudmind-content/biz/infrastructure/mapper/publicfile"
	"github.com/CloudStriver/go-pkg/utils/pagination/mongop"
	"github.com/CloudStriver/go-pkg/utils/util/log"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/google/wire"
	"github.com/samber/lo"
	"github.com/zeromicro/go-zero/core/mr"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

type IPublicFileService interface {
	GetFolderSize(ctx context.Context, path string) (resp int64, err error)
	GetPublicFile(ctx context.Context, req *gencontent.GetPublicFileReq) (resp *gencontent.GetPublicFileResp, err error)
	GetPublicFileByIds(ctx context.Context, req *gencontent.GetPublicFilesByIdsReq) (resp *gencontent.GetPublicFilesByIdsResp, err error)
	GetPublicFileList(ctx context.Context, req *gencontent.GetPublicFileListReq) (resp *gencontent.GetPublicFileListResp, err error)
	UpdatePublicFile(ctx context.Context, req *gencontent.UpdatePublicFileReq) (resp *gencontent.UpdatePublicFileResp, err error)
	MakeFilePrivate(ctx context.Context, req *gencontent.MakeFilePrivateReq) (resp *gencontent.MakeFilePrivateResp, err error)
	AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error)
}

type PublicFileService struct {
	Config                *config.Config
	PublicFileMongoMapper publicfilemapper.IMongoMapper
	PublicFileEsMapper    publicfilemapper.IEsMapper
	FileMongoMapper       filemapper.IMongoMapper
	DeleteFileRelationKq  *kq.DeleteFileRelationKq
}

var PublicFileSet = wire.NewSet(
	wire.Struct(new(PublicFileService), "*"),
	wire.Bind(new(IPublicFileService), new(*PublicFileService)),
)

func (s *PublicFileService) GetFolderSize(ctx context.Context, path string) (resp int64, err error) {
	if resp, err = s.PublicFileMongoMapper.FindFolderSize(ctx, path); err != nil {
		return 0, err
	}
	return resp, nil
}

func (s *PublicFileService) GetPublicFile(ctx context.Context, req *gencontent.GetPublicFileReq) (resp *gencontent.GetPublicFileResp, err error) {
	resp = new(gencontent.GetPublicFileResp)
	var file *publicfilemapper.PublicFile
	if file, err = s.PublicFileMongoMapper.FindOne(ctx, req.Id); err != nil {
		return resp, err
	}
	resp = &gencontent.GetPublicFileResp{
		UserId:      file.UserId,
		Name:        file.Name,
		Type:        file.Type,
		SpaceSize:   file.Size,
		Md5:         file.FileMd5,
		Zone:        file.Zone,
		Description: file.Description,
		Labels:      file.Labels,
		AuditStatus: file.AuditStatus,
		CreateAt:    file.CreateAt.UnixMilli(),
	}

	// 如果是获取文件夹大小，需要计算文件夹大小
	if req.IsGetSize && resp.SpaceSize == int64(gencontent.Folder_Folder_Size) {
		if resp.SpaceSize, err = s.GetFolderSize(ctx, resp.Path); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (s *PublicFileService) GetPublicFileByIds(ctx context.Context, req *gencontent.GetPublicFilesByIdsReq) (resp *gencontent.GetPublicFilesByIdsResp, err error) {
	resp = new(gencontent.GetPublicFilesByIdsResp)
	var files []*publicfilemapper.PublicFile
	if files, err = s.PublicFileMongoMapper.FindManyByIds(ctx, req.Ids); err != nil {
		return resp, err
	}

	// 创建映射：文件ID到文件
	fileMap := make(map[string]*publicfilemapper.PublicFile, len(req.Ids))
	lo.ForEach(files, func(file *publicfilemapper.PublicFile, _ int) {
		fileMap[file.ID.Hex()] = file
	})

	// 按req.FileIds中的ID顺序映射和转换
	resp.Files = lo.Map(req.Ids, func(id string, _ int) *gencontent.PublicFile {
		if file, ok := fileMap[id]; ok {
			return convertor.PublicFileMapperToPublicFile(file)
		}
		return nil // 或者处理找不到文件的情况
	})
	return resp, nil
}

func (s *PublicFileService) GetPublicFileList(ctx context.Context, req *gencontent.GetPublicFileListReq) (resp *gencontent.GetPublicFileListResp, err error) {
	resp = new(gencontent.GetPublicFileListResp)
	resp.FatherNamePath = "CloudMind"

	var (
		files  []*publicfilemapper.PublicFile
		total  int64
		cursor mongop.MongoCursor
	)

	if err = mr.Finish(func() error {
		getFileResp, err1 := s.GetPublicFile(ctx, &gencontent.GetPublicFileReq{
			Id: req.GetFilterOptions().GetOnlyZone(),
		})
		if errors.Is(err1, consts.ErrNotFound) || errors.Is(err1, consts.ErrInvalidId) {
			resp.FatherIdPath = req.GetFilterOptions().GetOnlyZone()
			return nil
		}
		if err1 != nil {
			return err1
		}
		resp.FatherIdPath = getFileResp.Path
		paths := strings.Split(getFileResp.Path, "/")
		if len(paths) > 1 {
			var res *gencontent.GetPublicFilesByIdsResp
			if res, err1 = s.GetPublicFileByIds(ctx, &gencontent.GetPublicFilesByIdsReq{Ids: paths[1:]}); err1 != nil {
				return err1
			}
			lo.ForEach(res.Files, func(item *gencontent.PublicFile, _ int) {
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

		filter := convertor.PublicFilterOptionsToFilterOptions(req.FilterOptions)
		p := convertor.ParsePagination(req.PaginationOptions)
		if req.SearchOption == nil {
			files, total, err = s.PublicFileMongoMapper.FindManyAndCount(ctx, filter, p, cursor)
		} else {
			files, total, err = s.PublicFileEsMapper.Search(ctx, convertor.ConvertPublicFileAllFieldsSearchQuery(*req.SearchOption.SearchKeyword),
				filter, p, req.SearchOption)
		}
		if err != nil {
			return err
		}
		if p.LastToken != nil {
			resp.Token = *p.LastToken
		}
		resp.Total = total
		resp.Files = lo.Map[*publicfilemapper.PublicFile, *gencontent.PublicFile](files, func(item *publicfilemapper.PublicFile, _ int) *gencontent.PublicFile {
			return convertor.PublicFileMapperToPublicFile(item)
		})
		return nil
	}); err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *PublicFileService) UpdatePublicFile(ctx context.Context, req *gencontent.UpdatePublicFileReq) (resp *gencontent.UpdatePublicFileResp, err error) {
	resp = new(gencontent.UpdatePublicFileResp)
	var fileId primitive.ObjectID
	if fileId, err = primitive.ObjectIDFromHex(req.Id); err != nil {
		return resp, err
	}

	if _, err = s.PublicFileMongoMapper.Update(ctx, &publicfilemapper.PublicFile{
		ID:          fileId,
		AuditStatus: req.AuditStatus,
	}); err != nil {
		return resp, err
	}
	return resp, nil
}

func (s *PublicFileService) AddFileToPublicSpace(ctx context.Context, req *gencontent.AddFileToPublicSpaceReq) (resp *gencontent.AddFileToPublicSpaceResp, err error) {
	resp = new(gencontent.AddFileToPublicSpaceResp)
	type kv struct {
		id   string
		path string
	}

	var (
		rootId string
		front  kv
		res    *filemapper.File
	)

	if res, err = s.FileMongoMapper.FindOne(ctx, req.Id); err != nil {
		return resp, err
	}

	tx := s.PublicFileMongoMapper.StartClient()
	err = tx.UseSession(ctx, func(sessionContext mongo.SessionContext) error {
		if err = sessionContext.StartTransaction(); err != nil {
			return err
		}

		if rootId, err = s.PublicFileMongoMapper.Insert(ctx, &publicfilemapper.PublicFile{ // 创建根文件
			ID:          primitive.NilObjectID,
			UserId:      res.UserId,
			Name:        res.Name,
			Type:        res.Type,
			Size:        res.Size,
			Path:        req.Zone,
			FileMd5:     res.FileMd5,
			Zone:        req.Zone,
			Description: req.Description,
			AuditStatus: int64(gencontent.AuditStatus_AuditStatus_wait),
			Labels:      req.Labels,
		}); err != nil {
			if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
				log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
			}
			return err
		}

		queue := make([]kv, 0, s.Config.InitialSliceLength)
		queue = append(queue, kv{req.Id, req.Zone + "/" + rootId})

		if res.Size == int64(gencontent.Folder_Folder_Size) {
			for len(queue) > 0 {
				front = queue[0]
				queue = queue[1:]

				var data []*filemapper.File
				filter := bson.M{consts.FatherId: front.id}
				if err = s.FileMongoMapper.GetConn().Find(sessionContext, &data, filter); err != nil {
					return err
				}

				if len(data) <= 0 {
					continue
				}

				newPublicFiles := lo.Map(data, func(item *filemapper.File, _ int) *publicfilemapper.PublicFile {
					return &publicfilemapper.PublicFile{
						ID:          primitive.NilObjectID,
						UserId:      item.UserId,
						Name:        item.Name,
						Type:        item.Type,
						Size:        item.Size,
						Path:        front.path,
						FileMd5:     item.FileMd5,
						Zone:        front.path[len(front.path)-len(front.id):],
						Description: req.Description,
						AuditStatus: int64(gencontent.AuditStatus_AuditStatus_wait),
						Labels:      req.Labels,
					}
				})

				if _, err = s.PublicFileMongoMapper.InsertMany(sessionContext, newPublicFiles); err != nil {
					if rbErr := sessionContext.AbortTransaction(sessionContext); rbErr != nil {
						log.CtxError(ctx, "上传文件到社区过程中产生错误[%v]: 回滚异常[%v]\n", err, rbErr)
					}
					return err
				}

				for i := range newPublicFiles {
					if newPublicFiles[i].Size == int64(gencontent.Folder_Folder_Size) {
						queue = append(queue, kv{id: data[i].ID.Hex(), path: front.path + "/" + newPublicFiles[i].ID.Hex()})
					}
				}
			}
		}

		if err = sessionContext.CommitTransaction(sessionContext); err != nil {
			log.CtxError(ctx, "上传文件到社区: 提交事务异常[%v]\n", err)
			return err
		}
		return nil
	})
	resp.Id = rootId
	return resp, err
}

func (s *PublicFileService) MakeFilePrivate(ctx context.Context, req *gencontent.MakeFilePrivateReq) (resp *gencontent.MakeFilePrivateResp, err error) {
	resp = &gencontent.MakeFilePrivateResp{}
	//data := convertor.FileToFileMapper(&gencontent.File{FileId: req.FileId, AuditStatus: int64(gencontent.AuditStatus_AuditStatus_notStart)})
	//update := bson.M{
	//	consts.Zone:        "",
	//	consts.SubZone:     "",
	//	consts.Description: "",
	//	consts.Labels:      "",
	//}
	//
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
