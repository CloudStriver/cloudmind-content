package consts

import (
	"errors"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidId             = status.Error(10101, "objectId无效")
	ErrPaginatorTokenExpired = status.Error(10102, "分页token已过期")
	ErrCalFileSize           = status.Error(10103, "计算文件大小失败")
	ErrFileIsNotDir          = status.Error(10104, "目标文件不是文件夹")
	ErrNotFound              = status.Error(10105, "数据不存在")
	ErrInvalidDeleteType     = status.Error(10106, "删除类型无效")
	ErrDataBase              = status.Error(10001, "数据库异常")
	ErrEsMapper              = status.Error(10002, "Es异常")
)

var (
	ErrInvalidObjectId = errors.New("invalid objectId")
)
