package consts

import (
	"errors"
	"github.com/zeromicro/go-zero/core/stores/mon"
	"google.golang.org/grpc/status"
)

var (
	ErrNoSuchFile            = status.Error(10101, "文件不存在")
	ErrInvalidId             = status.Error(10102, "文件id无效")
	ErrNoSuchPost            = status.Error(10301, "帖子不存在")
	ErrPaginatorTokenExpired = status.Error(10303, "分页token已过期")
	ErrNoSuchComment         = status.Error(10304, "评论不存在")
	ErrCalFileSize           = status.Error(10305, "计算文件大小失败")
	ErrFileIsNotDir          = status.Error(10306, "目标文件不是文件夹")
	ErrNoSuchLabel           = status.Error(10307, "标签不存在")
)

var (
	ErrNotFound        = mon.ErrNotFound
	ErrInvalidObjectId = errors.New("invalid objectId")
)
