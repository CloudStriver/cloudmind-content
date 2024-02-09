package consts

const (
	ID                       = "_id"
	UserId                   = "userId"
	TypeString               = "typeString"
	Name                     = "name"
	ProductId                = "productId"
	ProductName              = "productName"
	Type                     = "type"
	Path                     = "path"
	FatherId                 = "fatherId"
	Size                     = "size"
	FileMd5                  = "fileMd5"
	IsDel                    = "isDel"
	SpaceSize                = "spaceSize"
	Tags                     = "tags"
	Zone                     = "zone"
	Key                      = "key"
	SubZone                  = "subZone"
	Status                   = "status"
	Title                    = "title"
	Text                     = "text"
	Description              = "description"
	Labels                   = "labels"
	CreateAt                 = "createAt"
	UpdateAt                 = "updateAt"
	DeletedAt                = "deletedAt"
	TargetId                 = "targetId"
	TargetType               = "targetType"
	RelationType             = "relationType"
	DefaultAvatarUrl         = "d2042520dce2223751906a11e547d43e.png"
	DefaultDescription       = "点击添加描述，让大家更好的了解你..."
	PrivateSpace             = 1
	PublicSpace              = 2
	Intersection             = 1
	UnionSet                 = 2
	Perpetuity               = 0 // 永久有效
	Effective          int64 = 1 // 有效
	Invalid            int64 = 2 // 无效
	FolderSize         int64 = -1
)

var (
	NotDel       int64 = 1
	SoftDel      int64 = 2
	HardDel      int64 = 3
	DefaultLimit       = 10
)
