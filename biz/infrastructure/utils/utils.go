package utils

import (
	"github.com/CloudStriver/cloudmind-content/biz/infrastructure/consts"
	gencontent "github.com/CloudStriver/service-idl-gen-go/kitex_gen/cloudmind/content"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/multivaluemode"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/scoremode"
	"github.com/samber/lo"
)

func GetQuery(query []types.Query, filter []types.Query, s []types.SortCombinations, sa []types.FieldValue, sort *gencontent.SearchOption) *search.Request {
	filter = append(filter, GetTimer(sort.SearchTimeType))
	if sort.SearchSortType == gencontent.SearchSortType_SynthesisSearchSortType {
		dateDecayFunc := types.NewDateDecayFunction()
		decayPlacement := types.DecayPlacementDateMathDuration{
			Origin: lo.ToPtr("now"),              // 衰减起点
			Scale:  "2d",                         // 衰减尺度
			Offset: "1d",                         // 可选，定义不应用衰减的初始距离
			Decay:  lo.ToPtr(types.Float64(0.5)), // 衰减率
		}
		dateDecayFunc.DateDecayFunction[consts.CreateAt] = decayPlacement
		dateDecayFunc.MultiValueMode = &multivaluemode.Avg
		return &search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Must:   query,
					Filter: filter,
				},
			},
			Rescore: []types.Rescore{
				{
					Query: types.RescoreQuery{
						Query: types.Query{
							FunctionScore: &types.FunctionScoreQuery{
								Functions: []types.FunctionScore{
									{
										Gauss: dateDecayFunc,
									},
								},
							},
						},
						ScoreMode: &scoremode.Multiply,
					},
				},
			},
		}
	} else {
		return &search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Must:   query,
					Filter: filter,
				},
			},
			Sort:        s,
			SearchAfter: sa,
		}
	}

}

func GetTimer(timer gencontent.SearchTimeType) types.Query {
	var timeRange *string // 使用指针类型，允许其为 nil
	switch timer {
	case gencontent.SearchTimeType_DaySearchTimeType:
		tr := "now-1d/d" // 最近一天
		timeRange = &tr
	case gencontent.SearchTimeType_WeekSearchTimeType:
		tr := "now-7d/d" // 最近一周
		timeRange = &tr
	case gencontent.SearchTimeType_MonthSearchTimeType:
		tr := "now-1M/d" // 最近一个月
		timeRange = &tr
	case gencontent.SearchTimeType_YearSearchTimeType:
		tr := "now-1y/d" // 最近一年
		timeRange = &tr
	}

	// 创建一个 range 查询
	return types.Query{
		Range: map[string]types.RangeQuery{
			consts.CreateAt: types.DateRangeQuery{
				Gte:    timeRange, // 如果 timeRange 是 nil，则不会限制查询的开始时间
				Lte:    nil,       // 不限制查询的结束时间
				Format: nil,       // 如果需要特定的时间格式，可以在这里设置
			},
		},
	}
}
