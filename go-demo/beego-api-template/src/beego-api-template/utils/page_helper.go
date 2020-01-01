package utils

/**
分页信息
*/
type PageInfo struct {
	PageNo     int         `json:"page_no"`
	PageSize   int         `json:"page_size"`
	TotalPage  int         `json:"total_page"`
	TotalCount int         `json:"total_count"`
	FirstPage  bool        `json:"first_page"`
	LastPage   bool        `json:"last_page"`
	List       interface{} `json:"list"`
}

func NewPageInfo(totalCount int, pageNo int, pageSize int, list interface{}) PageInfo {
	if pageNo <= 0 || pageSize <= 0 {
		return PageInfo{PageNo: 1, PageSize: totalCount, TotalPage: 1, TotalCount: totalCount, FirstPage: true, LastPage: true, List: list}
	}

	tp := totalCount / pageSize
	if totalCount%pageSize > 0 {
		tp = totalCount/pageSize + 1
	}
	return PageInfo{PageNo: pageNo, PageSize: pageSize, TotalPage: tp, TotalCount: totalCount, FirstPage: pageNo == 1, LastPage: pageNo == tp, List: list}
}
