package responsetypes

type Response[T any] struct {
	Success bool       `json:"success"`
	Data    *T         `json:"data,omitempty"`
	Error   *ErrorInfo `json:"error,omitempty"`
}

type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type LeaderHttpAddrResponse struct {
	Leader string `json:"leader"`
}

type TagInfo struct {
	TagName string `json:"tagName"`
	TagType string `json:"tagType"`
	Visible string `json:"visible"`
}

type ListTagsResponse struct {
	Tags *[]TagInfo `json:"tags"`
}

type CreateBatchResponse struct {
	BatchUID string `json:"batchUID"`
}
