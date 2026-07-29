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

type ListBatchesEntry struct {
	BatchUID     string   `json:"batchUID"`
	CreationTime string   `json:"creationTime"`
	BatchName    string   `json:"batchName"`
	PrimaryTag   string   `json:"primaryTag"`
	SecondaryTag string   `json:"secondaryTag"`
	NormMD5      string   `json:"normMD5"`
	Conditions   []string `json:"conditions"`
	VideoMD5s    []string `json:"videoMD5s"`
	Note         string   `json:"note"`
}

type ListBatchesResponse struct {
	Batches []ListBatchesEntry `json:"batches"`
}
