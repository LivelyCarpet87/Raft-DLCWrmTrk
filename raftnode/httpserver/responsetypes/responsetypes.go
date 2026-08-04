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

type GetHttpEndpointsResponse struct {
	Endpoints []string `json:"endpoints"`
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

type GetBatcheResponse struct {
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
	BatchUIDs []string `json:"batchUIDs"`
}

type TrackletInfo struct {
	TrackID    string  `json:"trackID"`
	MinSpeed   float64 `json:"minSpeed"`
	MaxSpeed   float64 `json:"maxSpeed"`
	MedSpeed   float64 `json:"medSpeed"`
	MeanSpeed  float64 `json:"meanSpeed"`
	TrackLen   float64 `json:"trackLen"`
	WormLen    float64 `json:"wormLen"`
	Confidence float64 `json:"confidence"`
	WarnTxt    string  `json:"warnTxt"`
}

type GetVideoResponse struct {
	VideoName        string         `json:"videoName"`
	NumIndv          int            `json:"numIndv"`
	UploadTime       string         `json:"uploadTime"`
	SystemMessage    string         `json:"systemMessage"`
	LabeledVideoMD5  string         `json:"labeledVideoMD5"`
	ProcessingStatus string         `json:"processingStatus"`
	JobPosition      int            `json:"jobPosition"`
	Tracklets        []TrackletInfo `json:"tracklets"`
}

type GetNormResponse struct {
	ProcessingStatus string  `json:"processingStatus"`
	CreationTime     string  `json:"creationTime"`
	JobPosition      int     `json:"jobPosition"`
	LabeledNormMD5   string  `json:"labeledNormMD5"`
	NormValueAuto    float64 `json:"normValueAuto"`
	NormValueManual  float64 `json:"normValueManual"`
}

type GetWorkersStatusResponse struct {
	MeanJobTime int `json:"meanJobTime"` // Mean time of jobs in seconds
	NumWorkers  int `json:"numWorkers"`  // Number of workers free/assigned
	QueueLength int `json:"queueLength"`
}
