package jobresults

type DlcJobResRow struct {
	Indv       string  `json:"indv"`
	MeanSpeed  float64 `json:"meanSpeed"`
	Confidence float64 `json:"confidence"`
}

type DlcLabVideoFileInfo struct {
	HashMD5  string `json:"hashMD5"`
	VNodeID  string `json:"vNodeID"`
	Filesize int64  `json:"filesize"`
}

type DlcJobResults struct {
	NumIndv       int            `json:"numIndv"`
	Message       string         `json:"message"`
	Entries       []DlcJobResRow `json:"rows"`
	VideoFileInfo []byte         `json:"videoFileInfo"`
}
