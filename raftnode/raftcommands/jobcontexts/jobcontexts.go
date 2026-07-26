package jobcontexts

type NormJobContext struct {
	NormFileMD5  string  `json:"normFileMD5"`
	NormDistance float64 `json:"normDistance"` // Stored in millimeters
}

type DlcJobContext struct {
	VideoFileMD5 string `json:"videoFileMD5"`
	NumIndv      int    `json:"numIndv"`
}
