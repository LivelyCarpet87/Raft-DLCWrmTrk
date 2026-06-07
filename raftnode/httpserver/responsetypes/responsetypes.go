package responsetypes

type Response[T any] struct {
	Success bool        `json:"success"`
	Data    *T 			`json:"data,omitempty"`
	Error   *ErrorInfo  `json:"error,omitempty"`
}

type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type LeaderHttpAddrResponse struct {
	Leader string `json:"leader"`
}