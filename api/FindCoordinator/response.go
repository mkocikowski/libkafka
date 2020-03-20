package FindCoordinator

type Response struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   string
	NodeId         int32
	Host           string
	Port           int32
}
