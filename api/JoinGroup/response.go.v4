package JoinGroup

type Response struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	GenerationId   int32
	ProtocolName   string
	Leader         string
	MemberId       string
	Members        []Member // for leader this will not be empty
}

type Member struct {
	MemberId string
	Metadata []byte
}
