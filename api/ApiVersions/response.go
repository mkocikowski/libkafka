package ApiVersions

type Response struct {
	ErrorCode int16
	ApiKeys   []ApiKeyVersion // slice index same as ApiKey
}

type ApiKeyVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}
