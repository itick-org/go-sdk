package sdk

type Reponse[T Tick | Quote | Kline | Depth] struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data T      `json:"data"`
}

type Tick struct {
	S  string  `json:"s"`
	Ld float64 `json:"ld"`
	T  int64   `json:"t"`
	V  float64 `json:"v"`
	Tu float64 `json:"tu"`
	Ts int     `json:"ts"`
}

type Quote struct {
	S   string  `json:"s"`
	Ld  float64 `json:"ld"`
	O   float64 `json:"o"`
	H   float64 `json:"h"`
	L   float64 `json:"l"`
	T   int64   `json:"t"`
	V   float64 `json:"v"`
	Tu  float64 `json:"tu"`
	Ch  float64 `json:"ch"`
	Chp float64 `json:"chp"`
	Ts  int     `json:"ts"`
}

type Depth struct {
	S string      `json:"s"`
	A []DepthItem `json:"a"`
	B []DepthItem `json:"b"`
}

type DepthItem struct {
	Po int     `json:"po"`
	P  float64 `json:"p"`
	V  int64   `json:"v"`
	O  int64   `json:"o"`
}

type Kline struct {
	Tu float64 `json:"tu"`
	C  float64 `json:"c"`
	T  int64   `json:"t"`
	V  float64 `json:"v"`
	H  float64 `json:"h"`
	L  float64 `json:"l"`
	O  float64 `json:"o"`
}

type WssSubSymbols struct {
	Symbols map[string]bool `json:"symbols"`
	Types   map[string]bool `json:"types"`
}
