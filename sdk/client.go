package sdk

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	BaseURL = "https://api.itick.org"
	WSSURL  = "wss://api.itick.org"

	// WebSocket constants
	PingInterval         = 30 * time.Second
	ReconnectInterval    = 5 * time.Second
	MaxReconnectAttempts = 10
)

var reconnectLimiter = time.Tick(ReconnectInterval)

type Client struct {
	token            string
	client           *http.Client
	wsClient         *websocket.Conn
	wsPath           string
	wsConnected      bool
	wsMutex          sync.Mutex
	reconnectChan    chan struct{}
	closeChan        chan struct{}
	messageHandler   func([]byte)
	errorHandler     func(error)
	reconnectHandler func()
	readLoopRunning  bool
	wssSubSymbols    WssSubSymbols
}

type Response struct {
	Code int         `json:"code"`
	Msg  interface{} `json:"msg"`
	Data interface{} `json:"data"`
}

func NewClient(token string) *Client {
	c := &Client{
		token: token,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		reconnectChan: make(chan struct{}),
		closeChan:     make(chan struct{}),
		wssSubSymbols: WssSubSymbols{
			Symbols: map[string]bool{},
			Types:   map[string]bool{},
		},
	}

	go c.reconnectLoop()

	return c
}

func (c *Client) get(path string, params map[string]string, result interface{}) error {
	url := BaseURL + path
	if len(params) > 0 {
		url += "?"
		for k, v := range params {
			url += fmt.Sprintf("%s=%s&", k, v)
		}
		url = url[:len(url)-1]
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("token", c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return err
	}

	if response.Code != 0 {
		return fmt.Errorf("API error: %v", response.Msg)
	}

	data, err := json.Marshal(response.Data)
	if err != nil {
		return err
	}

	return json.Unmarshal(data, result)
}

// WebSocket methods with enhanced functionality

// SetMessageHandler sets the callback for received WebSocket messages
func (c *Client) SetMessageHandler(handler func([]byte)) {
	c.messageHandler = handler
}

// SetErrorHandler sets the callback for WebSocket errors
func (c *Client) SetErrorHandler(handler func(error)) {
	c.errorHandler = handler
}

// SetReconnectHandler sets the callback for successful reconnection
func (c *Client) SetReconnectHandler(handler func()) {
	c.reconnectHandler = handler
}

// ConnectWebSocket establishes a WebSocket connection with automatic reconnection
func (c *Client) ConnectWebSocket(path string) error {
	c.wsPath = path
	err := c.connectWebSocket()
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Subscribe(symbols []string, types []string) error {

	// Check if symbols are already subscribed
	subSymbols := []string{}
	subTypes := []string{}
	if len(symbols) > 0 {
		for _, symbol := range symbols {
			if c.wssSubSymbols.Symbols[symbol] {
				continue
			}
			c.wssSubSymbols.Symbols[symbol] = true
			subSymbols = append(subSymbols, symbol)
		}
	} else {
		for symbol := range c.wssSubSymbols.Symbols {
			subSymbols = append(subSymbols, symbol)
		}
	}
	if len(types) > 0 {
		for _, type_ := range types {
			if c.wssSubSymbols.Types[type_] {
				continue
			}
			c.wssSubSymbols.Types[type_] = true
		}
	}

	for type_ := range c.wssSubSymbols.Types {
		subTypes = append(subTypes, type_)
	}

	subStr := fmt.Sprintf(`{"ac": "subscribe", "params": "%s","types":"%s"}`, strings.Join(subSymbols, ","), strings.Join(subTypes, ","))

	subscribeMsg := []byte(subStr)

	err := c.SendWebSocketMessage(subscribeMsg)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) ClearSubcribe() {
	c.wssSubSymbols = WssSubSymbols{
		Symbols: map[string]bool{},
		Types:   map[string]bool{},
	}
}

func (c *Client) GetSubcribe() ([]string, []string) {
	symbols := []string{}
	types := []string{}
	for symbol := range c.wssSubSymbols.Symbols {
		symbols = append(symbols, symbol)
	}
	for type_ := range c.wssSubSymbols.Types {
		types = append(types, type_)
	}
	return symbols, types
}
func (c *Client) connectWebSocket() error {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()

	// Close existing connection if any
	if c.wsClient != nil {
		c.wsClient.Close()
		c.wsClient = nil
	}
	c.wsConnected = false

	url := WSSURL + c.wsPath
	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{
		"token": []string{c.token},
	})
	if err != nil {
		if c.errorHandler != nil {
			c.errorHandler(err)
		}
		return err
	}

	c.wsClient = conn
	c.wsConnected = true

	// Start ping goroutine
	go c.pingLoop()
	// Start read goroutine
	go c.readLoop()

	return nil
}

func (c *Client) pingLoop() {
	ticker := time.NewTicker(PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.wsMutex.Lock()
			if c.wsClient != nil && c.wsConnected {
				err := c.wsClient.WriteMessage(websocket.PingMessage, []byte{})
				if err != nil {
					c.wsConnected = false
					c.reconnectChan <- struct{}{}
				}
			}
			c.wsMutex.Unlock()
		case <-c.closeChan:
			return
		}
	}
}

func (c *Client) readLoop() {
	// Check if another readLoop is already running
	c.wsMutex.Lock()
	if c.readLoopRunning {
		c.wsMutex.Unlock()
		return
	}
	c.readLoopRunning = true
	c.wsMutex.Unlock()

	// Ensure flag is cleared when this readLoop exits
	defer func() {
		c.wsMutex.Lock()
		c.readLoopRunning = false
		c.wsMutex.Unlock()
	}()

	for {
		select {
		case <-c.closeChan:
			return
		default:
			c.wsMutex.Lock()
			conn := c.wsClient
			connected := c.wsConnected
			c.wsMutex.Unlock()

			if !connected || conn == nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				c.wsMutex.Lock()
				c.wsConnected = false
				c.wsMutex.Unlock()

				if c.errorHandler != nil {
					c.errorHandler(err)
				}
				c.reconnectChan <- struct{}{}
				return
			}

			if c.messageHandler != nil {
				c.messageHandler(message)
			}
		}
	}
}

func (c *Client) reconnectLoop() {
	reconnectAttempts := 0

	for {
		select {
		case <-c.reconnectChan:
			if reconnectAttempts >= MaxReconnectAttempts {
				if c.errorHandler != nil {
					c.errorHandler(fmt.Errorf("max reconnect attempts reached"))
				}
				continue
			}

			<-reconnectLimiter
			err := c.connectWebSocket()
			if err != nil {
				reconnectAttempts++
			} else {
				reconnectAttempts = 0
				if c.reconnectHandler != nil {
					c.reconnectHandler()
				} else {
					c.Subscribe([]string{}, []string{})
				}
			}
		case <-c.closeChan:
			return
		}
	}
}

func (c *Client) SendWebSocketMessage(message []byte) error {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()

	if !c.wsConnected || c.wsClient == nil {
		return fmt.Errorf("websocket not connected")
	}

	return c.wsClient.WriteMessage(websocket.TextMessage, message)
}

func (c *Client) CloseWebSocket() error {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()

	close(c.closeChan)

	if c.wsClient == nil {
		return nil
	}

	err := c.wsClient.Close()
	c.wsConnected = false
	c.wsClient = nil

	return err
}

// IsWebSocketConnected returns the current WebSocket connection status
func (c *Client) IsWebSocketConnected() bool {
	c.wsMutex.Lock()
	defer c.wsMutex.Unlock()
	return c.wsConnected
}

// 基础模块

// GetSymbolList 获取符号列表
func (c *Client) GetSymbolList() (interface{}, error) {
	var result interface{}
	err := c.get("/symbol/list", map[string]string{}, &result)
	return result, err
}

// GetSymbolHolidays 获取节假日信息
func (c *Client) GetSymbolHolidays() (interface{}, error) {
	var result interface{}
	err := c.get("/symbol/holidays", map[string]string{}, &result)
	return result, err
}

// 股票模块

// GetStockInfo 获取股票信息
func (c *Client) GetStockInfo(region, code string) (interface{}, error) {
	var result interface{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/info", params, &result)
	return result, err
}

// GetStockIPO 获取股票IPO信息
func (c *Client) GetStockIPO(region, code string) (interface{}, error) {
	var result interface{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/ipo", params, &result)
	return result, err
}

// GetStockSplit 获取股票分拆信息
func (c *Client) GetStockSplit(region, code string) (interface{}, error) {
	var result interface{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/split", params, &result)
	return result, err
}

// GetStockTick 获取股票实时成交
func (c *Client) GetStockTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/tick", params, &result)
	return result, err
}

// GetStockQuote 获取股票实时报价
func (c *Client) GetStockQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/quote", params, &result)
	return result, err
}

// GetStockDepth 获取股票实时盘口
func (c *Client) GetStockDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/stock/depth", params, &result)
	return result, err
}

// GetStockKline 获取股票历史K线
func (c *Client) GetStockKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/stock/kline", params, &result)
	return result, err
}

// GetStockTicks 获取股票批量实时成交
func (c *Client) GetStockTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/stock/ticks", params, &result)
	return result, err
}

// GetStockQuotes 获取股票批量实时报价
func (c *Client) GetStockQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/stock/quotes", params, &result)
	return result, err
}

// GetStockDepths 获取股票批量实时盘口
func (c *Client) GetStockDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/stock/depths", params, &result)
	return result, err
}

// GetStockKlines 获取股票批量历史K线
func (c *Client) GetStockKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/stock/klines", params, &result)
	return result, err
}

// ConnectStockWebSocket 连接股票WebSocket
func (c *Client) ConnectStockWebSocket() error {
	return c.ConnectWebSocket("/stock")
}

// 指数模块

// GetIndicesTick 获取指数实时成交
func (c *Client) GetIndicesTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/indices/tick", params, &result)
	return result, err
}

// GetIndicesQuote 获取指数实时报价
func (c *Client) GetIndicesQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/indices/quote", params, &result)
	return result, err
}

// GetIndicesDepth 获取指数实时盘口
func (c *Client) GetIndicesDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/indices/depth", params, &result)
	return result, err
}

// GetIndicesKline 获取指数历史K线
func (c *Client) GetIndicesKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/indices/kline", params, &result)
	return result, err
}

// GetIndicesTicks 获取指数批量实时成交
func (c *Client) GetIndicesTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/indices/ticks", params, &result)
	return result, err
}

// GetIndicesQuotes 获取指数批量实时报价
func (c *Client) GetIndicesQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/indices/quotes", params, &result)
	return result, err
}

// GetIndicesDepths 获取指数批量实时盘口
func (c *Client) GetIndicesDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/indices/depths", params, &result)
	return result, err
}

// GetIndicesKlines 获取指数批量历史K线
func (c *Client) GetIndicesKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/indices/klines", params, &result)
	return result, err
}

// ConnectIndicesWebSocket 连接指数WebSocket
func (c *Client) ConnectIndicesWebSocket() error {
	return c.ConnectWebSocket("/indices")
}

// 期货模块

// GetFutureTick 获取期货实时成交
func (c *Client) GetFutureTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/future/tick", params, &result)
	return result, err
}

// GetFutureQuote 获取期货实时报价
func (c *Client) GetFutureQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/future/quote", params, &result)
	return result, err
}

// GetFutureDepth 获取期货实时盘口
func (c *Client) GetFutureDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/future/depth", params, &result)
	return result, err
}

// GetFutureKline 获取期货历史K线
func (c *Client) GetFutureKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/future/kline", params, &result)
	return result, err
}

// GetFutureTicks 获取期货批量实时成交
func (c *Client) GetFutureTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/future/ticks", params, &result)
	return result, err
}

// GetFutureQuotes 获取期货批量实时报价
func (c *Client) GetFutureQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/future/quotes", params, &result)
	return result, err
}

// GetFutureDepths 获取期货批量实时盘口
func (c *Client) GetFutureDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/future/depths", params, &result)
	return result, err
}

// GetFutureKlines 获取期货批量历史K线
func (c *Client) GetFutureKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/future/klines", params, &result)
	return result, err
}

// ConnectFutureWebSocket 连接期货WebSocket
func (c *Client) ConnectFutureWebSocket() error {
	return c.ConnectWebSocket("/future")
}

// 基金模块

// GetFundTick 获取基金实时成交
func (c *Client) GetFundTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/fund/tick", params, &result)
	return result, err
}

// GetFundQuote 获取基金实时报价
func (c *Client) GetFundQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/fund/quote", params, &result)
	return result, err
}

// GetFundDepth 获取基金实时盘口
func (c *Client) GetFundDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/fund/depth", params, &result)
	return result, err
}

// GetFundKline 获取基金历史K线
func (c *Client) GetFundKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/fund/kline", params, &result)
	return result, err
}

// GetFundTicks 获取基金批量实时成交
func (c *Client) GetFundTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/fund/ticks", params, &result)
	return result, err
}

// GetFundQuotes 获取基金批量实时报价
func (c *Client) GetFundQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/fund/quotes", params, &result)
	return result, err
}

// GetFundDepths 获取基金批量实时盘口
func (c *Client) GetFundDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/fund/depths", params, &result)
	return result, err
}

// GetFundKlines 获取基金批量历史K线
func (c *Client) GetFundKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/fund/klines", params, &result)
	return result, err
}

// ConnectFundWebSocket 连接基金WebSocket
func (c *Client) ConnectFundWebSocket() error {
	return c.ConnectWebSocket("/fund")
}

// 外汇模块

// GetForexTick 获取外汇实时成交
func (c *Client) GetForexTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/forex/tick", params, &result)
	return result, err
}

// GetForexQuote 获取外汇实时报价
func (c *Client) GetForexQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/forex/quote", params, &result)
	return result, err
}

// GetForexDepth 获取外汇实时盘口
func (c *Client) GetForexDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/forex/depth", params, &result)
	return result, err
}

// GetForexKline 获取外汇历史K线
func (c *Client) GetForexKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/forex/kline", params, &result)
	return result, err
}

// GetForexTicks 获取外汇批量实时成交
func (c *Client) GetForexTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/forex/ticks", params, &result)
	return result, err
}

// GetForexQuotes 获取外汇批量实时报价
func (c *Client) GetForexQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/forex/quotes", params, &result)
	return result, err
}

// GetForexDepths 获取外汇批量实时盘口
func (c *Client) GetForexDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/forex/depths", params, &result)
	return result, err
}

// GetForexKlines 获取外汇批量历史K线
func (c *Client) GetForexKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/forex/klines", params, &result)
	return result, err
}

// ConnectForexWebSocket 连接外汇WebSocket
func (c *Client) ConnectForexWebSocket() error {
	return c.ConnectWebSocket("/forex")
}

// 加密货币模块

// GetCryptoTick 获取加密货币实时成交
func (c *Client) GetCryptoTick(region, code string) (Tick, error) {
	result := Tick{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/crypto/tick", params, &result)
	return result, err
}

// GetCryptoQuote 获取加密货币实时报价
func (c *Client) GetCryptoQuote(region, code string) (Quote, error) {
	result := Quote{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/crypto/quote", params, &result)
	return result, err
}

// GetCryptoDepth 获取加密货币实时盘口
func (c *Client) GetCryptoDepth(region, code string) (Depth, error) {
	result := Depth{}
	params := map[string]string{
		"region": region,
		"code":   code,
	}
	err := c.get("/crypto/depth", params, &result)
	return result, err
}

// GetCryptoKline 获取加密货币历史K线
func (c *Client) GetCryptoKline(region, code string, kType, limit int, end *int64) ([]Kline, error) {
	result := []Kline{}
	params := map[string]string{
		"region": region,
		"code":   code,
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/crypto/kline", params, &result)
	return result, err
}

// GetCryptoTicks 获取加密货币批量实时成交
func (c *Client) GetCryptoTicks(region string, codes []string) (map[string]Tick, error) {
	result := map[string]Tick{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/crypto/ticks", params, &result)
	return result, err
}

// GetCryptoQuotes 获取加密货币批量实时报价
func (c *Client) GetCryptoQuotes(region string, codes []string) (map[string]Quote, error) {
	result := map[string]Quote{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/crypto/quotes", params, &result)
	return result, err
}

// GetCryptoDepths 获取加密货币批量实时盘口
func (c *Client) GetCryptoDepths(region string, codes []string) (map[string]Depth, error) {
	result := map[string]Depth{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
	}
	err := c.get("/crypto/depths", params, &result)
	return result, err
}

// GetCryptoKlines 获取加密货币批量历史K线
func (c *Client) GetCryptoKlines(region string, codes []string, kType, limit int, end *int64) (map[string][]Kline, error) {
	result := map[string][]Kline{}
	params := map[string]string{
		"region": region,
		"codes":  strings.Join(codes, ","),
		"kType":  fmt.Sprintf("%d", kType),
		"limit":  fmt.Sprintf("%d", limit),
	}
	if end != nil {
		params["et"] = fmt.Sprintf("%d", *end)
	}
	err := c.get("/crypto/klines", params, &result)
	return result, err
}

// ConnectCryptoWebSocket 连接加密货币WebSocket
func (c *Client) ConnectCryptoWebSocket() error {
	return c.ConnectWebSocket("/crypto")
}
