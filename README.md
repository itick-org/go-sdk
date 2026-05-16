# iTick Go SDK

Go 语言版本的 iTick API SDK，提供基础、股票、指数、期货、基金、外汇、加密货币数据的 REST API 查询和 WebSocket 实时数据订阅功能。

# 官网地址：[https://itick.org](https://itick.org)

## 功能特性

- 支持 REST API 查询基础、股票、指数、期货、基金、外汇、加密货币数据
- 支持 WebSocket 实时数据订阅
- 自动重连机制
- 心跳保持连接
- 回调式事件处理

## 安装

```bash
go get -u github.com/itick-org/go-sdk@latest
```

## 快速开始

### 初始化客户端

```go
package main

import (
    "github.com/itick-org/go-sdk/sdk"
)

func main() {
    token := "your_api_token"
    client := sdk.NewClient(token)
}
```

### REST API 使用

#### 外汇数据查询

```go
// 获取外汇实时成交
// 测试外汇实时成交接口
tick, err := client.GetForexTick("GB", "EURUSD")
if err != nil {
    log.Fatalf("GetForexTick error: %v", err)
}
tickBy, _ := json.Marshal(tick)
fmt.Printf("Forex Tick: %+v\n", string(tickBy))

// 测试外汇实时批量成交接口
ticks, err := client.GetForexTicks("GB", []string{"EURUSD", "GBPUSD"})
if err != nil {
    log.Fatalf("GetForexTicks error: %v", err)
}
ticksBy, _ := json.Marshal(ticks)
fmt.Printf("Forex Ticks: %+v\n", string(ticksBy))

// 测试外汇实时报价接口
quote, err := client.GetForexQuote("GB", "GBPUSD")
if err != nil {
    log.Fatalf("GetForexQuote error: %v", err)
}
quoteBy, _ := json.Marshal(quote)
fmt.Printf("Forex Quote: %+v\n", string(quoteBy))

// 测试外汇实时批量报价接口
quotes, err := client.GetForexQuotes("GB", []string{"EURUSD", "GBPUSD"})
if err != nil {
    log.Fatalf("GetForexQuotes error: %v", err)
}
quotesBy, _ := json.Marshal(quotes)
fmt.Printf("Forex Quotes: %+v\n", string(quotesBy))

// 测试外汇实时盘口接口
depth, err := client.GetForexDepth("GB", "EURUSD")
if err != nil {
    log.Fatalf("GetForexDepth error: %v", err)
}
depthBy, _ := json.Marshal(depth)
fmt.Printf("Forex Depth: %+v\n", string(depthBy))

// 测试外汇实时批量盘口接口
depths, err := client.GetForexDepths("GB", []string{"EURUSD", "GBPUSD"})
if err != nil {
    log.Fatalf("GetForexDepths error: %v", err)
}
depthsBy, _ := json.Marshal(depths)
fmt.Printf("Forex Depths: %+v\n", string(depthsBy))

// 测试外汇历史K线接口
kline, err := client.GetForexKline("GB", "EURUSD", 2, 10, nil)
if err != nil {
    log.Fatalf("GetForexKlines error: %v", err)
}
klineBy, _ := json.Marshal(kline)
fmt.Printf("Forex Klines: %+v\n", string(klineBy))

// 测试外汇批量历史K线接口
klines, err := client.GetForexKlines("GB", []string{"EURUSD", "GBPUSD"}, 2, 10, nil)
if err != nil {
    log.Fatalf("GetForexKlines error: %v", err)
}
klinesBy, _ := json.Marshal(klines)
fmt.Printf("Forex Klines: %+v\n", string(klinesBy))
```

#### 股票数据查询

```go
// 获取股票实时成交
tick, err := client.GetStockTick("US", "AAPL")
if err != nil {
    log.Fatal(err)
}

// 获取股票实时报价
quote, err := client.GetStockQuote("US", "AAPL")
if err != nil {
    log.Fatal(err)
}

// 获取股票实时盘口
depth, err := client.GetStockDepth("US", "AAPL")
if err != nil {
    log.Fatal(err)
}

// 获取股票历史K线
kline, err := client.GetStockKline("US", "AAPL", 2, 10, nil)
if err != nil {
    log.Fatal(err)
}
```

#### 加密货币数据查询

```go
// 获取加密货币实时成交
tick, err := client.GetCryptoTick("BA", "BTCUSDT")
if err != nil {
    log.Fatal(err)
}

// 获取加密货币实时报价
quote, err := client.GetCryptoQuote("BA", "BTCUSDT")
if err != nil {
    log.Fatal(err)
}

// 获取加密货币实时盘口
depth, err := client.GetCryptoDepth("BA", "BTCUSDT")
if err != nil {
    log.Fatal(err)
}

// 获取加密货币历史K线
kline, err := client.GetCryptoKline("BA", "BTCUSDT", 2, 10, nil)
if err != nil {
    log.Fatal(err)
}
```

### WebSocket 使用

SDK 提供了增强的 WebSocket 功能，包括自动重连和心跳保持，用户无需手动管理连接状态。

#### 设置自动重连
```go
// open global reconnect task
sdk.StartGlobalReconnect()
// close global reconnect task
defer sdk.CloseGlobalReconnect()
```

#### 设置回调函数

```go
// 设置消息处理器
client.SetMessageHandler(func(message []byte) {
    fmt.Printf("Received WebSocket message: %s\n", message)
})

// 设置错误处理器
client.SetErrorHandler(func(err error) {
    log.Printf("WebSocket error: %v\n", err)
})
```

#### 连接和订阅

```go

// 连接加密货币 WebSocket
err := client.ConnectCryptoWebSocket()
if err != nil {
    log.Fatal(err)
}
defer client.CloseWebSocket()

// 订阅产品
err = client.Subscribe([]string{"BTCUSDT$ba"}, []string{"quote", "tick"})
if err != nil {
    log.Fatal(err)
}

// 等待接收消息
time.Sleep(10 * time.Second)

// 检查连接状态
fmt.Printf("WebSocket connected: %v\n", client.IsWebSocketConnected())
```

#### 其他 WebSocket 连接

```go
// 连接股票 WebSocket
err := client.ConnectStockWebSocket()

// 连接外汇 WebSocket
err := client.ConnectForexWebSocket()
```

## API 接口列表

### 基础 (Basics)

| 方法 | 说明 |
|------|------|
| GetSymbolList | 获取符号列表 |
| GetSymbolHolidays | 获取节假日信息 |

### 股票 (Stock)

| 方法 | 说明 |
|------|------|
| GetStockInfo | 获取股票信息 |
| GetStockIPO | 获取股票IPO信息 |
| GetStockSplit | 获取股票分拆信息 |
| GetStockTick | 获取股票实时成交 |
| GetStockQuote | 获取股票实时报价 |
| GetStockDepth | 获取股票实时盘口 |
| GetStockKline | 获取股票历史K线 |
| GetStockTicks | 获取股票批量实时成交 |
| GetStockQuotes | 获取股票批量实时报价 |
| GetStockDepths | 获取股票批量实时盘口 |
| GetStockKlines | 获取股票批量历史K线 |
| ConnectStockWebSocket | 连接股票 WebSocket |

### 指数 (Indices)

| 方法 | 说明 |
|------|------|
| GetIndicesTick | 获取指数实时成交 |
| GetIndicesQuote | 获取指数实时报价 |
| GetIndicesDepth | 获取指数实时盘口 |
| GetIndicesKline | 获取指数历史K线 |
| GetIndicesTicks | 获取指数批量实时成交 |
| GetIndicesQuotes | 获取指数批量实时报价 |
| GetIndicesDepths | 获取指数批量实时盘口 |
| GetIndicesKlines | 获取指数批量历史K线 |
| ConnectIndicesWebSocket | 连接指数 WebSocket |

### 期货 (Futures)

| 方法 | 说明 |
|------|------|
| GetFutureTick | 获取期货实时成交 |
| GetFutureQuote | 获取期货实时报价 |
| GetFutureDepth | 获取期货实时盘口 |
| GetFutureKline | 获取期货历史K线 |
| GetFutureTicks | 获取期货批量实时成交 |
| GetFutureQuotes | 获取期货批量实时报价 |
| GetFutureDepths | 获取期货批量实时盘口 |
| GetFutureKlines | 获取期货批量历史K线 |
| ConnectFutureWebSocket | 连接期货 WebSocket |

### 基金 (Funds)

| 方法 | 说明 |
|------|------|
| GetFundTick | 获取基金实时成交 |
| GetFundQuote | 获取基金实时报价 |
| GetFundDepth | 获取基金实时盘口 |
| GetFundKline | 获取基金历史K线 |
| GetFundTicks | 获取基金批量实时成交 |
| GetFundQuotes | 获取基金批量实时报价 |
| GetFundDepths | 获取基金批量实时盘口 |
| GetFundKlines | 获取基金批量历史K线 |
| ConnectFundWebSocket | 连接基金 WebSocket |

### 外汇 (Forex)

| 方法 | 说明 |
|------|------|
| GetForexTick | 获取外汇实时成交 |
| GetForexQuote | 获取外汇实时报价 |
| GetForexDepth | 获取外汇实时盘口 |
| GetForexKline | 获取外汇历史K线 |
| GetForexTicks | 获取外汇批量实时成交 |
| GetForexQuotes | 获取外汇批量实时报价 |
| GetForexDepths | 获取外汇批量实时盘口 |
| GetForexKlines | 获取外汇批量历史K线 |
| ConnectForexWebSocket | 连接外汇 WebSocket |

### 加密货币 (Crypto)

| 方法 | 说明 |
|------|------|
| GetCryptoTick | 获取加密货币实时成交 |
| GetCryptoQuote | 获取加密货币实时报价 |
| GetCryptoDepth | 获取加密货币实时盘口 |
| GetCryptoKline | 获取加密货币历史K线 |
| GetCryptoTicks | 获取加密货币批量实时成交 |
| GetCryptoQuotes | 获取加密货币批量实时报价 |
| GetCryptoDepths | 获取加密货币批量实时盘口 |
| GetCryptoKlines | 获取加密货币批量历史K线 |
| ConnectCryptoWebSocket | 连接加密货币 WebSocket |

## WebSocket 功能说明

### 自动重连

SDK 内置自动重连机制，当网络异常或连接断开时，会自动尝试重新连接：
- 重连间隔：5 秒
- 最大重连次数：10 次
- 重连成功后自动恢复订阅

### 心跳保持

SDK 自动维护 WebSocket 连接的心跳：
- 心跳间隔：30 秒
- 自动发送 ping 消息保持连接活跃

### 连接状态检查

```go
// 检查 WebSocket 是否连接
connected := client.IsWebSocketConnected()
```

## 完整示例

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/itick-org/go-sdk/sdk"
)

func main() {
    // 初始化客户端
    token := "your_api_token"
    client := sdk.NewClient(token)

    // 设置 WebSocket 消息处理器
    client.SetMessageHandler(func(message []byte) {
        fmt.Printf("Received WebSocket message: %s\n", message)
    })

    // 设置 WebSocket 错误处理器
    client.SetErrorHandler(func(err error) {
        log.Printf("WebSocket error: %v\n", err)
    })

    // 测试 REST API
    tick, err := client.GetForexTick("GB", "EURUSD")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Forex Tick: %+v\n", tick)

    err := client.ConnectCryptoWebSocket()
	if err != nil {
		log.Printf("ConnectForexWebSocket error: %v", err)
		// Continue even if WebSocket fails
	} else {
		// defer client.CloseWebSocket()

		// 发送订阅消息
		err = client.Subscribe([]string{"BTCUSDT$ba"}, []string{"quote", "tick"})
		if err != nil {
			log.Printf("Subscribe error: %v", err)
		}

		// 等待接收消息
		fmt.Println("Waiting for WebSocket messages...")
		time.Sleep(10 * time.Second)
}
```

## 文档

详细 API 文档请参考：[https://docs.itick.org](https://docs.itick.org)

## 许可证

MIT License
