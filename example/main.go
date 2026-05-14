package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/itick-org/go-sdk/sdk"
)

func main() {
	// 初始化客户端
	token := "8850*****************ee4127087"
	client := sdk.NewClient(token)

	// 设置 WebSocket 消息处理器
	client.SetMessageHandler(func(message []byte) {
		fmt.Printf("Received WebSocket message: %s\n", message)
	})

	// 设置 WebSocket 错误处理器
	client.SetErrorHandler(func(err error) {
		log.Printf("WebSocket error: %v\n", err)
	})

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

	// 测试 WebSocket 连接
	err = client.ConnectCryptoWebSocket()
	if err != nil {
		log.Printf("ConnectForexWebSocket error: %v", err)
		// Continue even if WebSocket fails
	} else {
		defer client.CloseWebSocket()

		// 发送订阅消息
		// subscribeMsg := []byte(`{"ac": "subscribe", "params": "BTCUSDT$ba","types":"quote,tick"}`)
		err = client.Subscribe([]string{"BTCUSDT$ba"}, []string{"quote", "tick"})
		if err != nil {
			log.Printf("SendWebSocketMessage error: %v", err)
		}

		err = client.Subscribe([]string{"ETHUSDT$ba"}, []string{"quote", "tick"})
		if err != nil {
			log.Printf("SendWebSocketMessage error: %v", err)
		}

		// 等待接收消息
		fmt.Println("Waiting for WebSocket messages...")
		time.Sleep(10 * time.Second)

		// 检查连接状态
		fmt.Printf("WebSocket connected: %v\n", client.IsWebSocketConnected())

		symbols, types := client.GetSubcribe()
		fmt.Printf("GetSubcribe: %s ,%s\n", strings.Join(symbols, ","), strings.Join(types, ","))

		// Wait for interrupt signal to gracefully close
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		fmt.Println("Waiting for WebSocket messages... Press Ctrl+C to exit")
		<-sigCh
		fmt.Println("\nShutting down...")

	}
}
