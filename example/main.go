package main

import (
	"fmt"
	"log"
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
	fmt.Printf("Forex Tick: %+v\n", tick)

	// 测试外汇实时报价接口
	quote, err := client.GetForexQuote("GB", "EURUSD")
	if err != nil {
		log.Fatalf("GetForexQuote error: %v", err)
	}
	fmt.Printf("Forex Quote: %+v\n", quote)

	// 测试外汇实时盘口接口
	depth, err := client.GetForexDepth("GB", "EURUSD")
	if err != nil {
		log.Fatalf("GetForexDepth error: %v", err)
	}
	fmt.Printf("Forex Depth: %+v\n", depth)

	// 测试外汇历史K线接口
	kline, err := client.GetForexKline("GB", "EURUSD", 2, 10, nil)
	if err != nil {
		log.Fatalf("GetForexKline error: %v", err)
	}
	fmt.Printf("Forex Kline: %+v\n", kline)

	// 测试 WebSocket 连接
	err = client.ConnectForexWebSocket()
	if err != nil {
		log.Printf("ConnectForexWebSocket error: %v", err)
		// Continue even if WebSocket fails
	} else {
		defer client.CloseWebSocket()

		// 发送订阅消息
		subscribeMsg := []byte(`{"ac": "subscribe", "params": "EURUSD$gb","types":"quote"}`)
		err = client.SendWebSocketMessage(subscribeMsg)
		if err != nil {
			log.Printf("SendWebSocketMessage error: %v", err)
		}

		// 等待接收消息
		fmt.Println("Waiting for WebSocket messages...")
		time.Sleep(10 * time.Second)

		// 检查连接状态
		fmt.Printf("WebSocket connected: %v\n", client.IsWebSocketConnected())
	}
}
