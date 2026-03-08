package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kregonia/brander_mixer/bootstrap"
	"github.com/kregonia/brander_mixer/widget/server"
)

var (
	port = flag.String("port", "50051", "the port for controller gRPC server")
)

func main() {
	flag.Parse()

	// 初始化日志系统
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bootstrap.Bootstrap(ctx)

	fmt.Printf("🚀 starting controller on port %s...\n", *port)

	// 在独立 goroutine 中启动 gRPC 服务（阻塞调用）
	go server.ControllerServering(*port)

	// 优雅退出：监听 SIGINT / SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("✅ controller is running. Press Ctrl+C to stop.")
	sig := <-sigCh
	fmt.Printf("\n📴 received signal %v, shutting down...\n", sig)
	cancel()
	fmt.Println("👋 controller stopped.")
}
