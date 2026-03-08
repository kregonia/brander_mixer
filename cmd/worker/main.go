package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kregonia/brander_mixer/bootstrap"
	"github.com/kregonia/brander_mixer/widget/connection"
	"github.com/kregonia/brander_mixer/widget/executor"
	"github.com/kregonia/brander_mixer/widget/server"
)

var (
	password = flag.String("p", "", "the password of connect the center computer")
	target   = flag.String("t", "controller:50051", "the target address of center computer")
	grpcPort = flag.String("grpc-port", "0", "port for Controller2Worker gRPC server (0 = auto)")
	maxConc  = flag.Int("concurrency", 4, "max concurrent FFmpeg tasks")
)

// getWorkerID 生成稳定的 workerID：hostname + 首个非回环 IP
func getWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	ip := getOutboundIP()
	if ip == "" {
		ip = "127.0.0.1"
	}

	// workerID 格式: hostname@ip — 同一台设备重启后 ID 不变
	return fmt.Sprintf("%s@%s", hostname, ip)
}

// getOutboundIP 获取本机首个非回环、非虚拟的 IPv4 地址
func getOutboundIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	virtualPrefixes := []string{"lo", "docker", "veth", "br-", "cni", "flannel", "tun", "virbr"}

	for _, iface := range ifaces {
		// 跳过 down 的接口
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		// 跳过回环
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		// 跳过虚拟接口
		skip := false
		for _, prefix := range virtualPrefixes {
			if strings.HasPrefix(iface.Name, prefix) {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ipAddr net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ipAddr = v.IP
			case *net.IPAddr:
				ipAddr = v.IP
			}
			if ipAddr == nil || ipAddr.IsLoopback() {
				continue
			}
			// 只要 IPv4
			if ipAddr.To4() != nil {
				return ipAddr.String()
			}
		}
	}
	return ""
}

func main() {
	flag.Parse()

	// 初始化日志系统
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bootstrap.Bootstrap(ctx)

	// 生成稳定的 workerID
	workerID := getWorkerID()
	fmt.Printf("🖥  worker ID: %s\n", workerID)
	fmt.Printf("🎯 target controller: %s\n", *target)

	// -------------------------------------------------------------------
	// 1. 初始化 Executor（FFmpeg 任务执行器）
	// -------------------------------------------------------------------
	fmt.Println("⚙️  initializing executor...")
	exec, err := executor.NewExecutor(ctx, executor.ExecutorConfig{
		MaxConcurrency: *maxConc,
	})
	if err != nil {
		// FFmpeg 未安装时 executor 创建会失败，但不阻止 worker 启动
		// worker 仍然可以注册和发心跳，只是无法执行转码任务
		fmt.Fprintf(os.Stderr, "⚠️  executor init failed (ffmpeg may not be installed): %v\n", err)
		fmt.Println("⚠️  worker will start without task execution capability")
		exec = nil
	} else {
		fmt.Printf("✅ executor initialized (max concurrency=%d)\n", *maxConc)
	}

	// -------------------------------------------------------------------
	// 2. 启动 Worker 侧 Controller2Worker gRPC 服务
	//    这样 controller 注册成功后可以反向调用 worker
	// -------------------------------------------------------------------
	fmt.Println("⌛️ starting Controller2Worker gRPC server...")
	ws := server.NewWorkerServer(workerID, exec)

	grpcAddr, err := server.WorkerServering(ws, *grpcPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ failed to start worker gRPC server: %v\n", err)
		os.Exit(1)
	}

	// 如果监听在 0.0.0.0 或 [::] 上，替换为实际可达 IP
	grpcAddr = makeExternalAddr(grpcAddr)
	fmt.Printf("📡 Controller2Worker gRPC server listening on %s\n", grpcAddr)

	// -------------------------------------------------------------------
	// 3. 连接 controller（Worker → Controller 方向）
	// -------------------------------------------------------------------
	fmt.Println("⌛️ connecting to controller...")
	client := connection.InitWorkerConnection(*target)
	defer client.Close()

	// -------------------------------------------------------------------
	// 4. 注册（携带 worker gRPC 地址）
	// -------------------------------------------------------------------
	fmt.Println("⌛️ registering worker to controller...")
	success := client.RegistWorker2Controller(ctx, workerID, *password, grpcAddr)
	if !success {
		fmt.Fprintf(os.Stderr, "❌ failed to register worker to controller\n")
		os.Exit(1)
	}
	fmt.Println("✅ registered successfully")

	// -------------------------------------------------------------------
	// 5. 启动心跳（后台 goroutine，携带 secret）
	// -------------------------------------------------------------------
	fmt.Println("💓 starting heartbeat loop...")
	go client.SendHearting(ctx, workerID)

	// -------------------------------------------------------------------
	// 6. 优雅退出
	// -------------------------------------------------------------------
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("✅ worker is running. Press Ctrl+C to stop.")
	sig := <-sigCh
	fmt.Printf("\n📴 received signal %v, shutting down...\n", sig)
	cancel() // 取消 context，停止心跳循环
	if exec != nil {
		exec.Shutdown()
	}
	fmt.Println("👋 worker stopped.")
}

// makeExternalAddr 将 "0.0.0.0:port" 或 "[::]:port" 替换为本机可达 IP
func makeExternalAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		// 使用本机外部 IP 替换
		externalIP := getOutboundIP()
		if externalIP == "" {
			externalIP = "127.0.0.1"
		}
		return net.JoinHostPort(externalIP, port)
	}

	return addr
}
