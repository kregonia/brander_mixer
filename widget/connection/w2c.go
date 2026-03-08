package connection

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	worker_2_controller_service "github.com/kregonia/brander_mixer/script/rpc_server/worker"
	"github.com/kregonia/brander_mixer/widget/parameter"
	"github.com/kregonia/brander_mixer/widget/status"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ControllerClient struct {
	conn   *grpc.ClientConn
	client worker_2_controller_service.Worker2ControllerClient

	// secret 由注册成功后 controller 返回，后续心跳需携带
	secret string
}

func InitWorkerConnection(target string) *ControllerClient {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatalf("[w2c] failed to connect to controller at %s: %v", target, err)
	}
	client := worker_2_controller_service.NewWorker2ControllerClient(conn)
	return &ControllerClient{
		conn:   conn,
		client: client,
	}
}

func (cc *ControllerClient) Close() {
	if cc.conn != nil {
		cc.conn.Close()
	}
}

func (cc *ControllerClient) GetClient() worker_2_controller_service.Worker2ControllerClient {
	return cc.client
}

func (cc *ControllerClient) GetConn() *grpc.ClientConn {
	return cc.conn
}

func (cc *ControllerClient) GetSecret() string {
	return cc.secret
}

// ---------------------------------------------------------------------------
// GetWorkerInfo 收集本机设备信息，用于注册时上报给 controller
//
// 包括：主机信息、CPU 信息、内存总量、GPU/VPU/硬件编码器检测
// ---------------------------------------------------------------------------

func GetWorkerInfo() *worker_2_controller_service.WorkerInfo {
	info := &worker_2_controller_service.WorkerInfo{
		Arch: runtime.GOARCH,
		Os:   runtime.GOOS,
	}

	// 主机信息
	if hostInfo, err := host.Info(); err == nil {
		info.Hostname = hostInfo.Hostname
		info.Platform = hostInfo.Platform
		info.PlatformVersion = hostInfo.PlatformVersion
		info.KernelVersion = hostInfo.KernelVersion
	} else {
		logger.Warnf("[w2c GetWorkerInfo] failed to get host info: %v", err)
	}

	// CPU 信息
	if cpuInfos, err := cpu.Info(); err == nil && len(cpuInfos) > 0 {
		info.CpuModelName = cpuInfos[0].ModelName
		info.CpuPhysicalCores = cpuInfos[0].Cores

		// 逻辑核心数
		logicalCores, err := cpu.Counts(true)
		if err == nil {
			info.CpuLogicalCores = int32(logicalCores)
		} else {
			info.CpuLogicalCores = int32(len(cpuInfos))
		}
	} else {
		logger.Warnf("[w2c GetWorkerInfo] failed to get cpu info: %v", err)
	}

	// 内存总量
	if memInfo, err := mem.VirtualMemory(); err == nil {
		info.MemoryTotal = memInfo.Total
	} else {
		logger.Warnf("[w2c GetWorkerInfo] failed to get memory info: %v", err)
	}

	// GPU / VPU / 硬件编码器检测
	gpuName, gpuMemory, hwEncoder := detectGPU()
	info.GpuName = gpuName
	info.GpuMemory = gpuMemory
	info.HardwareEncoder = hwEncoder

	if gpuName != "" {
		logger.Noticef("[w2c GetWorkerInfo] GPU detected: %s (memory=%d bytes, hwEncoder=%v)",
			gpuName, gpuMemory, hwEncoder)
	} else {
		logger.Noticef("[w2c GetWorkerInfo] no dedicated GPU detected, hwEncoder=%v", hwEncoder)
	}

	return info
}

// ---------------------------------------------------------------------------
// detectGPU 检测本机 GPU / VPU / 硬件编码器能力
//
// 检测顺序：
//  1. NVIDIA GPU — 通过 nvidia-smi 获取型号和显存
//  2. Apple VideoToolbox — macOS 内置硬件编码器
//  3. Intel VAAPI — Linux 下 Intel 集成 GPU 硬件加速
//  4. FFmpeg 硬件编码器 — 通过 ffmpeg -encoders 检查支持的编码器
//
// 返回: (gpuName, gpuMemoryBytes, hasHardwareEncoder)
// ---------------------------------------------------------------------------

func detectGPU() (string, uint64, bool) {
	var gpuName string
	var gpuMemory uint64
	hasHWEncoder := false

	// ---- 1. NVIDIA GPU (nvidia-smi) ----
	name, mem, err := detectNvidiaGPU()
	if err == nil && name != "" {
		gpuName = name
		gpuMemory = mem
		hasHWEncoder = true // NVIDIA GPU 一般支持 NVENC
		logger.Noticef("[GPU] NVIDIA GPU detected: %s, memory=%d MB", name, mem/(1024*1024))
		return gpuName, gpuMemory, hasHWEncoder
	}

	// ---- 2. Apple VideoToolbox (macOS) ----
	if runtime.GOOS == "darwin" {
		if vtName, vtOK := detectVideoToolbox(); vtOK {
			gpuName = vtName
			hasHWEncoder = true
			logger.Noticef("[GPU] Apple VideoToolbox detected: %s", vtName)
			return gpuName, gpuMemory, hasHWEncoder
		}
	}

	// ---- 3. Intel VAAPI (Linux) ----
	if runtime.GOOS == "linux" {
		if vaName, vaOK := detectVAAPI(); vaOK {
			gpuName = vaName
			hasHWEncoder = true
			logger.Noticef("[GPU] Intel VAAPI detected: %s", vaName)
			return gpuName, gpuMemory, hasHWEncoder
		}
	}

	// ---- 4. FFmpeg 硬件编码器兜底检查 ----
	if !hasHWEncoder {
		hasHWEncoder = detectFFmpegHWEncoders()
	}

	return gpuName, gpuMemory, hasHWEncoder
}

// detectNvidiaGPU 使用 nvidia-smi 获取 NVIDIA GPU 信息
func detectNvidiaGPU() (string, uint64, error) {
	smiPath, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return "", 0, fmt.Errorf("nvidia-smi not found: %w", err)
	}

	// 查询 GPU 名称和显存（MiB）
	cmd := exec.Command(smiPath,
		"--query-gpu=name,memory.total",
		"--format=csv,noheader,nounits",
	)
	output, err := cmd.Output()
	if err != nil {
		return "", 0, fmt.Errorf("nvidia-smi query failed: %w", err)
	}

	line := strings.TrimSpace(string(output))
	if line == "" {
		return "", 0, fmt.Errorf("empty nvidia-smi output")
	}

	// 只取第一块 GPU（多 GPU 时有多行）
	lines := strings.Split(line, "\n")
	parts := strings.SplitN(strings.TrimSpace(lines[0]), ", ", 2)
	if len(parts) < 2 {
		return strings.TrimSpace(lines[0]), 0, nil
	}

	name := strings.TrimSpace(parts[0])
	memMiB, _ := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
	memBytes := memMiB * 1024 * 1024

	return name, memBytes, nil
}

// detectVideoToolbox 检测 macOS VideoToolbox 硬件编码器
func detectVideoToolbox() (string, bool) {
	// 使用 system_profiler 获取 GPU 信息
	cmd := exec.Command("system_profiler", "SPDisplaysDataType", "-detailLevel", "mini")
	output, err := cmd.Output()
	if err != nil {
		return "", false
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// 查找 "Chipset Model:" 行
		if strings.HasPrefix(trimmed, "Chipset Model:") {
			chipset := strings.TrimPrefix(trimmed, "Chipset Model:")
			chipset = strings.TrimSpace(chipset)
			if chipset != "" {
				return chipset + " (VideoToolbox)", true
			}
		}
	}

	// 如果无法获取具体型号，检查 ffmpeg 是否支持 videotoolbox
	if checkFFmpegEncoder("h264_videotoolbox") {
		return "Apple VideoToolbox", true
	}

	return "", false
}

// detectVAAPI 检测 Linux Intel VAAPI 硬件加速
func detectVAAPI() (string, bool) {
	// 检查 /dev/dri/renderD128（VAAPI 设备节点）
	cmd := exec.Command("ls", "/dev/dri/renderD128")
	if err := cmd.Run(); err != nil {
		return "", false
	}

	// 尝试从 vainfo 获取 GPU 信息
	vainfo, err := exec.LookPath("vainfo")
	if err == nil {
		cmd := exec.Command(vainfo)
		output, err := cmd.Output()
		if err == nil {
			outStr := string(output)
			// 解析 driver string
			for _, line := range strings.Split(outStr, "\n") {
				if strings.Contains(line, "vainfo: Driver version:") {
					driver := strings.TrimSpace(strings.SplitN(line, ":", 3)[2])
					return "Intel VAAPI (" + driver + ")", true
				}
			}
		}
	}

	// 兜底：检查 ffmpeg 是否支持 h264_vaapi
	if checkFFmpegEncoder("h264_vaapi") {
		return "Intel VAAPI", true
	}

	return "", false
}

// detectFFmpegHWEncoders 检查 ffmpeg 是否支持任何硬件编码器
func detectFFmpegHWEncoders() bool {
	hwEncoders := []string{
		"h264_nvenc",        // NVIDIA
		"hevc_nvenc",        // NVIDIA
		"h264_videotoolbox", // macOS
		"hevc_videotoolbox", // macOS
		"h264_vaapi",        // Intel Linux
		"hevc_vaapi",        // Intel Linux
		"h264_qsv",          // Intel QuickSync
		"hevc_qsv",          // Intel QuickSync
		"h264_amf",          // AMD
		"hevc_amf",          // AMD
	}

	for _, enc := range hwEncoders {
		if checkFFmpegEncoder(enc) {
			logger.Noticef("[GPU] FFmpeg hardware encoder detected: %s", enc)
			return true
		}
	}

	return false
}

// checkFFmpegEncoder 检查 ffmpeg 是否支持指定的编码器
func checkFFmpegEncoder(encoderName string) bool {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		return false
	}

	cmd := exec.Command(ffmpegPath, "-encoders")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	return strings.Contains(string(output), encoderName)
}

// ---------------------------------------------------------------------------
// RegistWorker2Controller 注册本节点到 controller
//
// 改动点：
//   1. 注册时携带完整的 WorkerInfo（设备画像基础数据）
//   2. 注册成功后保存 controller 返回的 secret
//   3. 增加重试间隔，避免密集重试冲击 controller
// ---------------------------------------------------------------------------

func (cc *ControllerClient) RegistWorker2Controller(ctx context.Context, workerID string, password string, workerGrpcAddr string) bool {
	workerInfo := GetWorkerInfo()

	fmt.Printf("📋 worker info: hostname=%s os=%s/%s cpu=%s cores=%d/%d\n",
		workerInfo.Hostname,
		workerInfo.Os,
		workerInfo.Arch,
		workerInfo.CpuModelName,
		workerInfo.CpuPhysicalCores,
		workerInfo.CpuLogicalCores,
	)
	if workerGrpcAddr != "" {
		fmt.Printf("📡 worker gRPC addr: %s\n", workerGrpcAddr)
	}

	maxRetries := 5
	for attempt := 1; attempt <= maxRetries; attempt++ {
		response, err := cc.client.RegistWorker(
			ctx,
			&worker_2_controller_service.RegistRequest{
				Ip:             workerID,
				Info:           workerInfo,
				WorkerGrpcAddr: workerGrpcAddr,
			},
		)
		if err != nil {
			logger.Errorf("[w2c RegistWorker] attempt %d/%d failed: %v", attempt, maxRetries, err)
			time.Sleep(time.Duration(attempt) * time.Second) // 递增退避
			continue
		}
		if response.GetSuccess() {
			cc.secret = response.GetSecret()
			logger.Noticef("[w2c RegistWorker] registered successfully, secret=%s...%s",
				cc.secret[:3], cc.secret[len(cc.secret)-3:])
			return true
		}
		logger.Warnf("[w2c RegistWorker] attempt %d/%d: server returned success=false", attempt, maxRetries)
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	logger.Errorf("[w2c RegistWorker] failed to register after %d attempts", maxRetries)
	return false
}

// ---------------------------------------------------------------------------
// SendHearting 持续发送心跳
//
// 改动点：
//   1. 携带 secret（之前没带，导致 controller 侧校验永远失败）
//   2. 增加错误计数和连续失败预警
//   3. 支持 context 取消优雅退出
// ---------------------------------------------------------------------------

func (cc *ControllerClient) SendHearting(ctx context.Context, ip string) {
	ticker := time.NewTicker(time.Second * time.Duration(parameter.DefaultIntervalSeconds))
	defer ticker.Stop()

	consecutiveFails := 0
	const maxConsecutiveFails = 6 // 连续失败 6 次（30 秒）则预警

	for {
		select {
		case <-ctx.Done():
			logger.Noticef("[w2c SendHearting] context cancelled, stopping heartbeat loop")
			return
		case <-ticker.C:
			workerStatus, err := status.GetWorkerStatus()
			if err != nil {
				logger.Errorf("[w2c SendHearting] get worker status failed: %v", err)
				continue
			}

			res, err := cc.client.Hearting(ctx, &worker_2_controller_service.HeartingRequest{
				Ip:     ip,
				Secret: cc.secret, // 关键修复：携带 secret
				Status: workerStatus,
			})

			if err != nil {
				consecutiveFails++
				logger.Errorf("[w2c SendHearting] send heartbeat failed (consecutive=%d): %v", consecutiveFails, err)
				if consecutiveFails >= maxConsecutiveFails {
					logger.Warnf("[w2c SendHearting] ⚠️ lost connection to controller for %d consecutive heartbeats",
						consecutiveFails)
				}
				continue
			}

			if res == nil || !res.GetSuccess() {
				consecutiveFails++
				logger.Warnf("[w2c SendHearting] heartbeat rejected by controller (consecutive=%d)", consecutiveFails)
				continue
			}

			// 心跳成功，重置计数
			if consecutiveFails > 0 {
				logger.Noticef("[w2c SendHearting] heartbeat recovered after %d failures", consecutiveFails)
			}
			consecutiveFails = 0
		}
	}
}
