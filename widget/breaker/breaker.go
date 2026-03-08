package breaker

import (
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
)

// ---------------------------------------------------------------------------
// VideoInfo 视频探测结果（通过 ffprobe 获取）
// ---------------------------------------------------------------------------

type VideoInfo struct {
	FilePath string

	// 基本信息
	Duration    float64 // 总时长（秒）
	BitRate     int64   // 比特率（bps）
	Size        int64   // 文件大小（bytes）
	FormatName  string  // 容器格式（如 "mov,mp4,m4a,3gp,3g2,mj2"）
	FormatLong  string  // 格式长名称
	StreamCount int     // 流数量

	// 视频流信息
	VideoCodec   string  // 视频编码器名称（如 "h264", "hevc", "av1"）
	Width        int     // 宽度
	Height       int     // 高度
	FPS          float64 // 帧率
	TotalFrames  int64   // 总帧数（如果可获取）
	PixelFormat  string  // 像素格式（如 "yuv420p"）
	VideoIndex   int     // 视频流索引
	GOPSize      int     // 关键帧间隔（帧数），0 表示未知
	VideoBitRate int64   // 视频流比特率

	// 音频流信息
	AudioCodec      string // 音频编码器名称
	AudioSampleRate int    // 采样率
	AudioChannels   int    // 声道数
	AudioIndex      int    // 音频流索引
	AudioBitRate    int64  // 音频流比特率
}

// ---------------------------------------------------------------------------
// TaskChunk 拆分后的任务分片
// ---------------------------------------------------------------------------

type TaskChunk struct {
	Index       int     // 分片索引（从 0 开始）
	TotalChunks int     // 总分片数
	StartTime   float64 // 起始时间（秒）
	EndTime     float64 // 结束时间（秒）
	Duration    float64 // 本分片时长（秒）
	StartFrame  int64   // 起始帧号（估算）
	EndFrame    int64   // 结束帧号（估算）
	FrameCount  int64   // 本分片帧数（估算）
}

// ---------------------------------------------------------------------------
// SplitConfig 拆分配置
// ---------------------------------------------------------------------------

type SplitConfig struct {
	// 拆分粒度（二选一，优先用 ChunkCount）
	ChunkCount    int     // 指定分片数量（> 0 时生效）
	ChunkDuration float64 // 每片目标时长（秒），ChunkCount <= 0 时生效

	// 转码参数
	Codec     string // 目标编码器（如 "libx264", "libsvtav1", "libx265"）
	Preset    string // 编码预设（如 "fast", "medium", "slow"）
	CRF       int    // 质量参数（0-51, 默认 23）
	OutputFmt string // 输出格式（如 "mp4", "mkv"，默认与输入相同）

	// 额外 FFmpeg 参数
	ExtraArgs []string

	// 输出路径模板（支持 %d 表示分片索引）
	// 例如 "/tmp/output/chunk_%03d.mp4"
	OutputPattern string
}

// DefaultSplitConfig 默认拆分配置
func DefaultSplitConfig() SplitConfig {
	return SplitConfig{
		ChunkCount:    0,
		ChunkDuration: 10.0, // 默认每片 10 秒
		Codec:         "libx264",
		Preset:        "medium",
		CRF:           23,
		OutputFmt:     "mp4",
		OutputPattern: "chunk_%03d.mp4",
	}
}

// ---------------------------------------------------------------------------
// FFmpegCommand 单个分片对应的 FFmpeg 命令
// ---------------------------------------------------------------------------

type FFmpegCommand struct {
	Chunk      TaskChunk // 对应的分片信息
	Args       []string  // FFmpeg 参数列表（不含 "ffmpeg" 本身）
	CmdLine    string    // 完整命令行字符串（用于日志/调试）
	InputFile  string
	OutputFile string
}

// ---------------------------------------------------------------------------
// Probe 使用 ffprobe 探测视频文件信息
// ---------------------------------------------------------------------------

func Probe(filePath string) (*VideoInfo, error) {
	startTime := time.Now()

	// 先检查 ffprobe 是否可用
	ffprobePath, err := exec.LookPath("ffprobe")
	if err != nil {
		return nil, fmt.Errorf("ffprobe not found in PATH: %w", err)
	}

	// 执行 ffprobe 获取 JSON 格式的媒体信息
	args := []string{
		"-v", "quiet",
		"-print_format", "json",
		"-show_format",
		"-show_streams",
		filePath,
	}

	cmd := exec.Command(ffprobePath, args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe execution failed for %s: %w", filePath, err)
	}

	// 解析 JSON
	var probeResult ffprobeResult
	if err := json.Unmarshal(output, &probeResult); err != nil {
		return nil, fmt.Errorf("failed to parse ffprobe output: %w", err)
	}

	info := &VideoInfo{
		FilePath:    filePath,
		StreamCount: len(probeResult.Streams),
	}

	// 解析 format 信息
	if probeResult.Format != nil {
		info.FormatName = probeResult.Format.FormatName
		info.FormatLong = probeResult.Format.FormatLongName
		info.Duration = parseFloat(probeResult.Format.Duration)
		info.BitRate = parseInt64(probeResult.Format.BitRate)
		info.Size = parseInt64(probeResult.Format.Size)
	}

	// 解析 stream 信息
	for _, stream := range probeResult.Streams {
		switch stream.CodecType {
		case "video":
			info.VideoCodec = stream.CodecName
			info.Width = stream.Width
			info.Height = stream.Height
			info.PixelFormat = stream.PixFmt
			info.VideoIndex = stream.Index
			info.VideoBitRate = parseInt64(stream.BitRate)
			info.TotalFrames = parseInt64(stream.NbFrames)

			// 帧率解析（r_frame_rate 格式如 "30000/1001"）
			info.FPS = parseRational(stream.RFrameRate)

			// 如果总帧数为 0，尝试通过 duration × fps 估算
			if info.TotalFrames == 0 && info.FPS > 0 && info.Duration > 0 {
				info.TotalFrames = int64(math.Round(info.Duration * info.FPS))
			}

		case "audio":
			info.AudioCodec = stream.CodecName
			info.AudioSampleRate = parseInt(stream.SampleRate)
			info.AudioChannels = stream.Channels
			info.AudioIndex = stream.Index
			info.AudioBitRate = parseInt64(stream.BitRate)
		}
	}

	// 尝试获取 GOP 大小（关键帧间隔）
	gopSize, err := probeGOPSize(ffprobePath, filePath)
	if err == nil && gopSize > 0 {
		info.GOPSize = gopSize
	}

	elapsed := time.Since(startTime)
	logger.Noticef("[Breaker] probed %s in %v: duration=%.2fs, resolution=%dx%d, fps=%.2f, codec=%s, frames=%d",
		filePath, elapsed, info.Duration, info.Width, info.Height, info.FPS, info.VideoCodec, info.TotalFrames)

	return info, nil
}

// ---------------------------------------------------------------------------
// SplitByTime 按时间维度拆分视频为多个 TaskChunk
//
// 拆分逻辑：
//   1. 如果指定了 ChunkCount，则按 duration / ChunkCount 切分
//   2. 否则按 ChunkDuration 切分
//   3. 最后一片自动延伸到视频末尾（避免精度丢失导致漏帧）
//   4. 如果已知 GOP 大小，尝试将切点对齐到关键帧边界
// ---------------------------------------------------------------------------

func SplitByTime(info *VideoInfo, cfg SplitConfig) []TaskChunk {
	if info.Duration <= 0 {
		logger.Warnf("[Breaker] video duration is 0 or negative, cannot split: %s", info.FilePath)
		return nil
	}

	var chunkDuration float64
	var chunkCount int

	if cfg.ChunkCount > 0 {
		chunkCount = cfg.ChunkCount
		chunkDuration = info.Duration / float64(chunkCount)
	} else {
		if cfg.ChunkDuration <= 0 {
			cfg.ChunkDuration = 10.0
		}
		chunkDuration = cfg.ChunkDuration
		chunkCount = int(math.Ceil(info.Duration / chunkDuration))
	}

	// 如果已知 GOP 大小和帧率，对齐切分时长到 GOP 边界
	if info.GOPSize > 0 && info.FPS > 0 {
		gopDuration := float64(info.GOPSize) / info.FPS
		if gopDuration > 0 && chunkDuration > gopDuration {
			// 向上取整到 GOP 倍数
			gopMultiple := math.Round(chunkDuration / gopDuration)
			if gopMultiple < 1 {
				gopMultiple = 1
			}
			alignedDuration := gopMultiple * gopDuration
			// 只有偏差不太大时才对齐（避免分片过大或过小）
			if math.Abs(alignedDuration-chunkDuration)/chunkDuration < 0.3 {
				chunkDuration = alignedDuration
				chunkCount = int(math.Ceil(info.Duration / chunkDuration))
				logger.Noticef("[Breaker] aligned chunk duration to GOP boundary: %.3fs (GOP=%.3fs, %d frames)",
					chunkDuration, gopDuration, info.GOPSize)
			}
		}
	}

	// 最少 1 片
	if chunkCount < 1 {
		chunkCount = 1
	}

	chunks := make([]TaskChunk, 0, chunkCount)
	var offset float64

	for i := 0; i < chunkCount; i++ {
		start := offset
		end := start + chunkDuration

		// 最后一片延伸到视频末尾
		if i == chunkCount-1 || end >= info.Duration {
			end = info.Duration
		}

		dur := end - start
		if dur <= 0 {
			break
		}

		var startFrame, endFrame, frameCount int64
		if info.FPS > 0 {
			startFrame = int64(math.Round(start * info.FPS))
			endFrame = int64(math.Round(end * info.FPS))
			frameCount = endFrame - startFrame
		}

		chunks = append(chunks, TaskChunk{
			Index:       i,
			TotalChunks: chunkCount,
			StartTime:   roundTo3(start),
			EndTime:     roundTo3(end),
			Duration:    roundTo3(dur),
			StartFrame:  startFrame,
			EndFrame:    endFrame,
			FrameCount:  frameCount,
		})

		offset = end
		if offset >= info.Duration {
			break
		}
	}

	// 修正 TotalChunks（实际可能少于预估）
	for i := range chunks {
		chunks[i].TotalChunks = len(chunks)
	}

	logger.Noticef("[Breaker] split %s into %d chunks (target duration=%.2fs, actual GOP-aligned=%v)",
		info.FilePath, len(chunks), chunkDuration, info.GOPSize > 0)

	return chunks
}

// ---------------------------------------------------------------------------
// GenerateFFmpegCommands 为每个分片生成 FFmpeg 转码命令
//
// 使用 seek-before-input 模式（-ss before -i）实现快速 seek，
// 配合 -to 参数精确控制时长。
// ---------------------------------------------------------------------------

func GenerateFFmpegCommands(info *VideoInfo, chunks []TaskChunk, cfg SplitConfig) []FFmpegCommand {
	if len(chunks) == 0 {
		return nil
	}

	codec := cfg.Codec
	if codec == "" {
		codec = "libx264"
	}
	preset := cfg.Preset
	if preset == "" {
		preset = "medium"
	}
	crf := cfg.CRF
	if crf <= 0 {
		crf = 23
	}
	outputFmt := cfg.OutputFmt
	if outputFmt == "" {
		outputFmt = "mp4"
	}
	outputPattern := cfg.OutputPattern
	if outputPattern == "" {
		outputPattern = fmt.Sprintf("chunk_%%03d.%s", outputFmt)
	}

	commands := make([]FFmpegCommand, 0, len(chunks))

	for _, chunk := range chunks {
		outputFile := fmt.Sprintf(outputPattern, chunk.Index)

		args := []string{
			"-y", // 覆盖输出文件
		}

		// seek-before-input（快速定位）
		if chunk.StartTime > 0 {
			args = append(args, "-ss", formatDuration(chunk.StartTime))
		}

		// 输入文件
		args = append(args, "-i", info.FilePath)

		// 结束时间（相对于 seek 后的起点）
		args = append(args, "-t", formatDuration(chunk.Duration))

		// 视频编码
		args = append(args,
			"-c:v", codec,
			"-preset", preset,
			"-crf", strconv.Itoa(crf),
		)

		// 音频编码（默认 AAC）
		if info.AudioCodec != "" {
			args = append(args, "-c:a", "aac", "-b:a", "128k")
		} else {
			args = append(args, "-an") // 无音频
		}

		// 确保关键帧对齐（便于后续合并）
		args = append(args,
			"-force_key_frames", fmt.Sprintf("expr:gte(t,n_forced*%.3f)", chunk.Duration),
		)

		// 输出格式
		args = append(args, "-f", outputFmt)

		// 额外参数
		if len(cfg.ExtraArgs) > 0 {
			args = append(args, cfg.ExtraArgs...)
		}

		// 输出文件（放最后）
		args = append(args, outputFile)

		// 构建完整命令行字符串
		cmdParts := make([]string, 0, len(args)+1)
		cmdParts = append(cmdParts, "ffmpeg")
		cmdParts = append(cmdParts, args...)
		cmdLine := strings.Join(cmdParts, " ")

		commands = append(commands, FFmpegCommand{
			Chunk:      chunk,
			Args:       args,
			CmdLine:    cmdLine,
			InputFile:  info.FilePath,
			OutputFile: outputFile,
		})
	}

	logger.Noticef("[Breaker] generated %d FFmpeg commands for %s (codec=%s, preset=%s, crf=%d)",
		len(commands), info.FilePath, codec, preset, crf)

	return commands
}

// ---------------------------------------------------------------------------
// SplitByNodeCapacity 按节点数量自动拆分（快捷方法）
//
// 接收节点数作为 ChunkCount，自动 probe + split + generate
// ---------------------------------------------------------------------------

func SplitByNodeCapacity(filePath string, nodeCount int, cfg SplitConfig) ([]FFmpegCommand, *VideoInfo, error) {
	info, err := Probe(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to probe video: %w", err)
	}

	if nodeCount <= 0 {
		nodeCount = 1
	}
	cfg.ChunkCount = nodeCount

	chunks := SplitByTime(info, cfg)
	if len(chunks) == 0 {
		return nil, info, fmt.Errorf("split produced 0 chunks for %s", filePath)
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)
	return commands, info, nil
}

// ---------------------------------------------------------------------------
// SplitByWeights 按权重比例拆分（用于异构设备调度）
//
// weights: 每个节点的能力权重（相对比例），例如 [3.0, 1.0, 2.0]
// 表示第一个节点分配 50%，第二个 ~17%，第三个 ~33%
// ---------------------------------------------------------------------------

func SplitByWeights(info *VideoInfo, weights []float64, cfg SplitConfig) []TaskChunk {
	if info.Duration <= 0 || len(weights) == 0 {
		return nil
	}

	totalWeight := 0.0
	for _, w := range weights {
		if w <= 0 {
			w = 0.01
		}
		totalWeight += w
	}

	chunks := make([]TaskChunk, 0, len(weights))
	var offset float64
	remaining := info.Duration

	for i, w := range weights {
		if w <= 0 {
			w = 0.01
		}

		var dur float64
		if i == len(weights)-1 {
			// 最后一个拿走全部剩余
			dur = remaining
		} else {
			ratio := w / totalWeight
			dur = info.Duration * ratio
			if dur > remaining {
				dur = remaining
			}
		}

		if dur <= 0 {
			continue
		}

		start := offset
		end := start + dur

		var startFrame, endFrame, frameCount int64
		if info.FPS > 0 {
			startFrame = int64(math.Round(start * info.FPS))
			endFrame = int64(math.Round(end * info.FPS))
			frameCount = endFrame - startFrame
		}

		chunks = append(chunks, TaskChunk{
			Index:       i,
			TotalChunks: len(weights),
			StartTime:   roundTo3(start),
			EndTime:     roundTo3(end),
			Duration:    roundTo3(dur),
			StartFrame:  startFrame,
			EndFrame:    endFrame,
			FrameCount:  frameCount,
		})

		offset = end
		remaining -= dur
	}

	// 修正 TotalChunks
	for i := range chunks {
		chunks[i].TotalChunks = len(chunks)
	}

	logger.Noticef("[Breaker] split %s by weights into %d chunks (weights=%v)",
		info.FilePath, len(chunks), weights)

	return chunks
}

// ---------------------------------------------------------------------------
// ffprobe JSON 结构体
// ---------------------------------------------------------------------------

type ffprobeResult struct {
	Format  *ffprobeFormat  `json:"format"`
	Streams []ffprobeStream `json:"streams"`
}

type ffprobeFormat struct {
	FormatName     string `json:"format_name"`
	FormatLongName string `json:"format_long_name"`
	Duration       string `json:"duration"`
	BitRate        string `json:"bit_rate"`
	Size           string `json:"size"`
	NbStreams      int    `json:"nb_streams"`
}

type ffprobeStream struct {
	Index      int    `json:"index"`
	CodecType  string `json:"codec_type"`
	CodecName  string `json:"codec_name"`
	Width      int    `json:"width"`
	Height     int    `json:"height"`
	PixFmt     string `json:"pix_fmt"`
	RFrameRate string `json:"r_frame_rate"`
	NbFrames   string `json:"nb_frames"`
	BitRate    string `json:"bit_rate"`
	SampleRate string `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

// ---------------------------------------------------------------------------
// probeGOPSize 使用 ffprobe 探测关键帧间隔
//
// 分析前 300 帧的关键帧分布来估算 GOP 大小
// ---------------------------------------------------------------------------

func probeGOPSize(ffprobePath, filePath string) (int, error) {
	args := []string{
		"-v", "quiet",
		"-select_streams", "v:0",
		"-show_frames",
		"-show_entries", "frame=pict_type",
		"-read_intervals", "%+#300", // 前 300 帧
		"-print_format", "csv",
		filePath,
	}

	cmd := exec.Command(ffprobePath, args...)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	// 解析输出，统计 I 帧间隔
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var iFramePositions []int

	for i, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), ",")
		if len(fields) >= 2 && strings.TrimSpace(fields[1]) == "I" {
			iFramePositions = append(iFramePositions, i)
		}
	}

	if len(iFramePositions) < 2 {
		return 0, fmt.Errorf("not enough I-frames found to estimate GOP size")
	}

	// 计算平均 I 帧间隔
	totalInterval := 0
	for i := 1; i < len(iFramePositions); i++ {
		totalInterval += iFramePositions[i] - iFramePositions[i-1]
	}
	avgGOP := totalInterval / (len(iFramePositions) - 1)

	return avgGOP, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(s))
	return v
}

// parseRational 解析类似 "30000/1001" 或 "30/1" 的分数
func parseRational(s string) float64 {
	s = strings.TrimSpace(s)
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		v, _ := strconv.ParseFloat(s, 64)
		return v
	}
	num, _ := strconv.ParseFloat(parts[0], 64)
	den, _ := strconv.ParseFloat(parts[1], 64)
	if den == 0 {
		return 0
	}
	return num / den
}

// formatDuration 将秒数格式化为 HH:MM:SS.mmm
func formatDuration(seconds float64) string {
	h := int(seconds) / 3600
	m := (int(seconds) % 3600) / 60
	s := math.Mod(seconds, 60)
	return fmt.Sprintf("%02d:%02d:%06.3f", h, m, s)
}

// roundTo3 保留 3 位小数
func roundTo3(v float64) float64 {
	return math.Round(v*1000) / 1000
}
