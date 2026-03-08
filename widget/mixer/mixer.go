package mixer

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	logger "github.com/kregonia/brander_mixer/log"
	"github.com/kregonia/brander_mixer/widget/breaker"
)

// ---------------------------------------------------------------------------
// MergeConfig 合并配置
// ---------------------------------------------------------------------------

type MergeConfig struct {
	// 输出文件路径
	OutputFile string

	// 合并模式
	Mode MergeMode

	// 是否在合并完成后删除分片文件
	CleanupChunks bool

	// 是否重新编码（默认 false，使用 stream copy 速度最快）
	ReEncode bool

	// 重新编码参数（仅 ReEncode=true 时生效）
	Codec  string // 如 "libx264"
	Preset string // 如 "medium"
	CRF    int

	// 音频处理
	AudioCodec string // 如 "aac", "copy"

	// 额外 FFmpeg 参数
	ExtraArgs []string

	// 超时（秒），0 表示不限
	TimeoutSeconds int
}

// MergeMode 合并模式
type MergeMode int

const (
	// MergeModeConcat 使用 FFmpeg concat demuxer（最快，要求分片格式一致）
	MergeModeConcat MergeMode = iota

	// MergeModeProtocol 使用 concat protocol（适用于 MPEG-TS 等流式格式）
	MergeModeProtocol

	// MergeModeFilter 使用 concat filter（最灵活，但需要重新编码）
	MergeModeFilter
)

func (m MergeMode) String() string {
	switch m {
	case MergeModeConcat:
		return "concat_demuxer"
	case MergeModeProtocol:
		return "concat_protocol"
	case MergeModeFilter:
		return "concat_filter"
	}
	return "unknown"
}

// DefaultMergeConfig 默认合并配置
func DefaultMergeConfig(outputFile string) MergeConfig {
	return MergeConfig{
		OutputFile:    outputFile,
		Mode:          MergeModeConcat,
		CleanupChunks: false,
		ReEncode:      false,
		AudioCodec:    "copy",
	}
}

// ---------------------------------------------------------------------------
// MergeResult 合并结果
// ---------------------------------------------------------------------------

type MergeResult struct {
	OutputFile string
	Success    bool
	Error      error
	Duration   time.Duration      // 合并耗时
	OutputSize int64              // 输出文件大小（bytes）
	ChunkCount int                // 合并的分片数
	CleanedUp  bool               // 是否已清理分片
	OutputInfo *breaker.VideoInfo // 输出文件的探测信息（可选）
}

// ---------------------------------------------------------------------------
// ChunkFile 待合并的分片文件描述
// ---------------------------------------------------------------------------

type ChunkFile struct {
	Index    int     // 分片索引
	FilePath string  // 文件路径
	Size     int64   // 文件大小（bytes）
	Duration float64 // 时长（秒），0 表示未知
}

// ---------------------------------------------------------------------------
// Merge 合并分片文件为完整输出
//
// chunkFiles: 待合并的分片文件列表（会按 Index 自动排序）
// cfg: 合并配置
// ---------------------------------------------------------------------------

func Merge(chunkFiles []ChunkFile, cfg MergeConfig) *MergeResult {
	startTime := time.Now()

	result := &MergeResult{
		OutputFile: cfg.OutputFile,
		ChunkCount: len(chunkFiles),
	}

	// 参数校验
	if len(chunkFiles) == 0 {
		result.Error = fmt.Errorf("no chunk files provided")
		return result
	}

	if cfg.OutputFile == "" {
		result.Error = fmt.Errorf("output file path is empty")
		return result
	}

	// 按索引排序
	sort.Slice(chunkFiles, func(i, j int) bool {
		return chunkFiles[i].Index < chunkFiles[j].Index
	})

	// 验证分片文件存在性和连续性
	if err := validateChunkFiles(chunkFiles); err != nil {
		result.Error = fmt.Errorf("chunk validation failed: %w", err)
		return result
	}

	// 确保输出目录存在
	outputDir := filepath.Dir(cfg.OutputFile)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			result.Error = fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
			return result
		}
	}

	// 根据合并模式执行
	var err error
	switch cfg.Mode {
	case MergeModeConcat:
		err = mergeWithConcatDemuxer(chunkFiles, cfg)
	case MergeModeProtocol:
		err = mergeWithConcatProtocol(chunkFiles, cfg)
	case MergeModeFilter:
		err = mergeWithConcatFilter(chunkFiles, cfg)
	default:
		err = mergeWithConcatDemuxer(chunkFiles, cfg)
	}

	if err != nil {
		result.Error = err
		result.Duration = time.Since(startTime)
		return result
	}

	// 获取输出文件信息
	if fileInfo, err := os.Stat(cfg.OutputFile); err == nil {
		result.OutputSize = fileInfo.Size()
	}

	result.Success = true
	result.Duration = time.Since(startTime)

	logger.Noticef("[Mixer] merge completed: %d chunks -> %s (size=%s, elapsed=%v, mode=%s)",
		len(chunkFiles), cfg.OutputFile, formatSize(result.OutputSize), result.Duration, cfg.Mode)

	// 清理分片文件
	if cfg.CleanupChunks {
		cleanedCount := cleanupChunks(chunkFiles)
		result.CleanedUp = true
		logger.Noticef("[Mixer] cleaned up %d chunk files", cleanedCount)
	}

	return result
}

// ---------------------------------------------------------------------------
// MergeFromDirectory 从目录中自动发现分片文件并合并
//
// 按文件名排序，适用于分片文件命名为 chunk_000.mp4, chunk_001.mp4 ... 的场景
// ---------------------------------------------------------------------------

func MergeFromDirectory(chunkDir string, pattern string, cfg MergeConfig) *MergeResult {
	if pattern == "" {
		pattern = "chunk_*.mp4"
	}

	fullPattern := filepath.Join(chunkDir, pattern)
	matches, err := filepath.Glob(fullPattern)
	if err != nil {
		return &MergeResult{
			OutputFile: cfg.OutputFile,
			Error:      fmt.Errorf("failed to glob pattern %s: %w", fullPattern, err),
		}
	}

	if len(matches) == 0 {
		return &MergeResult{
			OutputFile: cfg.OutputFile,
			Error:      fmt.Errorf("no files matched pattern %s", fullPattern),
		}
	}

	// 按文件名排序
	sort.Strings(matches)

	chunkFiles := make([]ChunkFile, 0, len(matches))
	for i, path := range matches {
		var size int64
		if fi, err := os.Stat(path); err == nil {
			size = fi.Size()
		}
		chunkFiles = append(chunkFiles, ChunkFile{
			Index:    i,
			FilePath: path,
			Size:     size,
		})
	}

	logger.Noticef("[Mixer] discovered %d chunk files in %s (pattern=%s)", len(chunkFiles), chunkDir, pattern)

	return Merge(chunkFiles, cfg)
}

// ---------------------------------------------------------------------------
// MergeFromBreaker 直接从 breaker 的 FFmpegCommand 结果中合并
//
// 这个方法接收 breaker.GenerateFFmpegCommands 的输出，
// 方便在 breaker → execute → mixer 管线中直接调用。
// ---------------------------------------------------------------------------

func MergeFromBreaker(commands []breaker.FFmpegCommand, cfg MergeConfig) *MergeResult {
	if len(commands) == 0 {
		return &MergeResult{
			OutputFile: cfg.OutputFile,
			Error:      fmt.Errorf("no FFmpeg commands provided"),
		}
	}

	chunkFiles := make([]ChunkFile, 0, len(commands))
	for _, cmd := range commands {
		var size int64
		if fi, err := os.Stat(cmd.OutputFile); err == nil {
			size = fi.Size()
		}
		chunkFiles = append(chunkFiles, ChunkFile{
			Index:    cmd.Chunk.Index,
			FilePath: cmd.OutputFile,
			Size:     size,
			Duration: cmd.Chunk.Duration,
		})
	}

	return Merge(chunkFiles, cfg)
}

// ---------------------------------------------------------------------------
// Validate 验证合并后的输出文件完整性
//
// 通过 ffprobe 探测输出文件，与预期时长比较
// ---------------------------------------------------------------------------

func Validate(outputFile string, expectedDuration float64, tolerancePercent float64) error {
	if tolerancePercent <= 0 {
		tolerancePercent = 2.0 // 默认允许 2% 误差
	}

	info, err := breaker.Probe(outputFile)
	if err != nil {
		return fmt.Errorf("validation probe failed: %w", err)
	}

	if info.Duration <= 0 {
		return fmt.Errorf("output file has zero duration")
	}

	if expectedDuration > 0 {
		diff := math.Abs(info.Duration - expectedDuration)
		diffPercent := (diff / expectedDuration) * 100

		if diffPercent > tolerancePercent {
			return fmt.Errorf(
				"duration mismatch: expected=%.3fs, actual=%.3fs, diff=%.2f%% (tolerance=%.2f%%)",
				expectedDuration, info.Duration, diffPercent, tolerancePercent,
			)
		}

		logger.Noticef("[Mixer] validation passed: duration=%.3fs (expected=%.3fs, diff=%.2f%%)",
			info.Duration, expectedDuration, diffPercent)
	} else {
		logger.Noticef("[Mixer] validation passed: duration=%.3fs (no expected duration provided)", info.Duration)
	}

	return nil
}

// ---------------------------------------------------------------------------
// mergeWithConcatDemuxer 使用 FFmpeg concat demuxer 合并
//
// 这是最常用的合并方式，速度最快（stream copy），
// 要求所有分片使用相同的编码参数。
//
// 步骤：
//   1. 生成 concat 列表文件
//   2. 调用 ffmpeg -f concat -i list.txt -c copy output.mp4
// ---------------------------------------------------------------------------

func mergeWithConcatDemuxer(chunkFiles []ChunkFile, cfg MergeConfig) error {
	// 生成 concat 列表文件
	listFile, err := createConcatListFile(chunkFiles)
	if err != nil {
		return fmt.Errorf("failed to create concat list: %w", err)
	}
	defer os.Remove(listFile)

	args := []string{
		"-y",
		"-f", "concat",
		"-safe", "0",
		"-i", listFile,
	}

	if cfg.ReEncode {
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
		args = append(args,
			"-c:v", codec,
			"-preset", preset,
			"-crf", strconv.Itoa(crf),
		)

		audioCodec := cfg.AudioCodec
		if audioCodec == "" {
			audioCodec = "aac"
		}
		args = append(args, "-c:a", audioCodec)
	} else {
		args = append(args, "-c", "copy")
	}

	// 额外参数
	if len(cfg.ExtraArgs) > 0 {
		args = append(args, cfg.ExtraArgs...)
	}

	args = append(args, cfg.OutputFile)

	return runFFmpeg(args, cfg.TimeoutSeconds)
}

// ---------------------------------------------------------------------------
// mergeWithConcatProtocol 使用 FFmpeg concat protocol 合并
//
// 适用于 MPEG-TS 等无需 demux 即可直接拼接的格式。
// 命令：ffmpeg -i "concat:chunk0.ts|chunk1.ts|..." -c copy output.mp4
// ---------------------------------------------------------------------------

func mergeWithConcatProtocol(chunkFiles []ChunkFile, cfg MergeConfig) error {
	paths := make([]string, len(chunkFiles))
	for i, cf := range chunkFiles {
		absPath, err := filepath.Abs(cf.FilePath)
		if err != nil {
			absPath = cf.FilePath
		}
		paths[i] = absPath
	}

	concatInput := "concat:" + strings.Join(paths, "|")

	args := []string{
		"-y",
		"-i", concatInput,
	}

	if cfg.ReEncode {
		codec := cfg.Codec
		if codec == "" {
			codec = "libx264"
		}
		args = append(args, "-c:v", codec)

		if cfg.Preset != "" {
			args = append(args, "-preset", cfg.Preset)
		}
		if cfg.CRF > 0 {
			args = append(args, "-crf", strconv.Itoa(cfg.CRF))
		}

		audioCodec := cfg.AudioCodec
		if audioCodec == "" {
			audioCodec = "aac"
		}
		args = append(args, "-c:a", audioCodec)
	} else {
		args = append(args, "-c", "copy")
	}

	if len(cfg.ExtraArgs) > 0 {
		args = append(args, cfg.ExtraArgs...)
	}

	args = append(args, cfg.OutputFile)

	return runFFmpeg(args, cfg.TimeoutSeconds)
}

// ---------------------------------------------------------------------------
// mergeWithConcatFilter 使用 FFmpeg concat filter 合并
//
// 最灵活的方式，支持不同编码参数的分片合并，但需要重新编码（较慢）。
// 命令：ffmpeg -i chunk0.mp4 -i chunk1.mp4 ... -filter_complex concat=n=N:v=1:a=1 output.mp4
// ---------------------------------------------------------------------------

func mergeWithConcatFilter(chunkFiles []ChunkFile, cfg MergeConfig) error {
	args := []string{"-y"}

	// 添加所有输入文件
	for _, cf := range chunkFiles {
		args = append(args, "-i", cf.FilePath)
	}

	// 构建 filter_complex 参数
	hasAudio := true // 假设有音频，如果没有可以外部配置
	audioFlag := 1
	if !hasAudio {
		audioFlag = 0
	}

	filterParts := make([]string, 0, len(chunkFiles))
	for i := range chunkFiles {
		if hasAudio {
			filterParts = append(filterParts, fmt.Sprintf("[%d:v:0][%d:a:0]", i, i))
		} else {
			filterParts = append(filterParts, fmt.Sprintf("[%d:v:0]", i))
		}
	}

	filterStr := fmt.Sprintf("%sconcat=n=%d:v=1:a=%d[outv]",
		strings.Join(filterParts, ""), len(chunkFiles), audioFlag)
	if hasAudio {
		filterStr = fmt.Sprintf("%sconcat=n=%d:v=1:a=%d[outv][outa]",
			strings.Join(filterParts, ""), len(chunkFiles), audioFlag)
	}

	args = append(args, "-filter_complex", filterStr)

	if hasAudio {
		args = append(args, "-map", "[outv]", "-map", "[outa]")
	} else {
		args = append(args, "-map", "[outv]")
	}

	// 编码参数
	codec := cfg.Codec
	if codec == "" {
		codec = "libx264"
	}
	args = append(args, "-c:v", codec)

	if cfg.Preset != "" {
		args = append(args, "-preset", cfg.Preset)
	}
	if cfg.CRF > 0 {
		args = append(args, "-crf", strconv.Itoa(cfg.CRF))
	}

	if hasAudio {
		audioCodec := cfg.AudioCodec
		if audioCodec == "" {
			audioCodec = "aac"
		}
		args = append(args, "-c:a", audioCodec)
	}

	if len(cfg.ExtraArgs) > 0 {
		args = append(args, cfg.ExtraArgs...)
	}

	args = append(args, cfg.OutputFile)

	return runFFmpeg(args, cfg.TimeoutSeconds)
}

// ---------------------------------------------------------------------------
// createConcatListFile 生成 FFmpeg concat demuxer 需要的列表文件
//
// 格式：
//   file '/absolute/path/to/chunk_000.mp4'
//   file '/absolute/path/to/chunk_001.mp4'
//   ...
// ---------------------------------------------------------------------------

func createConcatListFile(chunkFiles []ChunkFile) (string, error) {
	tmpFile, err := os.CreateTemp("", "brander_concat_*.txt")
	if err != nil {
		return "", fmt.Errorf("failed to create temp concat list file: %w", err)
	}

	writer := bufio.NewWriter(tmpFile)

	for _, cf := range chunkFiles {
		absPath, err := filepath.Abs(cf.FilePath)
		if err != nil {
			absPath = cf.FilePath
		}
		// 路径中的单引号需要转义
		escaped := strings.ReplaceAll(absPath, "'", "'\\''")
		line := fmt.Sprintf("file '%s'\n", escaped)
		if _, err := writer.WriteString(line); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return "", fmt.Errorf("failed to write concat list: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return "", err
	}

	tmpFile.Close()

	logger.Noticef("[Mixer] created concat list file: %s (%d entries)", tmpFile.Name(), len(chunkFiles))
	return tmpFile.Name(), nil
}

// ---------------------------------------------------------------------------
// validateChunkFiles 验证分片文件
// ---------------------------------------------------------------------------

func validateChunkFiles(chunkFiles []ChunkFile) error {
	for i, cf := range chunkFiles {
		if cf.FilePath == "" {
			return fmt.Errorf("chunk %d has empty file path", i)
		}

		fi, err := os.Stat(cf.FilePath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("chunk %d file does not exist: %s", cf.Index, cf.FilePath)
			}
			return fmt.Errorf("chunk %d file stat failed: %w", cf.Index, err)
		}

		if fi.Size() == 0 {
			return fmt.Errorf("chunk %d file is empty: %s", cf.Index, cf.FilePath)
		}

		// 更新实际大小
		chunkFiles[i].Size = fi.Size()
	}

	// 检查索引连续性（警告，不强制失败）
	for i := 1; i < len(chunkFiles); i++ {
		if chunkFiles[i].Index != chunkFiles[i-1].Index+1 {
			logger.Warnf("[Mixer] chunk index gap detected: chunk[%d].Index=%d, chunk[%d].Index=%d",
				i-1, chunkFiles[i-1].Index, i, chunkFiles[i].Index)
		}
	}

	totalSize := int64(0)
	for _, cf := range chunkFiles {
		totalSize += cf.Size
	}
	logger.Noticef("[Mixer] validated %d chunks, total size=%s", len(chunkFiles), formatSize(totalSize))

	return nil
}

// ---------------------------------------------------------------------------
// runFFmpeg 执行 FFmpeg 命令
// ---------------------------------------------------------------------------

func runFFmpeg(args []string, timeoutSeconds int) error {
	ffmpegPath, err := exec.LookPath("ffmpeg")
	if err != nil {
		return fmt.Errorf("ffmpeg not found in PATH: %w", err)
	}

	cmdLine := ffmpegPath + " " + strings.Join(args, " ")
	logger.Noticef("[Mixer] executing: %s", cmdLine)

	cmd := exec.Command(ffmpegPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ffmpeg start failed: %w", err)
	}

	// 如果设置了超时
	if timeoutSeconds > 0 {
		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("ffmpeg exited with error: %w", err)
			}
			return nil
		case <-time.After(time.Duration(timeoutSeconds) * time.Second):
			_ = cmd.Process.Kill()
			return fmt.Errorf("ffmpeg timed out after %d seconds", timeoutSeconds)
		}
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("ffmpeg exited with error: %w", err)
	}

	return nil
}

// ---------------------------------------------------------------------------
// cleanupChunks 清理分片文件
// ---------------------------------------------------------------------------

func cleanupChunks(chunkFiles []ChunkFile) int {
	count := 0
	for _, cf := range chunkFiles {
		if err := os.Remove(cf.FilePath); err != nil {
			logger.Warnf("[Mixer] failed to remove chunk file %s: %v", cf.FilePath, err)
		} else {
			count++
		}
	}
	return count
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
