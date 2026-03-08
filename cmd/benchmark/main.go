package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kregonia/brander_mixer/bootstrap"
	logger "github.com/kregonia/brander_mixer/log"
	"github.com/kregonia/brander_mixer/widget/breaker"
	"github.com/kregonia/brander_mixer/widget/executor"
	"github.com/kregonia/brander_mixer/widget/mixer"
)

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

var (
	inputFile  = flag.String("i", "", "input video file path (required)")
	outputDir  = flag.String("o", "./benchmark_output", "output directory for benchmark results")
	codec      = flag.String("codec", "libx264", "video codec (e.g. libx264, libx265, libsvtav1)")
	preset     = flag.String("preset", "fast", "encoding preset")
	crf        = flag.Int("crf", 23, "CRF quality value (0-51)")
	workers    = flag.Int("workers", 0, "number of simulated worker nodes (0 = auto detect CPU cores)")
	cleanup    = flag.Bool("cleanup", true, "cleanup intermediate files after benchmark")
	skipSingle = flag.Bool("skip-single", false, "skip single-machine benchmark (use previous result)")
	verbose    = flag.Bool("v", false, "verbose output")
)

// ---------------------------------------------------------------------------
// BenchmarkResult stores timing and metadata for one run
// ---------------------------------------------------------------------------

type BenchmarkResult struct {
	Label       string
	InputFile   string
	OutputFile  string
	Codec       string
	Preset      string
	CRF         int
	Workers     int
	ChunkCount  int
	Duration    time.Duration
	OutputSize  int64
	InputSize   int64
	SpeedRatio  float64 // input duration / encode duration
	VideoDurSec float64 // input video duration in seconds
	Success     bool
	Error       string
}

func (r BenchmarkResult) String() string {
	status := "✅ SUCCESS"
	if !r.Success {
		status = "❌ FAILED: " + r.Error
	}

	return fmt.Sprintf(
		"%-25s | %s\n"+
			"  Input:       %s (%s)\n"+
			"  Output:      %s (%s)\n"+
			"  Codec:       %s / %s / crf=%d\n"+
			"  Workers:     %d (chunks=%d)\n"+
			"  Video Dur:   %.2fs\n"+
			"  Encode Time: %v\n"+
			"  Speed Ratio: %.2fx realtime\n",
		r.Label, status,
		r.InputFile, formatSize(r.InputSize),
		r.OutputFile, formatSize(r.OutputSize),
		r.Codec, r.Preset, r.CRF,
		r.Workers, r.ChunkCount,
		r.VideoDurSec,
		r.Duration.Round(time.Millisecond),
		r.SpeedRatio,
	)
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Usage: benchmark -i <input_video> [options]\n\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Init
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bootstrap.Bootstrap(ctx)

	// Check ffmpeg/ffprobe availability
	if err := checkDependencies(); err != nil {
		fmt.Fprintf(os.Stderr, "❌ %v\n", err)
		os.Exit(1)
	}

	// Resolve worker count
	numWorkers := *workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
		if numWorkers > 16 {
			numWorkers = 16
		}
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Probe input
	fmt.Printf("🔍 Probing input video: %s\n", *inputFile)
	videoInfo, err := breaker.Probe(*inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to probe video: %v\n", err)
		os.Exit(1)
	}

	inputSize := int64(0)
	if fi, err := os.Stat(*inputFile); err == nil {
		inputSize = fi.Size()
	}

	printVideoInfo(videoInfo, inputSize)

	// Ensure output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create output dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n📊 Benchmark Configuration:\n")
	fmt.Printf("   Codec:   %s\n", *codec)
	fmt.Printf("   Preset:  %s\n", *preset)
	fmt.Printf("   CRF:     %d\n", *crf)
	fmt.Printf("   Workers: %d (simulated distributed nodes)\n", numWorkers)
	fmt.Printf("   Output:  %s\n\n", *outputDir)

	// -----------------------------------------------------------------------
	// Benchmark 1: Single-machine transcoding
	// -----------------------------------------------------------------------

	var singleResult BenchmarkResult
	singleOutputFile := filepath.Join(*outputDir, "single_output.mp4")

	if *skipSingle {
		fmt.Println("⏭  Skipping single-machine benchmark (--skip-single)")
		// Try to read previous result timing from file
		singleResult = BenchmarkResult{
			Label:       "Single Machine",
			InputFile:   *inputFile,
			OutputFile:  singleOutputFile,
			Codec:       *codec,
			Preset:      *preset,
			CRF:         *crf,
			Workers:     1,
			ChunkCount:  1,
			InputSize:   inputSize,
			VideoDurSec: videoInfo.Duration,
			Success:     true,
		}
		if fi, err := os.Stat(singleOutputFile); err == nil {
			singleResult.OutputSize = fi.Size()
		}
	} else {
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("🖥  Benchmark 1: Single-Machine Transcoding")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		singleResult = runSingleMachine(videoInfo, singleOutputFile, inputSize)
		fmt.Println()
		fmt.Println(singleResult)
	}

	// -----------------------------------------------------------------------
	// Benchmark 2: Distributed split → transcode → merge
	// -----------------------------------------------------------------------

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("🌐 Benchmark 2: Distributed Transcoding (%d workers)\n", numWorkers)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	distOutputFile := filepath.Join(*outputDir, "distributed_output.mp4")
	distResult := runDistributed(ctx, videoInfo, distOutputFile, inputSize, numWorkers)
	fmt.Println()
	fmt.Println(distResult)

	// -----------------------------------------------------------------------
	// Benchmark 3: Naive parallel (equal split, no smart scheduling)
	// -----------------------------------------------------------------------

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("⚙️  Benchmark 3: Naive Parallel (%d equal splits)\n", numWorkers)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	naiveOutputFile := filepath.Join(*outputDir, "naive_output.mp4")
	naiveResult := runNaiveParallel(ctx, videoInfo, naiveOutputFile, inputSize, numWorkers)
	fmt.Println()
	fmt.Println(naiveResult)

	// -----------------------------------------------------------------------
	// Summary report
	// -----------------------------------------------------------------------

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                     BENCHMARK SUMMARY                           ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")

	results := []BenchmarkResult{singleResult, naiveResult, distResult}
	printSummaryTable(results)

	// Speedup calculation
	if singleResult.Success && singleResult.Duration > 0 {
		fmt.Println()
		fmt.Println("📈 Speedup Analysis:")
		singleMs := float64(singleResult.Duration.Milliseconds())
		if singleMs > 0 {
			if naiveResult.Success && naiveResult.Duration > 0 {
				naiveSpeedup := singleMs / float64(naiveResult.Duration.Milliseconds())
				naiveEfficiency := naiveSpeedup / float64(numWorkers) * 100
				fmt.Printf("   Naive Parallel:  %.2fx speedup (%.1f%% efficiency with %d workers)\n",
					naiveSpeedup, naiveEfficiency, numWorkers)
			}
			if distResult.Success && distResult.Duration > 0 {
				distSpeedup := singleMs / float64(distResult.Duration.Milliseconds())
				distEfficiency := distSpeedup / float64(numWorkers) * 100
				fmt.Printf("   Smart Schedule:  %.2fx speedup (%.1f%% efficiency with %d workers)\n",
					distSpeedup, distEfficiency, numWorkers)
			}
			if naiveResult.Success && distResult.Success &&
				naiveResult.Duration > 0 && distResult.Duration > 0 {
				improvement := (float64(naiveResult.Duration.Milliseconds()) - float64(distResult.Duration.Milliseconds())) /
					float64(naiveResult.Duration.Milliseconds()) * 100
				if improvement > 0 {
					fmt.Printf("   Smart vs Naive:  %.1f%% faster with intelligent scheduling\n", improvement)
				} else {
					fmt.Printf("   Smart vs Naive:  %.1f%% overhead (scheduling cost > benefit at this scale)\n", -improvement)
				}
			}
		}
	}

	fmt.Println()
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")

	// Cleanup
	if *cleanup {
		fmt.Println("\n🧹 Cleaning up intermediate files...")
		cleanupDir := filepath.Join(*outputDir, "chunks")
		os.RemoveAll(cleanupDir)
		cleanupDir2 := filepath.Join(*outputDir, "naive_chunks")
		os.RemoveAll(cleanupDir2)
		fmt.Println("   Done.")
	}
}

// ---------------------------------------------------------------------------
// runSingleMachine: baseline single FFmpeg invocation
// ---------------------------------------------------------------------------

func runSingleMachine(info *breaker.VideoInfo, outputFile string, inputSize int64) BenchmarkResult {
	result := BenchmarkResult{
		Label:       "Single Machine",
		InputFile:   info.FilePath,
		OutputFile:  outputFile,
		Codec:       *codec,
		Preset:      *preset,
		CRF:         *crf,
		Workers:     1,
		ChunkCount:  1,
		InputSize:   inputSize,
		VideoDurSec: info.Duration,
	}

	args := []string{
		"-y",
		"-i", info.FilePath,
		"-c:v", *codec,
		"-preset", *preset,
		"-crf", fmt.Sprintf("%d", *crf),
	}

	if info.AudioCodec != "" {
		args = append(args, "-c:a", "aac", "-b:a", "128k")
	} else {
		args = append(args, "-an")
	}

	args = append(args, outputFile)

	fmt.Printf("   ⏳ Running: ffmpeg %s\n", strings.Join(args, " "))

	start := time.Now()
	cmd := exec.Command("ffmpeg", args...)
	if *verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	if err := cmd.Run(); err != nil {
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}

	result.Duration = time.Since(start)
	result.Success = true

	if fi, err := os.Stat(outputFile); err == nil {
		result.OutputSize = fi.Size()
	}

	if result.Duration.Seconds() > 0 {
		result.SpeedRatio = info.Duration / result.Duration.Seconds()
	}

	fmt.Printf("   ✅ Completed in %v\n", result.Duration.Round(time.Millisecond))

	return result
}

// ---------------------------------------------------------------------------
// runDistributed: smart split → concurrent transcode → merge
//
// Uses breaker.SplitByWeights to simulate heterogeneous node capacities
// (some workers get more work based on simulated capability differences).
// ---------------------------------------------------------------------------

func runDistributed(ctx context.Context, info *breaker.VideoInfo, outputFile string, inputSize int64, numWorkers int) BenchmarkResult {
	result := BenchmarkResult{
		Label:       "Smart Schedule",
		InputFile:   info.FilePath,
		OutputFile:  outputFile,
		Codec:       *codec,
		Preset:      *preset,
		CRF:         *crf,
		Workers:     numWorkers,
		InputSize:   inputSize,
		VideoDurSec: info.Duration,
	}

	totalStart := time.Now()

	// Phase 1: Smart split (simulate heterogeneous node weights)
	fmt.Println("   📐 Phase 1: Intelligent task splitting...")

	splitStart := time.Now()

	// Simulate varied node capabilities: worker 0 is strongest,
	// worker N-1 is weakest. This mimics real heterogeneous edge devices.
	weights := make([]float64, numWorkers)
	for i := 0; i < numWorkers; i++ {
		// Linearly decreasing: 1.0 ... 0.5
		weights[i] = 1.0 - float64(i)*0.5/float64(numWorkers)
		if weights[i] < 0.2 {
			weights[i] = 0.2
		}
	}

	chunks := breaker.SplitByWeights(info, weights, breaker.SplitConfig{
		Codec:     *codec,
		Preset:    *preset,
		CRF:       *crf,
		OutputFmt: "mp4",
	})

	if len(chunks) == 0 {
		result.Error = "split produced 0 chunks"
		result.Duration = time.Since(totalStart)
		return result
	}

	result.ChunkCount = len(chunks)
	splitDur := time.Since(splitStart)
	fmt.Printf("   ✅ Split into %d chunks (weights=%v) in %v\n", len(chunks), formatWeights(weights), splitDur)

	for _, c := range chunks {
		fmt.Printf("      chunk[%d]: %.3fs - %.3fs (duration=%.3fs, ~%d frames)\n",
			c.Index, c.StartTime, c.EndTime, c.Duration, c.FrameCount)
	}

	// Phase 2: Generate FFmpeg commands
	chunkDir := filepath.Join(*outputDir, "chunks")
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		result.Error = "failed to create chunk dir: " + err.Error()
		result.Duration = time.Since(totalStart)
		return result
	}

	cfg := breaker.SplitConfig{
		Codec:         *codec,
		Preset:        *preset,
		CRF:           *crf,
		OutputFmt:     "mp4",
		OutputPattern: filepath.Join(chunkDir, "chunk_%03d.mp4"),
	}
	cmds := breaker.GenerateFFmpegCommands(info, chunks, cfg)

	// Phase 3: Concurrent execution
	fmt.Printf("   ⚡ Phase 2: Concurrent transcoding (%d tasks, max concurrency=%d)...\n",
		len(cmds), numWorkers)

	execStart := time.Now()

	execCfg := executor.DefaultConfig()
	execCfg.MaxConcurrency = numWorkers
	execCfg.ProgressInterval = 2 * time.Second
	execCfg.OnProgress = func(p executor.TaskProgress) {
		if *verbose {
			fmt.Printf("      [progress] task=%s state=%s progress=%.1f%%\n",
				p.TaskID, p.State, p.ProgressPercent)
		}
	}

	exec, err := executor.NewExecutor(ctx, execCfg)
	if err != nil {
		result.Error = "failed to create executor: " + err.Error()
		result.Duration = time.Since(totalStart)
		return result
	}
	defer exec.Shutdown()

	// Build executor tasks from FFmpeg commands
	tasks := make([]executor.ExecTask, 0, len(cmds))
	for i, cmd := range cmds {
		tasks = append(tasks, executor.ExecTask{
			TaskID:      fmt.Sprintf("dist_%03d", i),
			ChunkIndex:  cmd.Chunk.Index,
			TotalChunks: cmd.Chunk.TotalChunks,
			InputFile:   cmd.InputFile,
			OutputFile:  cmd.OutputFile,
			FFmpegArgs:  cmd.Args,
		})
	}

	submitted, submitErr := exec.SubmitBatch(tasks)
	if submitErr != nil {
		logger.Warnf("[Benchmark] some tasks failed to submit: %v", submitErr)
	}
	fmt.Printf("   📦 Submitted %d/%d tasks\n", submitted, len(tasks))

	// Wait for all
	finalStates := exec.WaitForAll(500 * time.Millisecond)

	execDur := time.Since(execStart)

	// Check results
	completed := 0
	failed := 0
	for _, fs := range finalStates {
		if fs.State == executor.TaskStateCompleted {
			completed++
		} else {
			failed++
			if *verbose {
				fmt.Printf("      ❌ task %s: %s - %s\n", fs.TaskID, fs.State, fs.ErrorMessage)
			}
		}
	}

	fmt.Printf("   ✅ Transcoding done in %v (completed=%d, failed=%d)\n",
		execDur.Round(time.Millisecond), completed, failed)

	if failed > 0 && completed == 0 {
		result.Error = fmt.Sprintf("all %d tasks failed", failed)
		result.Duration = time.Since(totalStart)
		return result
	}

	// Phase 4: Merge
	fmt.Println("   🔗 Phase 3: Merging chunks...")
	mergeStart := time.Now()

	mergeResult := mixer.MergeFromBreaker(cmds, mixer.MergeConfig{
		OutputFile:    outputFile,
		Mode:          mixer.MergeModeConcat,
		CleanupChunks: false,
		ReEncode:      false,
		AudioCodec:    "copy",
	})

	mergeDur := time.Since(mergeStart)

	if mergeResult.Error != nil {
		result.Error = "merge failed: " + mergeResult.Error.Error()
		result.Duration = time.Since(totalStart)
		return result
	}

	fmt.Printf("   ✅ Merge completed in %v (output=%s)\n",
		mergeDur.Round(time.Millisecond), formatSize(mergeResult.OutputSize))

	// Phase 5: Validate
	fmt.Println("   🔎 Phase 4: Validating output...")
	if err := mixer.Validate(outputFile, info.Duration, 5.0); err != nil {
		fmt.Printf("   ⚠️  Validation warning: %v\n", err)
	} else {
		fmt.Println("   ✅ Validation passed")
	}

	result.Duration = time.Since(totalStart)
	result.Success = true
	result.OutputSize = mergeResult.OutputSize

	if result.Duration.Seconds() > 0 {
		result.SpeedRatio = info.Duration / result.Duration.Seconds()
	}

	// Print phase breakdown
	fmt.Printf("\n   ⏱  Phase Breakdown:\n")
	fmt.Printf("      Split:     %v\n", splitDur.Round(time.Millisecond))
	fmt.Printf("      Transcode: %v\n", execDur.Round(time.Millisecond))
	fmt.Printf("      Merge:     %v\n", mergeDur.Round(time.Millisecond))
	fmt.Printf("      Total:     %v\n", result.Duration.Round(time.Millisecond))
	overhead := result.Duration - execDur
	fmt.Printf("      Overhead:  %v (%.1f%%)\n",
		overhead.Round(time.Millisecond),
		float64(overhead.Milliseconds())/float64(result.Duration.Milliseconds())*100)

	return result
}

// ---------------------------------------------------------------------------
// runNaiveParallel: equal-split parallel transcoding (no smart scheduling)
//
// This is the "dumb" baseline for distributed: split video into N equal
// parts and transcode all in parallel. No consideration of heterogeneous
// node capabilities.
// ---------------------------------------------------------------------------

func runNaiveParallel(ctx context.Context, info *breaker.VideoInfo, outputFile string, inputSize int64, numWorkers int) BenchmarkResult {
	result := BenchmarkResult{
		Label:       "Naive Parallel",
		InputFile:   info.FilePath,
		OutputFile:  outputFile,
		Codec:       *codec,
		Preset:      *preset,
		CRF:         *crf,
		Workers:     numWorkers,
		InputSize:   inputSize,
		VideoDurSec: info.Duration,
	}

	totalStart := time.Now()

	// Equal split
	chunkDir := filepath.Join(*outputDir, "naive_chunks")
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		result.Error = "failed to create chunk dir: " + err.Error()
		result.Duration = time.Since(totalStart)
		return result
	}

	cfg := breaker.SplitConfig{
		ChunkCount:    numWorkers,
		Codec:         *codec,
		Preset:        *preset,
		CRF:           *crf,
		OutputFmt:     "mp4",
		OutputPattern: filepath.Join(chunkDir, "naive_%03d.mp4"),
	}

	chunks := breaker.SplitByTime(info, cfg)
	if len(chunks) == 0 {
		result.Error = "split produced 0 chunks"
		result.Duration = time.Since(totalStart)
		return result
	}
	result.ChunkCount = len(chunks)

	cmds := breaker.GenerateFFmpegCommands(info, chunks, cfg)

	fmt.Printf("   📐 Split into %d equal chunks\n", len(chunks))

	// Concurrent execution
	fmt.Printf("   ⚡ Concurrent transcoding (%d tasks)...\n", len(cmds))
	execStart := time.Now()

	execCfg := executor.DefaultConfig()
	execCfg.MaxConcurrency = numWorkers

	exec, err := executor.NewExecutor(ctx, execCfg)
	if err != nil {
		result.Error = "failed to create executor: " + err.Error()
		result.Duration = time.Since(totalStart)
		return result
	}
	defer exec.Shutdown()

	tasks := make([]executor.ExecTask, 0, len(cmds))
	for i, cmd := range cmds {
		tasks = append(tasks, executor.ExecTask{
			TaskID:      fmt.Sprintf("naive_%03d", i),
			ChunkIndex:  cmd.Chunk.Index,
			TotalChunks: cmd.Chunk.TotalChunks,
			InputFile:   cmd.InputFile,
			OutputFile:  cmd.OutputFile,
			FFmpegArgs:  cmd.Args,
		})
	}

	exec.SubmitBatch(tasks)
	exec.WaitForAll(500 * time.Millisecond)

	execDur := time.Since(execStart)
	fmt.Printf("   ✅ Transcoding done in %v\n", execDur.Round(time.Millisecond))

	// Merge
	mergeResult := mixer.MergeFromBreaker(cmds, mixer.MergeConfig{
		OutputFile:    outputFile,
		Mode:          mixer.MergeModeConcat,
		CleanupChunks: false,
		ReEncode:      false,
		AudioCodec:    "copy",
	})

	if mergeResult.Error != nil {
		result.Error = "merge failed: " + mergeResult.Error.Error()
		result.Duration = time.Since(totalStart)
		return result
	}

	result.Duration = time.Since(totalStart)
	result.Success = true
	result.OutputSize = mergeResult.OutputSize

	if result.Duration.Seconds() > 0 {
		result.SpeedRatio = info.Duration / result.Duration.Seconds()
	}

	fmt.Printf("   ✅ Total: %v\n", result.Duration.Round(time.Millisecond))

	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func checkDependencies() error {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return fmt.Errorf("ffmpeg not found in PATH. Please install FFmpeg first")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		return fmt.Errorf("ffprobe not found in PATH. Please install FFmpeg first")
	}
	return nil
}

func printVideoInfo(info *breaker.VideoInfo, inputSize int64) {
	fmt.Printf("\n📁 Video Information:\n")
	fmt.Printf("   File:       %s\n", info.FilePath)
	fmt.Printf("   Format:     %s\n", info.FormatName)
	fmt.Printf("   Duration:   %.2fs\n", info.Duration)
	fmt.Printf("   Resolution: %dx%d\n", info.Width, info.Height)
	fmt.Printf("   FPS:        %.2f\n", info.FPS)
	fmt.Printf("   Codec:      %s\n", info.VideoCodec)
	fmt.Printf("   Frames:     %d\n", info.TotalFrames)
	fmt.Printf("   Bitrate:    %s/s\n", formatSize(info.BitRate/8))
	fmt.Printf("   File Size:  %s\n", formatSize(inputSize))
	if info.GOPSize > 0 {
		fmt.Printf("   GOP Size:   %d frames\n", info.GOPSize)
	}
	if info.AudioCodec != "" {
		fmt.Printf("   Audio:      %s (%d ch, %d Hz)\n",
			info.AudioCodec, info.AudioChannels, info.AudioSampleRate)
	}
}

func printSummaryTable(results []BenchmarkResult) {
	fmt.Printf("║ %-18s │ %10s │ %10s │ %8s │ %6s ║\n",
		"Method", "Time", "Output", "Speed", "Status")
	fmt.Println("╟────────────────────┼────────────┼────────────┼──────────┼────────╢")

	for _, r := range results {
		status := "✅"
		if !r.Success {
			status = "❌"
		}

		timeStr := r.Duration.Round(time.Millisecond).String()
		sizeStr := formatSize(r.OutputSize)
		speedStr := fmt.Sprintf("%.2fx", r.SpeedRatio)

		fmt.Printf("║ %-18s │ %10s │ %10s │ %8s │ %6s ║\n",
			r.Label, timeStr, sizeStr, speedStr, status)
	}

	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
}

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

func formatWeights(w []float64) string {
	parts := make([]string, len(w))
	for i, v := range w {
		parts[i] = fmt.Sprintf("%.2f", v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
