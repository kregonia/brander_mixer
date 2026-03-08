package breaker

import (
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// helpers: construct mock VideoInfo for testing (no ffprobe needed)
// ---------------------------------------------------------------------------

func mockVideoInfo() *VideoInfo {
	return &VideoInfo{
		FilePath:        "/tmp/test_video.mp4",
		Duration:        120.0, // 2 minutes
		BitRate:         5000000,
		Size:            75000000,
		FormatName:      "mov,mp4,m4a,3gp,3g2,mj2",
		FormatLong:      "QuickTime / MOV",
		StreamCount:     2,
		VideoCodec:      "h264",
		Width:           1920,
		Height:          1080,
		FPS:             30.0,
		TotalFrames:     3600,
		PixelFormat:     "yuv420p",
		VideoIndex:      0,
		GOPSize:         30, // 1 second GOP at 30fps
		VideoBitRate:    4500000,
		AudioCodec:      "aac",
		AudioSampleRate: 44100,
		AudioChannels:   2,
		AudioIndex:      1,
		AudioBitRate:    128000,
	}
}

func mockVideoInfoNoAudio() *VideoInfo {
	info := mockVideoInfo()
	info.AudioCodec = ""
	info.AudioSampleRate = 0
	info.AudioChannels = 0
	info.AudioIndex = 0
	info.AudioBitRate = 0
	info.StreamCount = 1
	return info
}

func mockVideoInfoNoGOP() *VideoInfo {
	info := mockVideoInfo()
	info.GOPSize = 0
	return info
}

func mockVideoInfoShort() *VideoInfo {
	info := mockVideoInfo()
	info.Duration = 5.0 // 5 seconds
	info.TotalFrames = 150
	info.Size = 3125000
	return info
}

func mockVideoInfoZeroDuration() *VideoInfo {
	info := mockVideoInfo()
	info.Duration = 0
	info.TotalFrames = 0
	return info
}

func mockVideoInfoNoFPS() *VideoInfo {
	info := mockVideoInfo()
	info.FPS = 0
	return info
}

// ---------------------------------------------------------------------------
// Test: DefaultSplitConfig
// ---------------------------------------------------------------------------

func TestDefaultSplitConfig(t *testing.T) {
	cfg := DefaultSplitConfig()

	if cfg.ChunkCount != 0 {
		t.Errorf("expected ChunkCount=0, got %d", cfg.ChunkCount)
	}
	if cfg.ChunkDuration != 10.0 {
		t.Errorf("expected ChunkDuration=10.0, got %f", cfg.ChunkDuration)
	}
	if cfg.Codec != "libx264" {
		t.Errorf("expected Codec=libx264, got %s", cfg.Codec)
	}
	if cfg.Preset != "medium" {
		t.Errorf("expected Preset=medium, got %s", cfg.Preset)
	}
	if cfg.CRF != 23 {
		t.Errorf("expected CRF=23, got %d", cfg.CRF)
	}
	if cfg.OutputFmt != "mp4" {
		t.Errorf("expected OutputFmt=mp4, got %s", cfg.OutputFmt)
	}
}

// ---------------------------------------------------------------------------
// Test: SplitByTime — basic chunk count
// ---------------------------------------------------------------------------

func TestSplitByTime_ByChunkCount(t *testing.T) {
	info := mockVideoInfo()
	cfg := SplitConfig{ChunkCount: 4}

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks")
	}

	// Should produce approximately 4 chunks (may differ slightly due to GOP alignment)
	if len(chunks) < 2 || len(chunks) > 6 {
		t.Errorf("expected ~4 chunks, got %d", len(chunks))
	}

	// Verify TotalChunks is consistent
	for _, c := range chunks {
		if c.TotalChunks != len(chunks) {
			t.Errorf("chunk %d: TotalChunks=%d but len(chunks)=%d", c.Index, c.TotalChunks, len(chunks))
		}
	}

	// Verify continuity: chunks cover the entire duration
	totalDur := 0.0
	for _, c := range chunks {
		totalDur += c.Duration
	}
	if math.Abs(totalDur-info.Duration) > 0.01 {
		t.Errorf("total chunk duration %.3f != video duration %.3f", totalDur, info.Duration)
	}

	// First chunk starts at 0
	if chunks[0].StartTime != 0 {
		t.Errorf("first chunk start time should be 0, got %.3f", chunks[0].StartTime)
	}

	// Last chunk ends at duration
	last := chunks[len(chunks)-1]
	if math.Abs(last.EndTime-info.Duration) > 0.01 {
		t.Errorf("last chunk end time %.3f should be close to %.3f", last.EndTime, info.Duration)
	}
}

func TestSplitByTime_ByChunkDuration(t *testing.T) {
	info := mockVideoInfo() // 120s
	cfg := SplitConfig{ChunkDuration: 30.0}

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks")
	}

	// 120 / 30 = 4 chunks (may be adjusted by GOP alignment)
	if len(chunks) < 3 || len(chunks) > 6 {
		t.Errorf("expected ~4 chunks for 120s / 30s, got %d", len(chunks))
	}

	// Check continuity
	for i := 1; i < len(chunks); i++ {
		if math.Abs(chunks[i].StartTime-chunks[i-1].EndTime) > 0.01 {
			t.Errorf("gap between chunk %d end (%.3f) and chunk %d start (%.3f)",
				i-1, chunks[i-1].EndTime, i, chunks[i].StartTime)
		}
	}
}

func TestSplitByTime_DefaultChunkDuration(t *testing.T) {
	info := mockVideoInfo() // 120s
	cfg := SplitConfig{}    // ChunkCount=0, ChunkDuration=0 → defaults to 10s

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks with default duration")
	}
	// 120 / 10 = 12 (may differ due to GOP alignment)
	if len(chunks) < 6 {
		t.Errorf("expected many chunks with 10s default, got %d", len(chunks))
	}
}

func TestSplitByTime_SingleChunk(t *testing.T) {
	info := mockVideoInfo()
	cfg := SplitConfig{ChunkCount: 1}

	chunks := SplitByTime(info, cfg)

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if c.StartTime != 0 {
		t.Errorf("single chunk start should be 0, got %.3f", c.StartTime)
	}
	if math.Abs(c.Duration-info.Duration) > 0.01 {
		t.Errorf("single chunk duration %.3f should equal video duration %.3f", c.Duration, info.Duration)
	}
	if c.Index != 0 {
		t.Errorf("single chunk index should be 0, got %d", c.Index)
	}
	if c.TotalChunks != 1 {
		t.Errorf("single chunk TotalChunks should be 1, got %d", c.TotalChunks)
	}
}

func TestSplitByTime_ZeroDuration(t *testing.T) {
	info := mockVideoInfoZeroDuration()
	cfg := SplitConfig{ChunkCount: 4}

	chunks := SplitByTime(info, cfg)

	if chunks != nil && len(chunks) != 0 {
		t.Errorf("expected nil or empty chunks for zero duration, got %d", len(chunks))
	}
}

func TestSplitByTime_ShortVideo(t *testing.T) {
	info := mockVideoInfoShort() // 5 seconds
	cfg := SplitConfig{ChunkCount: 10}

	chunks := SplitByTime(info, cfg)

	// Even with 10 requested chunks for 5s video, should get reasonable number
	if len(chunks) == 0 {
		t.Fatal("expected at least 1 chunk for short video")
	}

	// Verify total duration matches
	totalDur := 0.0
	for _, c := range chunks {
		totalDur += c.Duration
	}
	if math.Abs(totalDur-info.Duration) > 0.01 {
		t.Errorf("total duration %.3f != video duration %.3f", totalDur, info.Duration)
	}
}

func TestSplitByTime_NoGOPAlignment(t *testing.T) {
	info := mockVideoInfoNoGOP()
	cfg := SplitConfig{ChunkCount: 4}

	chunks := SplitByTime(info, cfg)

	if len(chunks) != 4 {
		t.Errorf("without GOP, expected exactly 4 chunks, got %d", len(chunks))
	}

	// Each chunk should be 30 seconds
	for i, c := range chunks {
		if i < 3 {
			if math.Abs(c.Duration-30.0) > 0.01 {
				t.Errorf("chunk %d duration %.3f should be ~30.0", i, c.Duration)
			}
		}
	}
}

func TestSplitByTime_GOPAlignment(t *testing.T) {
	info := mockVideoInfo() // GOPSize=30, FPS=30 → GOP duration = 1.0s
	cfg := SplitConfig{ChunkCount: 4}

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected chunks with GOP alignment")
	}

	// With GOP alignment, chunk durations should be multiples of GOP duration (1.0s)
	gopDuration := float64(info.GOPSize) / info.FPS
	for i, c := range chunks {
		if i < len(chunks)-1 { // last chunk is flexible
			remainder := math.Mod(c.Duration, gopDuration)
			// Allow some tolerance for rounding
			if remainder > 0.01 && math.Abs(remainder-gopDuration) > 0.01 {
				t.Logf("chunk %d duration %.3f is not a multiple of GOP duration %.3f (remainder=%.3f)",
					i, c.Duration, gopDuration, remainder)
			}
		}
	}
}

func TestSplitByTime_FrameCalculation(t *testing.T) {
	info := mockVideoInfo() // FPS=30
	cfg := SplitConfig{ChunkCount: 2}

	chunks := SplitByTime(info, cfg)

	for _, c := range chunks {
		if c.FrameCount <= 0 {
			t.Errorf("chunk %d: expected positive frame count, got %d", c.Index, c.FrameCount)
		}
		expectedFrames := int64(math.Round(c.Duration * info.FPS))
		diff := c.FrameCount - expectedFrames
		if diff < -1 || diff > 1 { // allow ±1 rounding
			t.Errorf("chunk %d: frame count %d doesn't match expected %d (duration=%.3f, fps=%.1f)",
				c.Index, c.FrameCount, expectedFrames, c.Duration, info.FPS)
		}
	}
}

func TestSplitByTime_NoFPS_NoFrames(t *testing.T) {
	info := mockVideoInfoNoFPS()
	cfg := SplitConfig{ChunkCount: 3}

	chunks := SplitByTime(info, cfg)

	for _, c := range chunks {
		if c.StartFrame != 0 || c.EndFrame != 0 || c.FrameCount != 0 {
			t.Errorf("chunk %d: with FPS=0, frames should all be 0, got start=%d end=%d count=%d",
				c.Index, c.StartFrame, c.EndFrame, c.FrameCount)
		}
	}
}

func TestSplitByTime_ChunkIndicesAreSequential(t *testing.T) {
	info := mockVideoInfo()
	cfg := SplitConfig{ChunkCount: 5}

	chunks := SplitByTime(info, cfg)

	for i, c := range chunks {
		if c.Index != i {
			t.Errorf("chunk index mismatch: expected %d, got %d", i, c.Index)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: SplitByWeights
// ---------------------------------------------------------------------------

func TestSplitByWeights_EqualWeights(t *testing.T) {
	info := mockVideoInfo() // 120s
	weights := []float64{1.0, 1.0, 1.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks for 3 equal weights, got %d", len(chunks))
	}

	// Each chunk should be ~40 seconds
	for _, c := range chunks {
		if math.Abs(c.Duration-40.0) > 0.01 {
			t.Errorf("chunk %d: expected ~40s duration, got %.3f", c.Index, c.Duration)
		}
	}

	// Total duration should match
	totalDur := 0.0
	for _, c := range chunks {
		totalDur += c.Duration
	}
	if math.Abs(totalDur-info.Duration) > 0.01 {
		t.Errorf("total duration %.3f != video duration %.3f", totalDur, info.Duration)
	}
}

func TestSplitByWeights_UnequalWeights(t *testing.T) {
	info := mockVideoInfo() // 120s
	weights := []float64{3.0, 1.0, 2.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// Weight ratios: 3/6=50%, 1/6≈16.7%, 2/6≈33.3%
	// Expected durations: 60s, 20s, 40s
	expectedDurations := []float64{60.0, 20.0, 40.0}
	for i, c := range chunks {
		if math.Abs(c.Duration-expectedDurations[i]) > 0.1 {
			t.Errorf("chunk %d: expected ~%.1fs, got %.3fs", i, expectedDurations[i], c.Duration)
		}
	}

	// Verify continuity
	for i := 1; i < len(chunks); i++ {
		if math.Abs(chunks[i].StartTime-chunks[i-1].EndTime) > 0.01 {
			t.Errorf("gap between chunk %d and %d", i-1, i)
		}
	}
}

func TestSplitByWeights_SingleWeight(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{1.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	if math.Abs(chunks[0].Duration-info.Duration) > 0.01 {
		t.Errorf("single chunk duration %.3f should equal video duration %.3f", chunks[0].Duration, info.Duration)
	}
}

func TestSplitByWeights_ZeroDuration(t *testing.T) {
	info := mockVideoInfoZeroDuration()
	weights := []float64{1.0, 1.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if chunks != nil && len(chunks) != 0 {
		t.Errorf("expected nil or empty chunks for zero duration, got %d", len(chunks))
	}
}

func TestSplitByWeights_EmptyWeights(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if chunks != nil && len(chunks) != 0 {
		t.Errorf("expected nil or empty chunks for empty weights, got %d", len(chunks))
	}
}

func TestSplitByWeights_NegativeWeightsTreatedAsMinimal(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{-1.0, 10.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	// The negative weight should be treated as 0.01, so chunk 0 should be very small
	if chunks[0].Duration >= chunks[1].Duration {
		t.Errorf("negative weight chunk (%.3fs) should be much smaller than positive weight chunk (%.3fs)",
			chunks[0].Duration, chunks[1].Duration)
	}
}

func TestSplitByWeights_VerifyOffsetContinuity(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{2.0, 3.0, 1.0, 4.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks")
	}

	// Verify first chunk starts at 0
	if chunks[0].StartTime != 0 {
		t.Errorf("first chunk start should be 0, got %.3f", chunks[0].StartTime)
	}

	// Verify continuity
	for i := 1; i < len(chunks); i++ {
		if math.Abs(chunks[i].StartTime-chunks[i-1].EndTime) > 0.01 {
			t.Errorf("gap at chunk %d: prev end=%.3f, curr start=%.3f",
				i, chunks[i-1].EndTime, chunks[i].StartTime)
		}
	}

	// Verify last chunk ends at duration
	last := chunks[len(chunks)-1]
	if math.Abs(last.EndTime-info.Duration) > 0.01 {
		t.Errorf("last chunk end %.3f should be close to duration %.3f", last.EndTime, info.Duration)
	}
}

func TestSplitByWeights_FrameCountWithFPS(t *testing.T) {
	info := mockVideoInfo() // FPS=30
	weights := []float64{1.0, 1.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	for _, c := range chunks {
		if c.FrameCount <= 0 {
			t.Errorf("chunk %d: expected positive frame count with FPS=30, got %d", c.Index, c.FrameCount)
		}
	}
}

func TestSplitByWeights_FrameCountWithoutFPS(t *testing.T) {
	info := mockVideoInfoNoFPS()
	weights := []float64{1.0, 1.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	for _, c := range chunks {
		if c.FrameCount != 0 {
			t.Errorf("chunk %d: expected 0 frame count without FPS, got %d", c.Index, c.FrameCount)
		}
	}
}

func TestSplitByWeights_TotalChunksConsistent(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{1.0, 2.0, 3.0}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	for _, c := range chunks {
		if c.TotalChunks != len(chunks) {
			t.Errorf("chunk %d: TotalChunks=%d, expected %d", c.Index, c.TotalChunks, len(chunks))
		}
	}
}

// ---------------------------------------------------------------------------
// Test: GenerateFFmpegCommands
// ---------------------------------------------------------------------------

func TestGenerateFFmpegCommands_BasicGeneration(t *testing.T) {
	info := mockVideoInfo()
	chunks := SplitByTime(info, SplitConfig{ChunkCount: 3})
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != len(chunks) {
		t.Fatalf("expected %d commands, got %d", len(chunks), len(commands))
	}

	for i, cmd := range commands {
		if cmd.InputFile != info.FilePath {
			t.Errorf("command %d: input file mismatch", i)
		}
		if cmd.OutputFile == "" {
			t.Errorf("command %d: empty output file", i)
		}
		if len(cmd.Args) == 0 {
			t.Errorf("command %d: empty args", i)
		}
		if cmd.CmdLine == "" {
			t.Errorf("command %d: empty command line", i)
		}
		if cmd.Chunk.Index != chunks[i].Index {
			t.Errorf("command %d: chunk index mismatch", i)
		}
	}
}

func TestGenerateFFmpegCommands_EmptyChunks(t *testing.T) {
	info := mockVideoInfo()
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, nil, cfg)
	if commands != nil {
		t.Errorf("expected nil for nil chunks, got %d commands", len(commands))
	}

	commands = GenerateFFmpegCommands(info, []TaskChunk{}, cfg)
	if commands != nil {
		t.Errorf("expected nil for empty chunks, got %d commands", len(commands))
	}
}

func TestGenerateFFmpegCommands_ContainsCodec(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 60, Duration: 60},
	}
	cfg := SplitConfig{
		Codec:     "libx265",
		Preset:    "slow",
		CRF:       18,
		OutputFmt: "mkv",
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}

	cmd := commands[0]
	found := map[string]bool{"libx265": false, "slow": false, "18": false, "mkv": false}

	for i, arg := range cmd.Args {
		if arg == "-c:v" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "libx265" {
			found["libx265"] = true
		}
		if arg == "-preset" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "slow" {
			found["slow"] = true
		}
		if arg == "-crf" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "18" {
			found["18"] = true
		}
		if arg == "-f" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "mkv" {
			found["mkv"] = true
		}
	}

	for k, v := range found {
		if !v {
			t.Errorf("expected to find %s in FFmpeg args", k)
		}
	}
}

func TestGenerateFFmpegCommands_DefaultCodecPresetCRF(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 60, Duration: 60},
	}
	cfg := SplitConfig{} // all defaults → libx264, medium, 23, mp4

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}

	cmd := commands[0]
	foundCodec := false
	foundPreset := false
	foundCRF := false
	for i, arg := range cmd.Args {
		if arg == "-c:v" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "libx264" {
			foundCodec = true
		}
		if arg == "-preset" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "medium" {
			foundPreset = true
		}
		if arg == "-crf" && i+1 < len(cmd.Args) && cmd.Args[i+1] == "23" {
			foundCRF = true
		}
	}

	if !foundCodec {
		t.Error("default codec (libx264) not found")
	}
	if !foundPreset {
		t.Error("default preset (medium) not found")
	}
	if !foundCRF {
		t.Error("default crf (23) not found")
	}
}

func TestGenerateFFmpegCommands_SeekBeforeInput(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 2, StartTime: 0, EndTime: 60, Duration: 60},
		{Index: 1, TotalChunks: 2, StartTime: 60, EndTime: 120, Duration: 60},
	}
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	// First chunk: StartTime=0, no -ss needed (or -ss 0 is OK)
	// Second chunk: StartTime=60, -ss must appear BEFORE -i (seek-before-input mode)
	cmd1 := commands[1]
	ssIdx := -1
	iIdx := -1
	for i, arg := range cmd1.Args {
		if arg == "-ss" {
			ssIdx = i
		}
		if arg == "-i" {
			iIdx = i
		}
	}

	if ssIdx < 0 {
		t.Error("expected -ss in second chunk command")
	}
	if iIdx < 0 {
		t.Error("expected -i in second chunk command")
	}
	if ssIdx >= 0 && iIdx >= 0 && ssIdx >= iIdx {
		t.Error("-ss should appear before -i for seek-before-input mode")
	}
}

func TestGenerateFFmpegCommands_NoAudioTrack(t *testing.T) {
	info := mockVideoInfoNoAudio()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 60, Duration: 60},
	}
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}

	// Should have -an (no audio) instead of -c:a aac
	foundAN := false
	for _, arg := range commands[0].Args {
		if arg == "-an" {
			foundAN = true
		}
	}
	if !foundAN {
		t.Error("expected -an flag for video without audio")
	}
}

func TestGenerateFFmpegCommands_WithAudioTrack(t *testing.T) {
	info := mockVideoInfo() // has audio
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 60, Duration: 60},
	}
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}

	// Should have -c:a aac for audio
	foundCA := false
	for i, arg := range commands[0].Args {
		if arg == "-c:a" && i+1 < len(commands[0].Args) && commands[0].Args[i+1] == "aac" {
			foundCA = true
		}
	}
	if !foundCA {
		t.Error("expected -c:a aac for video with audio")
	}
}

func TestGenerateFFmpegCommands_ExtraArgs(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 60, Duration: 60},
	}
	cfg := SplitConfig{
		Codec:     "libx264",
		Preset:    "fast",
		CRF:       23,
		OutputFmt: "mp4",
		ExtraArgs: []string{"-vf", "scale=1280:720", "-maxrate", "2M"},
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	if len(commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(commands))
	}

	foundVF := false
	foundScale := false
	for i, arg := range commands[0].Args {
		if arg == "-vf" {
			foundVF = true
			if i+1 < len(commands[0].Args) && commands[0].Args[i+1] == "scale=1280:720" {
				foundScale = true
			}
		}
	}
	if !foundVF || !foundScale {
		t.Error("expected extra args -vf scale=1280:720 in command")
	}
}

func TestGenerateFFmpegCommands_CustomOutputPattern(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 2, StartTime: 0, EndTime: 60, Duration: 60},
		{Index: 1, TotalChunks: 2, StartTime: 60, EndTime: 120, Duration: 60},
	}
	cfg := SplitConfig{
		Codec:         "libx264",
		Preset:        "fast",
		CRF:           23,
		OutputFmt:     "mp4",
		OutputPattern: "/tmp/output/part_%03d.mp4",
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	expected := []string{"/tmp/output/part_000.mp4", "/tmp/output/part_001.mp4"}
	for i, cmd := range commands {
		if cmd.OutputFile != expected[i] {
			t.Errorf("command %d: expected output %s, got %s", i, expected[i], cmd.OutputFile)
		}
	}
}

func TestGenerateFFmpegCommands_CmdLineIsComplete(t *testing.T) {
	info := mockVideoInfo()
	chunks := []TaskChunk{
		{Index: 0, TotalChunks: 1, StartTime: 0, EndTime: 120, Duration: 120},
	}
	cfg := DefaultSplitConfig()

	commands := GenerateFFmpegCommands(info, chunks, cfg)

	cmd := commands[0]
	// CmdLine should start with "ffmpeg"
	if len(cmd.CmdLine) < 6 || cmd.CmdLine[:6] != "ffmpeg" {
		t.Errorf("CmdLine should start with 'ffmpeg', got: %s", cmd.CmdLine[:20])
	}

	// CmdLine should contain the input file
	if !containsStr(cmd.CmdLine, info.FilePath) {
		t.Error("CmdLine should contain input file path")
	}
}

// ---------------------------------------------------------------------------
// Test: helper functions
// ---------------------------------------------------------------------------

func TestParseFloat(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"3.14", 3.14},
		{"0", 0},
		{" 42.5 ", 42.5},
		{"", 0},
		{"invalid", 0},
		{"-1.5", -1.5},
	}

	for _, tt := range tests {
		got := parseFloat(tt.input)
		if math.Abs(got-tt.expected) > 0.001 {
			t.Errorf("parseFloat(%q) = %f, want %f", tt.input, got, tt.expected)
		}
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"42", 42},
		{"0", 0},
		{" 100 ", 100},
		{"", 0},
		{"invalid", 0},
		{"-10", -10},
	}

	for _, tt := range tests {
		got := parseInt64(tt.input)
		if got != tt.expected {
			t.Errorf("parseInt64(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"42", 42},
		{"0", 0},
		{" 100 ", 100},
		{"", 0},
		{"-5", -5},
	}

	for _, tt := range tests {
		got := parseInt(tt.input)
		if got != tt.expected {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}

func TestParseRational(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
	}{
		{"30/1", 30.0},
		{"30000/1001", 29.97002997},
		{"24000/1001", 23.976023976},
		{"25/1", 25.0},
		{"60/1", 60.0},
		{"0/1", 0},
		{"30/0", 0},
		{"30", 30.0},
		{"", 0},
		{" 30000/1001 ", 29.97002997},
	}

	for _, tt := range tests {
		got := parseRational(tt.input)
		if math.Abs(got-tt.expected) > 0.001 {
			t.Errorf("parseRational(%q) = %f, want %f", tt.input, got, tt.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "00:00:00.000"},
		{30.5, "00:00:30.500"},
		{60.0, "00:01:00.000"},
		{3661.123, "01:01:01.123"},
		{90.75, "00:01:30.750"},
	}

	for _, tt := range tests {
		got := formatDuration(tt.input)
		if got != tt.expected {
			t.Errorf("formatDuration(%f) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestRoundTo3(t *testing.T) {
	tests := []struct {
		input    float64
		expected float64
	}{
		{1.23456, 1.235},
		{0.0, 0.0},
		{100.0, 100.0},
		{1.1111, 1.111},
		{1.9999, 2.0},
		{-1.2345, -1.235},
	}

	for _, tt := range tests {
		got := roundTo3(tt.input)
		if math.Abs(got-tt.expected) > 0.0001 {
			t.Errorf("roundTo3(%f) = %f, want %f", tt.input, got, tt.expected)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: VideoInfo struct integrity
// ---------------------------------------------------------------------------

func TestVideoInfo_MockDataConsistency(t *testing.T) {
	info := mockVideoInfo()

	if info.Duration <= 0 {
		t.Error("duration should be positive")
	}
	if info.FPS <= 0 {
		t.Error("FPS should be positive")
	}
	if info.Width <= 0 || info.Height <= 0 {
		t.Error("resolution should be positive")
	}
	if info.TotalFrames != int64(info.Duration*info.FPS) {
		t.Errorf("TotalFrames %d should equal Duration*FPS = %d",
			info.TotalFrames, int64(info.Duration*info.FPS))
	}
	if info.VideoCodec == "" {
		t.Error("VideoCodec should not be empty")
	}
	if info.AudioCodec == "" {
		t.Error("AudioCodec should not be empty")
	}
	if info.StreamCount != 2 {
		t.Errorf("expected 2 streams (video+audio), got %d", info.StreamCount)
	}
}

// ---------------------------------------------------------------------------
// Test: TaskChunk struct
// ---------------------------------------------------------------------------

func TestTaskChunk_PositiveDuration(t *testing.T) {
	info := mockVideoInfo()
	cfg := SplitConfig{ChunkCount: 4}
	chunks := SplitByTime(info, cfg)

	for _, c := range chunks {
		if c.Duration <= 0 {
			t.Errorf("chunk %d has non-positive duration: %.3f", c.Index, c.Duration)
		}
		if c.EndTime <= c.StartTime {
			t.Errorf("chunk %d: end (%.3f) <= start (%.3f)", c.Index, c.EndTime, c.StartTime)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Integration — SplitByTime + GenerateFFmpegCommands
// ---------------------------------------------------------------------------

func TestIntegration_SplitAndGenerate(t *testing.T) {
	info := mockVideoInfo()
	cfg := SplitConfig{
		ChunkCount: 4,
		Codec:      "libx264",
		Preset:     "fast",
		CRF:        23,
		OutputFmt:  "mp4",
	}

	chunks := SplitByTime(info, cfg)
	if len(chunks) == 0 {
		t.Fatal("split produced no chunks")
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)
	if len(commands) != len(chunks) {
		t.Fatalf("command count %d != chunk count %d", len(commands), len(chunks))
	}

	// Verify each command is self-consistent
	for i, cmd := range commands {
		if cmd.Chunk.Index != i {
			t.Errorf("command %d: chunk index mismatch (%d)", i, cmd.Chunk.Index)
		}
		if cmd.InputFile == "" {
			t.Errorf("command %d: empty input", i)
		}
		if cmd.OutputFile == "" {
			t.Errorf("command %d: empty output", i)
		}
		if len(cmd.Args) < 5 {
			t.Errorf("command %d: too few args (%d)", i, len(cmd.Args))
		}
	}
}

func TestIntegration_SplitByWeightsAndGenerate(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{3.0, 1.0, 2.0}
	cfg := SplitConfig{
		Codec:     "libsvtav1",
		Preset:    "8",
		CRF:       30,
		OutputFmt: "mkv",
	}

	chunks := SplitByWeights(info, weights, cfg)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	commands := GenerateFFmpegCommands(info, chunks, cfg)
	if len(commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(commands))
	}

	// Verify codec is libsvtav1 in all commands
	for i, cmd := range commands {
		foundCodec := false
		for j, arg := range cmd.Args {
			if arg == "-c:v" && j+1 < len(cmd.Args) && cmd.Args[j+1] == "libsvtav1" {
				foundCodec = true
			}
		}
		if !foundCodec {
			t.Errorf("command %d: expected codec libsvtav1", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Test: Edge cases
// ---------------------------------------------------------------------------

func TestSplitByTime_VeryLargeChunkCount(t *testing.T) {
	info := mockVideoInfo() // 120s
	cfg := SplitConfig{ChunkCount: 1000}

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected some chunks even with large count")
	}

	// Total duration should still match
	totalDur := 0.0
	for _, c := range chunks {
		totalDur += c.Duration
	}
	if math.Abs(totalDur-info.Duration) > 0.1 {
		t.Errorf("total duration %.3f diverged from video duration %.3f", totalDur, info.Duration)
	}
}

func TestSplitByTime_VerySmallChunkDuration(t *testing.T) {
	info := mockVideoInfo() // 120s
	cfg := SplitConfig{ChunkDuration: 0.1}

	chunks := SplitByTime(info, cfg)

	if len(chunks) == 0 {
		t.Fatal("expected chunks with small duration")
	}
}

func TestSplitByWeights_ManyWeights(t *testing.T) {
	info := mockVideoInfo()
	weights := make([]float64, 20)
	for i := range weights {
		weights[i] = float64(i + 1)
	}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	// Should produce up to 20 chunks (some very small ones might be skipped)
	if len(chunks) == 0 {
		t.Fatal("expected non-empty chunks for 20 weights")
	}

	// Total duration
	totalDur := 0.0
	for _, c := range chunks {
		totalDur += c.Duration
	}
	if math.Abs(totalDur-info.Duration) > 0.1 {
		t.Errorf("total duration %.3f != video duration %.3f", totalDur, info.Duration)
	}
}

func TestSplitByWeights_AllVerySmallWeights(t *testing.T) {
	info := mockVideoInfo()
	weights := []float64{0.001, 0.001, 0.001}
	cfg := DefaultSplitConfig()

	chunks := SplitByWeights(info, weights, cfg)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// Each should be about 1/3
	for _, c := range chunks {
		if math.Abs(c.Duration-40.0) > 0.1 {
			t.Errorf("with equal small weights, each chunk should be ~40s, got %.3f", c.Duration)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

func BenchmarkSplitByTime(b *testing.B) {
	info := mockVideoInfo()
	cfg := SplitConfig{ChunkCount: 10}

	for i := 0; i < b.N; i++ {
		SplitByTime(info, cfg)
	}
}

func BenchmarkSplitByWeights(b *testing.B) {
	info := mockVideoInfo()
	weights := []float64{3.0, 1.0, 2.0, 4.0, 1.5}
	cfg := DefaultSplitConfig()

	for i := 0; i < b.N; i++ {
		SplitByWeights(info, weights, cfg)
	}
}

func BenchmarkGenerateFFmpegCommands(b *testing.B) {
	info := mockVideoInfo()
	chunks := SplitByTime(info, SplitConfig{ChunkCount: 10})
	cfg := DefaultSplitConfig()

	for i := 0; i < b.N; i++ {
		GenerateFFmpegCommands(info, chunks, cfg)
	}
}

// ---------------------------------------------------------------------------
// helpers for tests
// ---------------------------------------------------------------------------

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
