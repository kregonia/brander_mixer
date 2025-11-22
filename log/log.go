package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// 说明：下面是被替换前文件的较旧实现（为向后兼容某些符号名保留）。
// 但为保持代码库一致性，本文件整体已重写为简洁实现。

// 原实现的一些符号保留（空壳），以避免外部调用发生大量重构。
// 新实现在同名函数（Init / Notice / Errorf 等）处提供行为。

// 以下为老符号的占位类型（不会被外部直接使用）：
type LogLevel int

const (
	NOTICE LogLevel = iota
	WARNING
	ERROR
	FATAL
)

// 重新实现（简洁）在文件顶部：

// 简洁实现：单个全局 LogSystem，根据环境或 Init 配置决定 mode（debug/online）。

var levelNames = map[LogLevel]string{
	NOTICE:  "notice",
	WARNING: "warning",
	ERROR:   "error",
	FATAL:   "fatal",
}

type Config struct {
	Mode      string // "debug" 或 "online"，可被环境变量覆盖
	BaseDir   string // 日志根目录
	MaxDays   int    // （保留）未使用，保留为兼容字段
	CallDepth int    // 用于输出时确定调用者
}

type LogSystem struct {
	mu         sync.Mutex
	files      map[LogLevel]*os.File
	baseDir    string
	mode       string
	callDepth  int
	currentDay string
}

var (
	globalNew *LogSystem
	onceNew   sync.Once
)

func isOnlineEnv() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if env == "" {
		env = strings.ToLower(strings.TrimSpace(os.Getenv("ENV")))
	}
	if env == "production" || env == "prod" || env == "online" {
		return true
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("IS_PRODUCTION")))
	return v == "1" || v == "true" || v == "yes"
}

func Init(ctx context.Context, cfg Config) error {
	var err error
	onceNew.Do(func() {
		mode := "debug"
		if isOnlineEnv() {
			mode = "online"
		} else if cfg.Mode != "" {
			if strings.ToLower(cfg.Mode) == "online" {
				mode = "online"
			}
		}
		base := cfg.BaseDir
		if base == "" {
			base = "logs"
		}
		cd := cfg.CallDepth
		if cd <= 0 {
			cd = 3
		}

		ls := &LogSystem{
			files:      make(map[LogLevel]*os.File),
			baseDir:    base,
			mode:       mode,
			callDepth:  cd,
			currentDay: time.Now().Format("2006-01-02"),
		}
		if err = ls.createFilesForDay(); err != nil {
			return
		}
		globalNew = ls
	})
	return err
}

func (ls *LogSystem) createFilesForDay() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	modeDir := filepath.Join(ls.baseDir, ls.mode)
	if err := os.MkdirAll(modeDir, 0o755); err != nil {
		return err
	}
	ls.currentDay = time.Now().Format("2006-01-02")
	for lvl, name := range levelNames {
		path := filepath.Join(modeDir, name+".log")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
		if err != nil {
			for _, of := range ls.files {
				if of != nil {
					of.Close()
				}
			}
			ls.files = nil
			return err
		}
		ls.files[lvl] = f
	}
	return nil
}

func (ls *LogSystem) rotateIfNeeded() error {
	today := time.Now().Format("2006-01-02")
	if today == ls.currentDay {
		return nil
	}
	for _, f := range ls.files {
		if f != nil {
			f.Close()
		}
	}
	ls.files = make(map[LogLevel]*os.File)
	ls.currentDay = today
	return ls.createFilesForDay()
}

func (ls *LogSystem) write(level LogLevel, format string, v ...any) {
	if ls == nil {
		return
	}
	_ = ls.rotateIfNeeded()
	_, file, line, ok := runtime.Caller(ls.callDepth)
	if !ok {
		file = "???"
		line = 0
	} else {
		file = filepath.Base(file)
	}
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	lvl := strings.ToUpper(levelNames[level])
	prefix := fmt.Sprintf("[%s] [%s] [%s] %s:%d ", ts, ls.mode, lvl, file, line)
	var body string
	if format == "" {
		body = fmt.Sprint(v...)
	} else {
		body = fmt.Sprintf(format, v...)
	}
	lineOut := prefix + body + "\n"
	// 优先写入文件；若文件不可用或写入失败，再回退到 stdout
	ls.mu.Lock()
	f, ok := ls.files[level]
	ls.mu.Unlock()

	wroteToFile := false
	if ok && f != nil {
		if _, err := f.WriteString(lineOut); err == nil {
			wroteToFile = true
		}
	}

	if !wroteToFile {
		// 文件不可用或写入失败，回退到 stdout
		_, _ = io.WriteString(os.Stdout, lineOut)
	}
	if level == FATAL {
		os.Exit(1)
	}
}

func getGlobal() *LogSystem {
	if globalNew == nil {
		// lazy init with default config
		_ = Init(context.Background(), Config{})
	}
	return globalNew
}

func Notice(v ...any)                 { getGlobal().write(NOTICE, "", v...) }
func Noticef(format string, v ...any) { getGlobal().write(NOTICE, format, v...) }
func Warn(v ...any)                   { getGlobal().write(WARNING, "", v...) }
func Warnf(format string, v ...any)   { getGlobal().write(WARNING, format, v...) }
func Error(v ...any)                  { getGlobal().write(ERROR, "", v...) }
func Errorf(format string, v ...any)  { getGlobal().write(ERROR, format, v...) }
func Fatal(v ...any)                  { getGlobal().write(FATAL, "", v...) }
func Fatalf(format string, v ...any)  { getGlobal().write(FATAL, format, v...) }

func CloseAll() {
	if globalNew == nil {
		return
	}
	globalNew.mu.Lock()
	defer globalNew.mu.Unlock()
	for _, f := range globalNew.files {
		if f != nil {
			f.Close()
		}
	}
	globalNew.files = nil
}
