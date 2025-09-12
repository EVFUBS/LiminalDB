package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type LogLevel int

type Logger struct {
	logLevel LogLevel
	logDir   string
	logger   *log.Logger
}

const (
	INFO LogLevel = iota
	DEBUG
	ERROR
)

var (
	registryMu sync.RWMutex
	registry   = map[string]*Logger{}
)

func Get(name string) (logger *Logger) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	if ln, ok := registry[name]; ok {
		return ln
	}

	return nil
}

func New(name string, logDir string, logLevel LogLevel) *Logger {
	registryMu.Lock()
	defer registryMu.Unlock()

	if logger, exists := registry[name]; exists {
		return logger
	}

	logger := setupLogger(logLevel, logDir)

	registry[name] = logger
	return logger
}

func (l *Logger) init() error {
	if err := os.MkdirAll(l.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	timestamp := time.Now().Format("2006-01-02")

	logFile, err := os.OpenFile(
		filepath.Join(l.logDir, fmt.Sprintf("Liminal-%s.log", timestamp)),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return fmt.Errorf("failed to open info log file: %v", err)
	}

	l.logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)

	return nil
}

func setupLogger(LogLevel LogLevel, logDir string) *Logger {
	logger := &Logger{
		logLevel: LogLevel,
		logDir:   logDir,
		logger:   nil,
	}

	if err := logger.init(); err != nil {
		panic(err)
	}

	return logger
}

func (l *Logger) Info(format string, v ...any) {
	if l.logLevel >= INFO {
		l.logger.Printf("INFO: "+format, v...)
	}
}

func (l *Logger) Debug(format string, v ...any) {
	if l.logLevel >= DEBUG {
		l.logger.Printf("DEBUG: "+format, v...)
	}
}

func (l *Logger) Error(format string, v ...any) {
	if l.logLevel >= ERROR {
		l.logger.Printf("ERROR: "+format, v...)
	}
}

func ResetRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()

	registry = map[string]*Logger{}
}
