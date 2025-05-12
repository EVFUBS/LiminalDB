package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	InfoLogger  *log.Logger
	ErrorLogger *log.Logger
	DebugLogger *log.Logger
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	ERROR
)

func Init(logLevel LogLevel, logDir string) error {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %v", err)
	}

	timestamp := time.Now().Format("2006-01-02")

	infoFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("info-%s.log", timestamp)),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return fmt.Errorf("failed to open info log file: %v", err)
	}

	errorFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("error-%s.log", timestamp)),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return fmt.Errorf("failed to open error log file: %v", err)
	}

	debugFile, err := os.OpenFile(
		filepath.Join(logDir, fmt.Sprintf("debug-%s.log", timestamp)),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return fmt.Errorf("failed to open debug log file: %v", err)
	}

	InfoLogger = log.New(infoFile, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	ErrorLogger = log.New(errorFile, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	DebugLogger = log.New(debugFile, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)

	if logLevel > INFO {
		DebugLogger.SetOutput(os.Stderr)
	}
	if logLevel > ERROR {
		InfoLogger.SetOutput(os.Stderr)
	}

	return nil
}

func SetupLogger() {
	logDir := filepath.Join("logs")
	if err := Init(INFO, logDir); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
}

func Info(format string, v ...interface{}) {
	InfoLogger.Printf(format, v...)
}

func Error(format string, v ...interface{}) {
	ErrorLogger.Printf(format, v...)
}

func Debug(format string, v ...interface{}) {
	DebugLogger.Printf(format, v...)
}
