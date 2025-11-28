package main

import (
	"LiminalDb/helpers"
	"LiminalDb/internal/database/server"
	"LiminalDb/internal/interpreter"
	l "LiminalDb/internal/logger"
	"path/filepath"
)

func main() {
	logDir := filepath.Join("logs")

	replLogger := l.New("repl", logDir, l.ERROR)
	l.New("interpreter", logDir, l.ERROR)
	l.New("sql", logDir, l.ERROR)

	go server.StartServer()
	helpers.WaitForServer()

	replLogger.Info("Starting LiminalDB server")
	interpreter.Repl()
	replLogger.Info("Shutting down LiminalDB server")
}
