package server

import (
	"log/slog"
	"os"
)

func Run() {
	slog.Info("Hello, Function Stream!")
	err := StartRESTHandlers()
	if err != nil {
		slog.Error("Error starting REST handlers", err)
		os.Exit(1)
	}
}
