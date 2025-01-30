package cmdutil

import (
	"fmt"
	"go.uber.org/zap"
	"os"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func GetLogger() *zap.Logger {
	logger, _ := zap.NewProduction()
	return logger
}
