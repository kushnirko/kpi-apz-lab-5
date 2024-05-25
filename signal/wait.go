package signal

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/kushnirko/kpi-apz-lab-4/logger"
)

func WaitForTerminationSignal() {
	intChannel := make(chan os.Signal)
	signal.Notify(intChannel, syscall.SIGINT, syscall.SIGTERM)
	<-intChannel
	logger.Println("Shutting down...")
}
