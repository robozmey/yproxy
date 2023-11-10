package main

import (
	"net"

	"github.com/spf13/cobra"
	"github.com/yezzey-gp/yproxy/pkg/proc"
	"github.com/yezzey-gp/yproxy/pkg/ylogger"
)

var port int = 1337

var sockPath string = "/tmp/yezzey.sock"

var rootCmd = &cobra.Command{
	Use:   "run",
	Short: "run router",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := ylogger.NewZeroLogger("proxy.log")

		listener, err := net.Listen("unix", sockPath)
		if err != nil {
			logger.Error().Err(err).Msg("failed to start socket listener")
			return err
		}
		defer listener.Close()

		for {
			clConn, err := listener.Accept()
			if err != nil {
				logger.Error().Err(err).Msg("failed to accept connection")
			}
			go proc.ProcConn(clConn)
		}
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		ylogger.Zero.Fatal().Err(err).Msg("")
	}
}
