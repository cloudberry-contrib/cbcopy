//go:build cbcopy

package main

import (
	"os"

	"github.com/cloudberrydb/cbcopy/copy"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/spf13/cobra"
)

func main() {
	app := copy.NewApplication()

	var rootCmd = &cobra.Command{
		Use:     "cbcopy",
		Short:   "cbcopy utility for migrating data from Greenplum Database (GPDB) to Apache Cloudberry (CBDB)",
		Args:    cobra.NoArgs,
		Version: utils.GetVersion(),
		Run: func(cmd *cobra.Command, args []string) {
			app.Run(cmd)
		}}
	rootCmd.SetArgs(utils.HandleSingleDashes(os.Args[1:]))

	app.Initialize(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}
