//go:build cbcopy

package main

import (
	"os"

	"github.com/cloudberrydb/cbcopy/copy"
	"github.com/cloudberrydb/cbcopy/utils"
	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:     "cbcopy",
		Short:   "cbcopy utility for migrating data from Greenplum Database (GPDB) to Cloudberry Database (CBDB)",
		Args:    cobra.NoArgs,
		Version: utils.GetVersion(),
		Run: func(cmd *cobra.Command, args []string) {
			defer copy.DoTeardown()
			copy.DoFlagValidation(cmd)
			copy.DoSetup()
			copy.DoCopy()
		}}
	rootCmd.SetArgs(utils.HandleSingleDashes(os.Args[1:]))
	copy.DoInit(rootCmd)
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}
