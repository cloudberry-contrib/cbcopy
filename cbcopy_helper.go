//go:build cbcopy_helper

package main

import (
	"os"

	"github.com/cloudberry-contrib/cbcopy/helper"
	"github.com/apache/cloudberry-go-libs/gplog"
)

func main() {
	helper := helper.NewHelper(helper.NewConfig())
	if err := helper.Run(); err != nil {
		gplog.Error("cbcopy_helper exited with error: %v", err)
		os.Exit(helper.GetErrCode())
	}

	os.Exit(0)
}
