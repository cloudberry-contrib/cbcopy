//go:build cbcopy_helper

package main

import (
	"os"

	"github.com/cloudberrydb/cbcopy/helper"
)

func main() {
	helper := helper.NewHelper(helper.NewConfig())
	if err := helper.Run(); err != nil {
		os.Exit(helper.GetErrCode())
	}

	os.Exit(0)
}
