package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	iamSource    = flag.Bool("f", false, "Run in source mode")
	iamSink      = flag.Bool("t", false, "Run in sink mode")
	bwLimit      = flag.Int("l", 0, "Limit the bandwidth, specified in Kbit/s")
	iamRecursive = flag.Bool("r", false, "Copy directoires recursively following any symlinks")
	targetDir    = flag.Bool("d", false, "Target should be a directory")
)

func main() {
	flag.Parse()
	var args = flag.Args()

	var validMode = (*iamSource || *iamSink) && !(*iamSource && *iamSink)
	var validArgc = (*iamSource && len(args) > 0) || (*iamSink && len(args) == 1)

	if !validMode || !validArgc {
		usage()
	}

	if *iamSource {
		source(args)
	} else {
		sink(args[0])
	}
}

func source(args []string) {
}

func sink(arg string) {
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: rscp -f [-r] [-l limit] file1 ...\n"+
		"       rscp -t [-rd] [-l limit] directory\n")
	flag.PrintDefaults()
	os.Exit(1)
}
