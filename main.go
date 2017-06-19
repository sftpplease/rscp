package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"
)

var (
	iamSource    = flag.Bool("f", false, "Run in source mode")
	iamSink      = flag.Bool("t", false, "Run in sink mode")
	bwLimit      = flag.Int("l", 0, "Limit the bandwidth, specified in Kbit/s")
	iamRecursive = flag.Bool("r", false, "Copy directoires recursively following any symlinks")
	targetDir    = flag.Bool("d", false, "Target should be a directory")
	preserveAttr = flag.Bool("p", false, "Preserve modification and access times and mode from original file")
)

func main() {
	flag.Parse()
	var args = flag.Args()

	var validMode = (*iamSource || *iamSink) && !(*iamSource && *iamSink)
	var validArgc = (*iamSource && len(args) > 0) || (*iamSink && len(args) == 1)

	if !validMode || !validArgc {
		usage()
	}

	var err error

	if *iamSource {
		err = source(args)
	} else {
		err = sink(args[0])
	}

	if err != nil {
		os.Exit(1)
	}
}

func source(paths []string) error {
	if err := ack(); err != nil {
		return err
	}

	var sendErrs []error
	for _, path := range paths {
		if err := send(path); err != nil {
			if _, ok := err.(FatalError); ok {
				return err
			}
			sendErrs = append(sendErrs, err)
		}
	}

	if len(sendErrs) > 0 {
		return AccError{sendErrs}
	}
	return nil
}

func sink(arg string) error {
	return nil
}

func send(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return teeError(err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return teeError(err)
	}
	name := st.Name()

	switch st.Mode() & os.ModeType {
	case 0: /* regular file */
		break
	case os.ModeDir:
		if *iamRecursive {
			return sendDir(f, st)
		}
		return teeError(errors.New(fmt.Sprintf("%s: is a directory", name)))
	default:
		return teeError(errors.New(fmt.Sprintf("%s: not a regular file", name)))
	}

	if *preserveAttr {
		if err := sendAttr(st); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stdout, "C%04o %d %s\n", toPosixMode(st.Mode()), st.Size(), name)
	if err := ack(); err != nil {
		return err
	}

	if sent, err := io.Copy(os.Stdout, f); err != nil {
		patch := io.LimitReader(ConstReader(0), st.Size()-sent)
		if _, err := io.Copy(os.Stdout, patch); err != nil {
			return FatalError(err.Error())
		}
		if err := ack(); err != nil {
			return err
		}
		return teeError(err)
	}

	fmt.Fprintf(os.Stdout, "\x00")
	return ack()
}

func sendDir(dir *os.File, st os.FileInfo) error {
	content, err := dir.Readdirnames(0)
	if err != nil {
		return teeError(err)
	}

	if *preserveAttr {
		if err := sendAttr(st); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stdout, "D%04o %d %s\n", toPosixMode(st.Mode()), 0, st.Name())
	if err := ack(); err != nil {
		return err
	}

	var sendErrs []error
	for _, entry := range content {
		if err := send(path.Join(dir.Name(), entry)); err != nil {
			if _, ok := err.(FatalError); ok {
				return err
			}
			sendErrs = append(sendErrs, err)
		}
	}

	fmt.Fprintf(os.Stdout, "E\n")
	ackErr := ack()

	if len(sendErrs) > 0 {
		return AccError{sendErrs}
	}
	return ackErr
}

func sendAttr(st os.FileInfo) error {
	mtime := st.ModTime().Unix()
	atime := int64(0)

	if sysStat, ok := st.Sys().(*syscall.Stat_t); ok {
		atime, _ = sysStat.Atim.Unix()
	}

	fmt.Fprintf(os.Stdout, "T%d 0 %d 0\n", mtime, atime)
	return ack()
}

func ack() error {
	kind := []byte{0}
	if _, err := os.Stdin.Read(kind); err != nil {
		return FatalError(err.Error())
	}
	if kind[0] == 0 {
		return nil
	}

	l, err := readLine()
	if err != nil {
		return FatalError(err.Error())
	}

	switch kind[0] {
	case 1:
		return errors.New(l)
	case 2:
		return FatalError(l)
	default:
		return FatalError("Protocol error")
	}
}

func teeError(err error) error {
	fmt.Fprintf(os.Stdout, "\x01%s\n", err)
	return err
}

func readLine() (string, error) {
	l := make([]byte, 0, 64)
	ch := []byte{0}

	for {
		if _, err := os.Stdin.Read(ch); err != nil {
			return "", err
		} else {
			if ch[0] == '\n' {
				break
			}
			l = append(l, ch[0])
		}
	}

	return string(l), nil
}

func toPosixMode(m os.FileMode) int {
	pm := m & os.ModePerm
	if m&os.ModeSetuid != 0 {
		pm |= 04000
	}
	if m&os.ModeSetgid != 0 {
		pm |= 02000
	}
	return int(pm)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: rscp -f [-pr] [-l limit] file1 ...\n"+
		"       rscp -t [-prd] [-l limit] directory\n")
	flag.PrintDefaults()
	os.Exit(1)
}

type FatalError string

func (e FatalError) Error() string {
	return string(e)
}

type AccError struct {
	Errors []error
}

func (e AccError) Error() string {
	ve := []interface{}{}
	for _, err := range e.Errors {
		ve = append(ve, err)
	}
	return fmt.Sprintln(ve...)
}

type ConstReader byte

func (c ConstReader) Read(b []byte) (int, error) {
	for i, _ := range b {
		b[i] = byte(c)
	}
	return len(b), nil
}
