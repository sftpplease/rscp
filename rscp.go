package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"
)

const (
	S_IWUSR = 00200
	S_IRWXU = 00700
	S_ISUID = 04000
	S_ISGID = 02000

	MaxErrLen = 1024
)

var (
	iamSource     = flag.Bool("f", false, "Run in source mode")
	iamSink       = flag.Bool("t", false, "Run in sink mode")
	bwLimit       = flag.Uint("l", 0, "Limit the bandwidth, specified in Kbit/s")
	iamRecursive  = flag.Bool("r", false, "Copy directoires recursively following any symlinks")
	targetDir     = flag.Bool("d", false, "Target should be a directory")
	preserveAttrs = flag.Bool("p", false, "Preserve modification and access times and mode from original file")

	protocolErr = FatalError("protocol error")

	in io.Reader  = os.Stdin
	out io.Writer = os.Stdout
)

func main() {
	flag.Parse()
	var args = flag.Args()

	var validMode = (*iamSource || *iamSink) && !(*iamSource && *iamSink)
	var validArgc = (*iamSource && len(args) > 0) || (*iamSink && len(args) == 1)

	if !validMode || !validArgc {
		usage()
	}

	if *bwLimit > 0 {
		st := NewBwStats(*bwLimit * 1024)
		in = CapReader(in, st)
		out = CapWriter(out, st)
	}

	var err error

	if *iamSource {
		err = source(args)
	} else {
		err = sink(args[0], false)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func source(paths []string) error {
	if err := ack(); err != nil {
		return err
	}

	var sendErrs []error
	for _, path := range paths {
		if err := send(path); isFatal(err) {
			return err
		} else if err != nil {
			sendErrs = append(sendErrs, err)
		}
	}

	if len(sendErrs) > 0 {
		return AccError{sendErrs}
	}
	return nil
}

func sink(path string, recur bool) error {
	var errs []error
	var times *FileTimes

	if *targetDir {
		if st, err := os.Stat(path); err != nil {
			return teeError(FatalError(err.Error()))
		} else if !st.IsDir() {
			return teeError(FatalError(path + ": is not a directory"))
		}
	}

	fmt.Fprint(out, "\x00")

	for first := true; ; first = false {
		prefix := []byte{0}
		if _, err := in.Read(prefix); err != nil {
			if err == io.EOF {
				break
			}
			return FatalError(err.Error())
		}
		line, err := readLine()
		if err != nil {
			return FatalError(err.Error())
		}

		switch prefix[0] {
		case '\x01':
			errs = append(errs, errors.New(line))

		case '\x02':
			return FatalError(line)

		case 'E':
			if !recur {
				return teeError(protocolErr)
			}
			fmt.Fprint(out, "\x00")

		case 'T':
			if times == nil {
				times = new(FileTimes)
			}
			if n, err := fmt.Sscanf(line, "%d %d %d %d",
				&times.Mtime.Sec, &times.Mtime.Usec,
				&times.Atime.Sec, &times.Atime.Usec); err != nil {

				return teeError(FatalError(err.Error()))
			} else if n != 4 {
				return teeError(protocolErr)
			}
			fmt.Fprint(out, "\x00")

		case 'D':
			if err := sinkDir(path, line, times); isFatal(err) {
				return err
			} else if err != nil {
				errs = append(errs, err)
			}
			times = nil

		case 'C':
			if err := sinkFile(path, line, times); isFatal(err) {
				return err
			} else if err != nil {
				errs = append(errs, err)
			}
			times = nil

		default:
			err := protocolErr
			if first {
				compLine := append([]byte{prefix[0]}, line...)
				err = FatalError(string(compLine))
			}
			return teeError(err)
		}
	}

	if len(errs) > 0 {
		return AccError{errs}
	}
	return nil
}

func sinkDir(parent, line string, times *FileTimes) error {
	if !*iamRecursive {
		return teeError(FatalError("received directory without -r flag"))
	}

	perm, _, name, err := parseSubj(line)
	if err != nil {
		return teeError(FatalError(err.Error()))
	}

	name = path.Join(parent, name)

	resetPerm, err := prepareDir(name, perm)
	if err != nil {
		return teeError(err)
	}

	var errs []error
	if err := sink(name, true); isFatal(err) {
		return err
	} else if err != nil {
		errs = append(errs, err)
	}

	var pendErrs []error
	if times != nil {
		t := []syscall.Timeval{times.Atime, times.Mtime}
		if err := syscall.Utimes(name, t); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if resetPerm {
		if err := os.Chmod(name, perm); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if len(pendErrs) > 0 {
		errs = append(errs, pendErrs...)
		sendError(AccError{pendErrs})
	}

	if len(errs) > 0 {
		return AccError{errs}
	}
	return nil
}

func sinkFile(name, line string, times *FileTimes) error {
	perm, size, subj, err := parseSubj(line)
	if err != nil {
		return teeError(FatalError(err.Error()))
	}

	exists := false
	if st, err := os.Stat(name); err == nil {
		exists = true
		if st.IsDir() {
			name = path.Join(name, subj)
		}
	}

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, perm|S_IWUSR)
	if err != nil {
		return teeError(err)
	}
	defer f.Close() /* will sync explicitly */

	st, err := f.Stat()
	if err != nil {
		return teeError(err)
	}

	fmt.Fprint(out, "\x00")

	var pendErrs []error
	if wr, err := io.Copy(f, io.LimitReader(in, size)); err != nil {
		if _, err := io.Copy(ioutil.Discard, io.LimitReader(in, size-wr)); err != nil {
			return teeError(FatalError(err.Error()))
		}
		pendErrs = append(pendErrs, err)
	}

	if !exists || st.Mode().IsRegular() {
		if err := f.Truncate(size); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if err := f.Sync(); err != nil {
		pendErrs = append(pendErrs, err)
	}
	if *preserveAttrs || !exists {
		if err := f.Chmod(perm); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if times != nil {
		if err := syscall.Utimes(name, []syscall.Timeval{times.Atime, times.Mtime}); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}

	ackErr := ack()
	if isFatal(ackErr) {
		return ackErr
	}

	var sentErr error
	if len(pendErrs) > 0 {
		sentErr = AccError{pendErrs}
		sendError(sentErr)
	} else {
		fmt.Fprint(out, "\x00")
	}

	if ackErr != nil {
		return AccError{append(pendErrs, ackErr)}
	}
	return sentErr
}

func prepareDir(name string, perm os.FileMode) (bool, error) {
	resetPerm := false
	if st, err := os.Stat(name); err == nil {
		if !st.IsDir() {
			return resetPerm, errors.New(name + ": is not a directory")
		}
		if *preserveAttrs {
			if err := os.Chmod(name, perm); err != nil {
				return resetPerm, err
			}
		}
	} else if os.IsNotExist(err) {
		if err := os.Mkdir(name, perm|S_IRWXU); err != nil {
			return resetPerm, err
		}
		resetPerm = true
	} else {
		return resetPerm, err
	}
	return resetPerm, nil
}

func send(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return teeError(err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return teeError(err)
	}
	name = st.Name()

	if mode := st.Mode(); mode.IsDir() {
		if *iamRecursive {
			return sendDir(f, st)
		}
		return teeError(errors.New(name + ": is a directory"))
	} else if !mode.IsRegular() {
		return teeError(errors.New(name + ": not a regular file"))
	}

	if *preserveAttrs {
		if err := sendAttr(st); err != nil {
			return err
		}
	}

	fmt.Fprintf(out, "C%04o %d %s\n", toPosixPerm(st.Mode()), st.Size(), name)
	if err := ack(); err != nil {
		return err
	}

	if sent, err := io.Copy(out, f); err != nil {
		patch := io.LimitReader(ConstReader(0), st.Size()-sent)
		if _, err := io.Copy(out, patch); err != nil {
			return FatalError(err.Error())
		}
		if err := ack(); err != nil {
			return err
		}
		return teeError(err)
	}

	fmt.Fprint(out, "\x00")
	return ack()
}

func sendDir(dir *os.File, st os.FileInfo) error {
	content, err := dir.Readdirnames(0)
	if err != nil {
		return teeError(err)
	}

	if *preserveAttrs {
		if err := sendAttr(st); err != nil {
			return err
		}
	}

	fmt.Fprintf(out, "D%04o %d %s\n", toPosixPerm(st.Mode()), 0, st.Name())
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

	fmt.Fprintf(out, "E\n")
	ackErr := ack()
	if isFatal(ackErr) {
		return ackErr
	}

	if len(sendErrs) > 0 {
		return AccError{sendErrs}
	}
	return ackErr
}

func parseSubj(line string) (perm os.FileMode, size int64, name string, err error) {
	n := 0
	pperm := 0
	if n, err = fmt.Sscanf(line, "%o %d %s", &pperm, &size, &name); err != nil {
		return
	} else if n != 3 {
		err = protocolErr
		return
	}
	perm = toStdPerm(pperm)
	if name == ".." || strings.ContainsRune(name, '/') {
		err = FatalError(name + ": invalid name")
	}
	return
}

func sendAttr(st os.FileInfo) error {
	mtime := st.ModTime().Unix()
	atime := int64(0)

	if sysStat, ok := st.Sys().(*syscall.Stat_t); ok {
		atime, _ = sysStat.Atim.Unix()
	}

	fmt.Fprintf(out, "T%d 0 %d 0\n", mtime, atime)
	return ack()
}

func ack() error {
	kind := []byte{0}
	if _, err := in.Read(kind); err != nil {
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
		return protocolErr
	}
}

func teeError(err error) error {
	sendError(err)
	return err
}

func sendError(err error) {
	line := strings.Replace(err.Error(), "\n", "; ", -1)
	/* make complete protocol line with zero terminator (i.e \x01%s\n\x00) fit into MaxErrLen buffer */
	if len(line) > MaxErrLen-3 {
		line = line[:MaxErrLen-6] + "..."
	}
	fmt.Fprintf(out, "\x01%s\n", line)
}

func readLine() (string, error) {
	l := make([]byte, 0, 64)
	ch := []byte{0}

	for {
		if _, err := in.Read(ch); err != nil {
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

func toPosixPerm(perm os.FileMode) int {
	pp := perm & os.ModePerm
	if perm&os.ModeSetuid != 0 {
		pp |= S_ISUID
	}
	if perm&os.ModeSetgid != 0 {
		pp |= S_ISGID
	}
	return int(pp)
}

func toStdPerm(posixPerm int) os.FileMode {
	perm := os.FileMode(posixPerm) & os.ModePerm
	if posixPerm&S_ISUID != 0 {
		perm |= os.ModeSetuid
	}
	if posixPerm&S_ISGID != 0 {
		perm |= os.ModeSetgid
	}
	return perm
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: rscp -f [-pr] [-l limit] file1 ...\n"+
		"       rscp -t [-prd] [-l limit] directory\n")
	flag.PrintDefaults()
	os.Exit(1)
}

type FileTimes struct {
	Atime syscall.Timeval
	Mtime syscall.Timeval
}

type FatalError string

func (e FatalError) Error() string {
	return string(e)
}

func isFatal(err error) bool {
	_, isFatal := err.(FatalError)
	return isFatal
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
