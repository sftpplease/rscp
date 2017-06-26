package rscp

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/sftpplease/venv"
)

const (
	S_IWUSR = 00200
	S_IRWXU = 00700
	S_ISUID = 04000
	S_ISGID = 02000

	MaxErrLen        = 1024
	DirScanBatchSize = 256
)

type options struct {
	iamSource     *bool
	iamSink       *bool
	bwLimit       *uint
	iamRecursive  *bool
	targetDir     *bool
	preserveAttrs *bool
	in            io.Reader
	out           io.Writer
}

var (
	protocolErr = FatalError("protocol error")
)

func Main(env *venv.Env) {

	opts := &options{
		iamSource:     env.Flag.Bool("f", false, "Run in source mode"),
		iamSink:       env.Flag.Bool("t", false, "Run in sink mode"),
		bwLimit:       env.Flag.Uint("l", 0, "Limit the bandwidth, specified in Kbit/s"),
		iamRecursive:  env.Flag.Bool("r", false, "Copy directoires recursively following any symlinks"),
		targetDir:     env.Flag.Bool("d", false, "Target should be a directory"),
		preserveAttrs: env.Flag.Bool("p", false, "Preserve modification and access times and mode from original file"),
		in:            env.Os.Stdin,
		out:           env.Os.Stdout,
	}

	env.Flag.Parse()
	var args = env.Flag.Args()

	var validMode = (*opts.iamSource || *opts.iamSink) && !(*opts.iamSource && *opts.iamSink)
	var validArgc = (*opts.iamSource && len(args) > 0) || (*opts.iamSink && len(args) == 1)

	if !validMode || !validArgc {
		usage(env)
	}

	if *opts.bwLimit > 0 {
		st := NewBwStats(*opts.bwLimit * 1024)
		opts.in = CapReader(opts.in, st)
		opts.out = CapWriter(opts.out, st)
	}

	var err error

	if *opts.iamSource {
		err = source(env, opts, args)
	} else {
		err = sink(env, opts, args[0], false)
	}

	if err != nil {
		fmt.Fprintln(env.Os.Stderr, err)
		env.Os.Exit(1)
	}
}

func source(env *venv.Env, opts *options, paths []string) error {
	if err := ack(env, opts); err != nil {
		return err
	}

	var sendErrs []error
	for _, path := range paths {
		if err := send(env, opts, path); isFatal(err) {
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

func sink(env *venv.Env, opts *options, path string, recur bool) error {
	var errs []error
	var times *FileTimes

	if *opts.targetDir {
		if st, err := env.Os.Stat(path); err != nil {
			return teeError(env, opts, FatalError(err.Error()))
		} else if !st.IsDir() {
			return teeError(env, opts, FatalError(path+": is not a directory"))
		}
	}

	if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
		return FatalError(err.Error())
	}

	for first := true; ; first = false {
		prefix := []byte{0}
		if _, err := opts.in.Read(prefix); err != nil {
			if err == io.EOF {
				break
			}
			return FatalError(err.Error())
		}
		line, err := readLine(env, opts)
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
				return teeError(env, opts, protocolErr)
			}
			if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
				return FatalError(err.Error())
			}

		case 'T':
			if times == nil {
				times = new(FileTimes)
			}
			if n, err := fmt.Sscanf(line, "%d %d %d %d",
				&times.Mtime.Sec, &times.Mtime.Usec,
				&times.Atime.Sec, &times.Atime.Usec); err != nil {

				return teeError(env, opts, FatalError(err.Error()))
			} else if n != 4 {
				return teeError(env, opts, protocolErr)
			}

			if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
				return FatalError(err.Error())
			}

		case 'D':
			if err := sinkDir(env, opts, path, line, times); isFatal(err) {
				return err
			} else if err != nil {
				errs = append(errs, err)
			}
			times = nil

		case 'C':
			if err := sinkFile(env, opts, path, line, times); isFatal(err) {
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
			return teeError(env, opts, err)
		}
	}

	if len(errs) > 0 {
		return AccError{errs}
	}
	return nil
}

func sinkDir(env *venv.Env, opts *options, parent, line string, times *FileTimes) error {
	if !*opts.iamRecursive {
		return teeError(env, opts, FatalError("received directory without -r flag"))
	}

	perm, _, name, err := parseSubj(line)
	if err != nil {
		return teeError(env, opts, FatalError(err.Error()))
	}

	name = path.Join(parent, name)

	resetPerm, err := prepareDir(env, opts, name, perm)
	if err != nil {
		return teeError(env, opts, err)
	}

	var errs []error
	if err := sink(env, opts, name, true); isFatal(err) {
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
		if err := env.Os.Chmod(name, perm); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if len(pendErrs) > 0 {
		errs = append(errs, pendErrs...)
		if err := sendError(env, opts, AccError{pendErrs}); err != nil {
			return err
		}
	}

	if len(errs) > 0 {
		return AccError{errs}
	}
	return nil
}

func sinkFile(env *venv.Env, opts *options, name, line string, times *FileTimes) error {
	perm, size, subj, err := parseSubj(line)
	if err != nil {
		return teeError(env, opts, FatalError(err.Error()))
	}

	exists := false
	if st, err := env.Os.Stat(name); err == nil {
		exists = true
		if st.IsDir() {
			name = path.Join(name, subj)
		}
	}

	f, err := env.Os.OpenFile(name, os.O_WRONLY|os.O_CREATE, perm|S_IWUSR)
	if err != nil {
		return teeError(env, opts, err)
	}
	defer f.Close() /* will sync explicitly */

	st, err := f.Stat()
	if err != nil {
		return teeError(env, opts, err)
	}

	if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
		return FatalError(err.Error())
	}

	var pendErrs []error
	if wr, err := io.Copy(f, io.LimitReader(opts.in, size)); err != nil {
		if _, err := io.Copy(ioutil.Discard, io.LimitReader(opts.in, size-wr)); err != nil {
			return teeError(env, opts, FatalError(err.Error()))
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
	if *opts.preserveAttrs || !exists {
		if err := f.Chmod(perm); err != nil {
			pendErrs = append(pendErrs, err)
		}
	}
	if times != nil {
		if err := syscall.Utimes(name,
			[]syscall.Timeval{times.Atime, times.Mtime}); err != nil {

			pendErrs = append(pendErrs, err)
		}
	}

	ackErr := ack(env, opts)
	if isFatal(ackErr) {
		return ackErr
	}

	var sentErr error
	if len(pendErrs) > 0 {
		sentErr = AccError{pendErrs}
		if err := sendError(env, opts, sentErr); err != nil {
			return err
		}
	} else {
		if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
			return FatalError(err.Error())
		}
	}

	if ackErr != nil {
		return AccError{append(pendErrs, ackErr)}
	}
	return sentErr
}

func prepareDir(env *venv.Env, opts *options, name string, perm os.FileMode) (bool, error) {
	resetPerm := false
	if st, err := env.Os.Stat(name); err == nil {
		if !st.IsDir() {
			return resetPerm, errors.New(name + ": is not a directory")
		}
		if *opts.preserveAttrs {
			if err := env.Os.Chmod(name, perm); err != nil {
				return resetPerm, err
			}
		}
	} else if os.IsNotExist(err) {
		if err := env.Os.Mkdir(name, perm|S_IRWXU); err != nil {
			return resetPerm, err
		}
		resetPerm = true
	} else {
		return resetPerm, err
	}
	return resetPerm, nil
}

func send(env *venv.Env, opts *options, name string) error {
	f, err := env.Os.Open(name)
	if err != nil {
		return teeError(env, opts, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return teeError(env, opts, err)
	}
	name = st.Name()

	if mode := st.Mode(); mode.IsDir() {
		if *opts.iamRecursive {
			return sendDir(env, opts, f, st)
		}
		return teeError(env, opts, errors.New(name+": is a directory"))
	} else if !mode.IsRegular() {
		return teeError(env, opts, errors.New(name+": not a regular file"))
	}

	if *opts.preserveAttrs {
		if err := sendAttr(env, opts, st); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(opts.out, "C%04o %d %s\n",
		toPosixPerm(st.Mode()), st.Size(), name); err != nil {

		return FatalError(err.Error())
	}
	if err := ack(env, opts); err != nil {
		return err
	}

	if sent, err := io.Copy(opts.out, f); err != nil {
		patch := io.LimitReader(ConstReader(0), st.Size()-sent)
		if _, err := io.Copy(opts.out, patch); err != nil {
			return FatalError(err.Error())
		}
		if err := ack(env, opts); err != nil {
			return err
		}
		return teeError(env, opts, err)
	}

	if _, err := fmt.Fprint(opts.out, "\x00"); err != nil {
		return FatalError(err.Error())
	}
	return ack(env, opts)
}

func sendDir(env *venv.Env, opts *options, dir venv.File, st os.FileInfo) error {
	if *opts.preserveAttrs {
		if err := sendAttr(env, opts, st); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(opts.out, "D%04o %d %s\n",
		toPosixPerm(st.Mode()), 0, st.Name()); err != nil {
		return FatalError(err.Error())
	}
	if err := ack(env, opts); err != nil {
		return err
	}

	var sendErrs []error
	for {
		children, err := dir.Readdir(DirScanBatchSize)
		for _, child := range children {
			if err := send(env, opts, path.Join(dir.Name(), child.Name())); isFatal(err) {
				return err
			} else if err != nil {
				sendErrs = append(sendErrs, err)
			}
		}
		if err == io.EOF {
			break
		} else if err != nil {
			return teeError(env, opts, err)
		}
	}

	if _, err := fmt.Fprintf(opts.out, "E\n"); err != nil {
		return FatalError(err.Error())
	}
	ackErr := ack(env, opts)
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

func sendAttr(env *venv.Env, opts *options, st os.FileInfo) error {
	mtime := st.ModTime().Unix()
	atime := int64(0)

	if sysStat, ok := st.Sys().(*syscall.Stat_t); ok {
		atime, _ = sysStat.Atim.Unix()
	}

	if _, err := fmt.Fprintf(opts.out, "T%d 0 %d 0\n", mtime, atime); err != nil {
		return FatalError(err.Error())
	}
	return ack(env, opts)
}

func ack(env *venv.Env, opts *options) error {
	kind := []byte{0}
	if _, err := opts.in.Read(kind); err != nil {
		return FatalError(err.Error())
	}
	if kind[0] == 0 {
		return nil
	}

	l, err := readLine(env, opts)
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

func teeError(env *venv.Env, opts *options, err error) error {
	if err := sendError(env, opts, err); err != nil {
		return err
	}
	return err
}

func sendError(env *venv.Env, opts *options, err error) error {
	line := strings.Replace(err.Error(), "\n", "; ", -1)
	/* make complete protocol line with zero terminator (i.e \x01%s\n\x00) fit into MaxErrLen buffer */
	if len(line) > MaxErrLen-3 {
		line = line[:MaxErrLen-6] + "..."
	}
	if _, err := fmt.Fprintf(opts.out, "\x01%s\n", line); err != nil {
		return FatalError(err.Error())
	}
	return nil
}

func readLine(env *venv.Env, opts *options) (string, error) {
	l := make([]byte, 0, 64)
	ch := []byte{0}

	for {
		if _, err := opts.in.Read(ch); err != nil {
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

func usage(env *venv.Env) {
	fmt.Fprintf(env.Os.Stderr, "Usage: rscp -f [-pr] [-l limit] file1 ...\n"+
		"       rscp -t [-prd] [-l limit] directory\n")
	env.Flag.PrintDefaults()
	env.Os.Exit(1)
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
