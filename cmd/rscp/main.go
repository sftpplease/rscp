package main

import (
	"github.com/sftpplease/rscp"
	"github.com/sftpplease/venv"
	"github.com/sftpplease/venv/passthrough"
)

func main() {
	env := &venv.Env{}
	env.Os = passthrough.PassthroughOS()
	env.Flag = venv.NewFlag(env.Os)

	rscp.Main(env)
}
