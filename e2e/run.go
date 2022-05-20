package e2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

// Run runs the command and blocks until it is finished
func Run(ctx context.Context, out, errOut io.Writer, args ...string) error {
	cmd, err := start(ctx, out, errOut, args...)
	if err != nil {
		return err
	}
	cleanupOnDone(ctx.Done(), cmd.Process.Pid)
	return cmd.Wait()
}

// GoRun runs the command in the background and returns a process group id
func GoRun(ctx context.Context, out, errOut io.Writer, args ...string) (int, error) {
	cmd, err := start(ctx, out, errOut, args...)
	if err != nil {
		return 0, err
	}
	go func() {
		cleanupOnDone(ctx.Done(), cmd.Process.Pid)

		if err := cmd.Wait(); err != nil {
			fmt.Fprintln(errOut, err)
		}
	}()

	return cmd.Process.Pid, nil
}

func start(ctx context.Context, out, errOut io.Writer, args ...string) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...) // #nosec G204
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = out
	cmd.Stderr = errOut

	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func cleanupOnDone(done <-chan struct{}, pid int) {
	go func() {
		<-done
		fmt.Println("killing", pid)
		// negative Pid sends the signal to all child processes in the group
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
}
