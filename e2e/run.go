package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

func Run(ctx context.Context, logfile string, args ...string) error {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if len(logfile) > 0 {
		outfile, err := os.Create(logfile)
		if err != nil {
			return err
		}
		defer outfile.Close()
		cmd.Stdout = outfile
		cmd.Stderr = outfile
	} else {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	// clean up process group on context done
	go func() {
		defer func() {
			// negative pid sends the signal to all child processes in the group
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
				fmt.Println(err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
			}
		}
	}()

	return cmd.Wait()
}

func GoRun(ctx context.Context, logfile string, args ...string) (int, error) {
	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if len(logfile) > 0 {
		outfile, err := os.Create(logfile)
		if err != nil {
			return 0, err
		}
		defer outfile.Close()
		cmd.Stdout = outfile
		cmd.Stderr = outfile
	} else {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}

	go func() {
		// clean up process group on context done
		go func() {
			defer func() {
				// negative pid sends the signal to all child processes in the group
				if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
					fmt.Println(err)
				}
			}()
			for {
				select {
				case <-ctx.Done():
				}
			}
		}()

		if err := cmd.Wait(); err != nil {
			fmt.Println(err)
		}
	}()

	return cmd.Process.Pid, nil
}
