package nerd

import (
	"errors"
	"fmt"
	"testing"
)

func TestParentShimPID(t *testing.T) {
	readFile := func(path string) ([]byte, error) {
		switch path {
		case "/proc/123/status":
			return []byte("Name:\trunsc-sandbox\nPPid:\t456\n"), nil
		case "/proc/456/comm":
			return []byte("containerd-shim\n"), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", path)
		}
	}

	pid, err := parentShimPID(123, readFile)
	if err != nil {
		t.Fatalf("parentShimPID returned an error: %v", err)
	}
	if pid != 456 {
		t.Fatalf("parentShimPID returned %d, want 456", pid)
	}
}

func TestParentShimPIDRejectsUnverifiedParent(t *testing.T) {
	readFile := func(path string) ([]byte, error) {
		switch path {
		case "/proc/123/status":
			return []byte("PPid:\t456\n"), nil
		case "/proc/456/comm":
			return []byte("systemd\n"), nil
		default:
			return nil, fmt.Errorf("unexpected path %s", path)
		}
	}

	_, err := parentShimPID(123, readFile)
	if err == nil || err.Error() != "task parent process 456 is not a containerd shim" {
		t.Fatalf("parentShimPID returned %v, want unverified-parent error", err)
	}
}

func TestParentShimPIDRequiresParentEntry(t *testing.T) {
	_, err := parentShimPID(123, func(string) ([]byte, error) {
		return []byte("Name:\trunsc-sandbox\n"), nil
	})
	if err == nil || err.Error() != "parent process not found for task PID 123" {
		t.Fatalf("parentShimPID returned %v, want missing-parent error", err)
	}
}

func TestParentShimPIDPropagatesReadError(t *testing.T) {
	wantErr := errors.New("read failed")
	_, err := parentShimPID(123, func(string) ([]byte, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("parentShimPID returned %v, want %v", err, wantErr)
	}
}
