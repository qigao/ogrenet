package runtimecore

import (
	"os/exec"
	"strings"
	"testing"
)

func TestRuntimecoreDoesNotDependOnTransportOrNativePollers(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "github.com/qigao/ogrenet/internal/runtimecore")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	deps := make(map[string]bool)
	for _, dep := range strings.Fields(string(out)) {
		deps[dep] = true
	}
	for _, dep := range []string{
		"github.com/qigao/ogrenet/transport",
		"github.com/qigao/ogrenet/epoll",
		"github.com/qigao/ogrenet/kqueue",
		"github.com/qigao/ogrenet/iocp",
	} {
		if deps[dep] {
			t.Fatalf("runtimecore depends on %s", dep)
		}
	}
}
