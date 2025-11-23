package status

import (
	"fmt"
	"testing"
)

func TestGetWorkerStatus(t *testing.T) {
	status := GetWorkerStatus()
	fmt.Printf("Worker Status:\n%s\n", status)
}
