package chaos

import (
	"fmt"

	"github.com/CaueTech/celer-engine/internal/domain"
)

func defaultRobotPool() []string {
	pool := make([]string, 20)
	for i := range pool {
		pool[i] = fmt.Sprintf("bot-%03d", i+1)
	}
	return pool
}

func defaultStatusPool() []string {
	return []string{
		domain.StatusOK,
		domain.StatusOK,
		domain.StatusOK,
		domain.StatusWarning,
		domain.StatusError,
	}
}
