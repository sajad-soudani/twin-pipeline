// Package logging removes the logger setup that was copy-pasted in both mains.
package logging

import (
	"log/slog"
	"os"
)

// Setup installs the default logger: JSON in production, text+debug otherwise.
func Setup(production bool) {
	var h slog.Handler
	if production {
		h = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		h = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	slog.SetDefault(slog.New(h))
}
