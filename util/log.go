package util

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LogConfig struct {
	Level  string
	Format string
}

// trim full path. output in the form directory/file.go.
func consoleFormatCaller(i any) string {
	var c string
	if cc, ok := i.(string); ok {
		c = cc
	}
	if len(c) > 0 {
		l := strings.Split(c, "/")
		if len(l) == 1 {
			return l[0]
		}
		return l[len(l)-2] + "/" + l[len(l)-1]
	}
	return c
}

// Configure default logger.
func Configure(config LogConfig) {
	var (
		lvl zerolog.Level
		err error
	)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	if config.Level == "" {
		lvl = zerolog.Disabled
	} else {
		lvl, err = zerolog.ParseLevel(config.Level)
		if err != nil {
			lvl = zerolog.Disabled
		}
	}

	if config.Format == "console" {
		output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
		output.FormatCaller = consoleFormatCaller
		log.Logger = zerolog.New(output).Level(lvl).With().Timestamp().CallerWithSkipFrameCount(2).Stack().Logger()
	} else {
		log.Logger = zerolog.New(os.Stdout).Level(lvl).With().Timestamp().CallerWithSkipFrameCount(2).Stack().Logger()
	}
}

// E is a helper function to shortcut condition checking and logging
// in the case of error
// Used like this:
//
//	if E(err) {
//	    return err
//	}
//
// to replace:
//
//	if err != nil {
//	    log.Msgf(err.Error())
//	    return err
//	}
func E(err error) bool {
	if err == nil {
		return false
	}

	log.Error().CallerSkipFrame(1).Msg(err.Error())

	return true
}

// CE is a helper to shortcut error creation and logging
// Used like this:
//
// return CE("msg, value %v", value)
//
// to replace:
//
// err := fmt.Errorf("msg, value %v", value)
// log.Msgf("msg, value %v", value)
// return err.
func CE(format string, args ...any) error {
	err := fmt.Errorf(format, args...)

	log.Error().CallerSkipFrame(1).Msg(err.Error())

	return err
}
