package util

import (
	"bytes"
	"io"
	"log"
	"strings"

	"github.com/hashicorp/go-hclog"
	"github.com/sirupsen/logrus"
)

// HCLogAdapter logrus adapter for hclog
type HCLogAdapter struct{}

// Args are alternating key, val pairs
// keys must be strings
// vals can be any type, but display is implementation specific

// Trace Emit a message and key/value pairs at the TRACE level
func (*HCLogAdapter) Trace(msg string, args ...any) {
	logrus.Tracef(msg, args...)
}

// Debug Emit a message and key/value pairs at the DEBUG level
func (*HCLogAdapter) Debug(msg string, args ...any) {
	logrus.Debugf(msg, args...)
}

// Info Emit a message and key/value pairs at the INFO level
func (*HCLogAdapter) Info(msg string, args ...any) {
	logrus.Infof(msg, args...)
}

// Warn Emit a message and key/value pairs at the WARN level
func (*HCLogAdapter) Warn(msg string, args ...any) {
	logrus.Warnf(msg, args...)
}

// Error Emit a message and key/value pairs at the ERROR level
func (*HCLogAdapter) Error(msg string, args ...any) {
	logrus.Errorf(msg, args...)
}

// IsTrace Indicate if TRACE logs would be emitted. This and the other Is* guards
// are used to elide expensive logging code based on the current level.
func (*HCLogAdapter) IsTrace() bool {
	return logrus.IsLevelEnabled(logrus.TraceLevel)
}

// IsDebug Indicate if DEBUG logs would be emitted. This and the other Is* guards
func (*HCLogAdapter) IsDebug() bool {
	return logrus.IsLevelEnabled(logrus.DebugLevel)
}

// IsInfo Indicate if INFO logs would be emitted. This and the other Is* guards
func (*HCLogAdapter) IsInfo() bool {
	return logrus.IsLevelEnabled(logrus.InfoLevel)
}

// IsWarn Indicate if WARN logs would be emitted. This and the other Is* guards
func (*HCLogAdapter) IsWarn() bool {
	return logrus.IsLevelEnabled(logrus.WarnLevel)
}

// IsError Indicate if ERROR logs would be emitted. This and the other Is* guards
func (*HCLogAdapter) IsError() bool {
	return logrus.IsLevelEnabled(logrus.ErrorLevel)
}

// With Creates a sublogger that will always have the given key/value pairs
func (*HCLogAdapter) With(args ...any) hclog.Logger {
	return &HCLogAdapter{}
}

// Named Create a logger that will prepend the name string on the front of all messages.
// If the logger already has a name, the new value will be appended to the current
// name. That way, a major subsystem can use this to decorate all it's own logs
// without losing context.
func (*HCLogAdapter) Named(name string) hclog.Logger {
	return &HCLogAdapter{}
}

// ResetNamed Create a logger that will prepend the name string on the front of all messages.
// This sets the name of the logger to the value directly, unlike Named which honor
// the current name as well.
func (*HCLogAdapter) ResetNamed(name string) hclog.Logger {
	return &HCLogAdapter{}
}

// SetLevel Updates the level. This should affect all sub-loggers as well. If an
// implementation cannot update the level on the fly, it should no-op.
func (*HCLogAdapter) SetLevel(level hclog.Level) {
	switch level {
	case hclog.Trace:
		logrus.SetLevel(logrus.TraceLevel)
	case hclog.Debug:
		logrus.SetLevel(logrus.DebugLevel)
	case hclog.Info:
		logrus.SetLevel(logrus.InfoLevel)
	case hclog.Warn:
		logrus.SetLevel(logrus.WarnLevel)
	case hclog.Error:
		logrus.SetLevel(logrus.ErrorLevel)
	}
}

// Returns the current level
func (*HCLogAdapter) GetLevel() hclog.Level {
	switch logrus.GetLevel() {
	case logrus.TraceLevel:
		return hclog.Trace
	case logrus.DebugLevel:
		return hclog.Debug
	case logrus.WarnLevel:
		return hclog.Warn
	case logrus.ErrorLevel:
		return hclog.Error
	case logrus.InfoLevel:
		fallthrough
	default:
		return hclog.Info
	}
}

// StandardLogger Return a value that conforms to the stdlib log.Logger interface
func (*HCLogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(&stdlogAdapter{}, "", 0)
}

func (*HCLogAdapter) ImpliedArgs() []any {
	return []any{}
}

func (l *HCLogAdapter) Name() string { return "root" }

func (l *HCLogAdapter) Log(level hclog.Level, msg string, args ...any) {
	switch level {
	case hclog.Trace:
		l.Trace(msg, args...)
	case hclog.Debug:
		l.Debug(msg, args...)
	case hclog.Info:
		l.Info(msg, args...)
	case hclog.Warn:
		l.Warn(msg, args...)
	case hclog.Error:
		l.Error(msg, args...)
	}
}

func (l *HCLogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return logrus.StandardLogger().Out
}

// Provides a io.Writer to shim the data out of *log.Logger
// and back into our Logger. This is basically the only way to
// build upon *log.Logger.
type stdlogAdapter struct {
	hl          *logrus.Logger
	inferLevels bool
}

// Take the data, infer the levels if configured, and send it through
// a regular Logger
func (s *stdlogAdapter) Write(data []byte) (int, error) {
	str := string(bytes.TrimRight(data, " \t\n"))

	if s.inferLevels {
		level, str := s.pickLevel(str)
		switch level {
		case logrus.TraceLevel:
			s.hl.Trace(str)
		case logrus.DebugLevel:
			s.hl.Debug(str)
		case logrus.InfoLevel:
			s.hl.Info(str)
		case logrus.WarnLevel:
			s.hl.Warn(str)
		case logrus.ErrorLevel:
			s.hl.Error(str)
		default:
			s.hl.Info(str)
		}
	} else {
		s.hl.Info(str)
	}

	return len(data), nil
}

// Detect, based on conventions, what log level this is
func (s *stdlogAdapter) pickLevel(str string) (logrus.Level, string) {
	switch {
	case strings.HasPrefix(str, "[DEBUG]"):
		return logrus.DebugLevel, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[TRACE]"):
		return logrus.TraceLevel, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[INFO]"):
		return logrus.InfoLevel, strings.TrimSpace(str[6:])
	case strings.HasPrefix(str, "[WARN]"):
		return logrus.WarnLevel, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[ERROR]"):
		return logrus.ErrorLevel, strings.TrimSpace(str[7:])
	case strings.HasPrefix(str, "[ERR]"):
		return logrus.ErrorLevel, strings.TrimSpace(str[5:])
	default:
		return logrus.InfoLevel, str
	}
}
