package log

import (
	"bytes"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLog(t *testing.T) {
	// keep Std clean
	std := Std
	SetOutputLevel(Ldebug)

	Debugf("Debug: foo\n")
	Debug("Debug: foo")

	Infof("Info: foo\n")
	Info("Info: foo")

	Warnf("Warn: foo\n")
	Warn("Warn: foo")

	Errorf("Error: foo\n")
	Error("Error: foo")

	SetOutputLevel(Linfo)

	Debugf("Debug: foo\n")
	Debug("Debug: foo")

	Infof("Info: foo\n")
	Info("Info: foo")

	Warnf("Warn: foo\n")
	Warn("Warn: foo")

	Errorf("Error: foo\n")
	Error("Error: foo")

	Std = std
}

func TestLog_Time(t *testing.T) {
	// keep Std clean
	std := Std
	out := bytes.Buffer{}
	Std = New(&out, Std.Prefix(), Std.Flags())

	assert.Equal(t, Std.Level, Linfo)
	Info("test")
	outStr := out.String()
	assert.True(t, regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}.\d{6}$`).MatchString(outStr[:26]))
	Std = std
}

func TestLog_Level(t *testing.T) {
	// keep Std clean
	std := Std

	out := bytes.Buffer{}
	Std = New(&out, Std.Prefix(), Std.Flags())
	SetOutputLevel(Lwarn)

	assert.Equal(t, Std.Level, Lwarn)
	Debug("test")
	assert.Equal(t, out.String(), "")
	Info("test")
	assert.Equal(t, out.String(), "")
	Warn("test")
	Std = std
}
