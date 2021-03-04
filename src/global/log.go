package global

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// InitLog initializs logging according to passed configuration
func InitLog(config *Configuration) bool {

	// set a consistent output for the log no matter if file or stdout
	formatter := logFormat{}
	formatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(&formatter)

	if strings.ToLower(config.Global.LogTarget) == "stdout" {
		log.SetOutput(os.Stdout)
	} else if strings.ToLower(config.Global.LogTarget) == "file" &&
		config.Global.LogFile != "" {
		// try to initialize the log on file if it fails it will redirect to stdout
		file, err := os.OpenFile(config.Global.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
		if err == nil {
			log.SetOutput(file)
		} else {
			log.Error("Error logging to file ", err.Error())
			return false
		}
	}

	// set log severity level
	switch level := strings.ToLower(config.Global.LogLevel); level {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.ErrorLevel)
	}

	if log.GetLevel() == log.DebugLevel {
		log.Debug("Testing the log")
		log.Info("Testing the log")
		log.Warning("Testing the log")
		log.Error("testing log errors")
		log.SetLevel(log.DebugLevel)
		log.Debug("Log initialized")
	}
	return true
}

type logFormat struct {
	TimestampFormat string
}

func (f *logFormat) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer

	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	b.WriteString("\x1b[" + strconv.Itoa(getColorByLevel(entry.Level)) + "m")
	b.WriteByte('[')
	b.WriteString(strings.ToUpper(entry.Level.String()))
	b.WriteString("]")
	b.WriteString("\x1b[0m")
	b.WriteByte(':')
	b.WriteString(entry.Time.Format(f.TimestampFormat))

	if entry.Message != "" {
		b.WriteString(" - ")
		b.WriteString(entry.Message)
	}

	if len(entry.Data) > 0 {
		b.WriteString(" || ")
	}
	for key, value := range entry.Data {
		b.WriteString(key)
		b.WriteByte('=')
		b.WriteByte('{')
		fmt.Fprint(b, value)
		b.WriteString("}, ")
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

const (
	colorRed    = 31
	colorYellow = 33
	colorBlue   = 36
	colorGray   = 34
	paniclevel  = 35
)

func getColorByLevel(level log.Level) int {
	switch level {
	case log.DebugLevel:
		return colorGray
	case log.WarnLevel:
		return colorYellow
	case log.ErrorLevel:
		return colorRed
	case log.PanicLevel, log.FatalLevel:
		return paniclevel
	default:
		return colorBlue
	}
}
