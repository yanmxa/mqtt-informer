package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var customTimeEncoder = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	// enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
	enc.AppendString(t.Format("2006-01-02 15:04:05")) // MST
}

var customLevelEncoder = func(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(level.CapitalString())
}

var customCallerEncoder = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	// enc.AppendString("[" + caller.TrimmedPath() + "]")
}

func DefaultLogger() logr.Logger {
	zapLoggerEncoderConfig := zapcore.EncoderConfig{
		TimeKey:          "time",
		LevelKey:         "level",
		NameKey:          "logger",
		CallerKey:        "caller",
		MessageKey:       "message",
		StacktraceKey:    "stacktrace",
		EncodeCaller:     customCallerEncoder,
		EncodeTime:       customTimeEncoder,
		EncodeLevel:      customLevelEncoder,
		EncodeDuration:   zapcore.SecondsDurationEncoder,
		LineEnding:       " ", // "\n"
		ConsoleSeparator: " ",
	}
	zapLoggerEncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	syncWriter := zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout))

	zapCore := zapcore.NewCore(zapcore.NewConsoleEncoder(zapLoggerEncoderConfig), syncWriter, zapcore.DebugLevel)
	zapLogger := zap.New(zapCore, zap.AddCaller(), zap.AddCallerSkip(1))
	logger := zapr.NewLogger(zapLogger)
	return logger
}

// func PrettyPrint(i interface{}) string {
// 	bytes, _ := json.MarshalIndent(i, "", " ")
// 	return string(bytes)
// }

func PrettyPrint(v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
