package logger

import "log"

var enabled bool

func Println(v ...any) {
	if enabled {
		log.Println(v...)
	}
}

func Printf(format string, v ...any) {
	if enabled {
		log.Printf(format, v...)
	}
}

func Init(logEnabled bool) {
	enabled = logEnabled
}
