package work

type Logger interface {
	Printf(format string, v ...any)
}
