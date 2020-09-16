package fsm

const (
	OpSet   Op = "SET"
	OpCheck Op = "CHECK"
)

type Op string

type Message struct {
	Op
	Args []string
}
