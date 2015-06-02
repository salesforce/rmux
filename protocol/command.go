//Represents a redis client that is connected to our rmux server
package protocol

type Command interface {
	GetCommand() []byte
	GetBuffer() []byte
	GetFirstArg() []byte
	GetArgCount() int
}

