//Represents a redis client that is connected to our rmux server
package protocol
import "reflect"

type Command interface {
	GetCommand() []byte
	GetBuffer() []byte
	GetFirstArg() []byte
	GetArgCount() int
}

type RespCommand struct {
	RespData
	Command []byte
	FirstArg []byte
	ArgCount int
}

// TODO: Testing
func WrapRespCommand(respData RespData) (command *RespCommand, err error) {
	command = &RespCommand{}
	command.RespData = respData

	switch respData.(type) {
		case *RArray:
			ra := respData.(*RArray)
			command.Command = ra.FirstValue
			command.FirstArg = ra.SecondValue
			if argCount := ra.Count; argCount > 0 {
				command.ArgCount = argCount - 1
			} else {
				command.ArgCount = 0
			}
		case *RInlineString:
			is := respData.(*RInlineString)
			command.Command = is.Command
			command.FirstArg = is.FirstArg
			command.ArgCount = is.ArgCount
		case *RBulkString:
			// In theory bulk strings shouldn't be commands, but it's here for brevity.
			// These commands aren't meant to be performant since we shouldn't be getting these
			bs := respData.(*RBulkString)
			command.Command = bs.GetArg(0)
			command.FirstArg = bs.GetArg(1)
			if argCount := bs.GetArgCount(); argCount > 0 {
				command.ArgCount = argCount - 1
			} else {
				command.ArgCount = 0
			}
		case *RSimpleString:
			// In theory simple strings shouldn't be commands, but it's here for brevity.
			// These commands aren't meant to be performant since we shouldn't be getting these
			Debug("Warning: Got RSimpleString command")
			ss := respData.(*RSimpleString)
			command.Command = ss.GetFirstArg()
			command.FirstArg = ss.GetSecondArg()
			if argCount := ss.CountArgs(); argCount > 0 {
				command.ArgCount = argCount - 1
			} else {
				command.ArgCount = 0
			}
		default:
			Debug("Unrecognized type %s", reflect.TypeOf(respData))
			command, err = nil, ERROR_COMMAND_PARSE
	}

	if command != nil {
		command.lowercaseCommand()
	}

	return command, err
}

func (this *RespCommand) lowercaseCommand() {
	for i := 0; i < len(this.Command); i++ {
		if char := this.Command[i]; char >= 'A' && char <= 'Z' {
			this.Command[i] = this.Command[i] + 0x20
		}
	}
}

func (this *RespCommand) GetCommand() []byte {
	return this.Command
}

func (this *RespCommand) GetBuffer() []byte {
	return this.RespData.GetBuffer()
}

func (this *RespCommand) GetFirstArg() []byte {
	return this.FirstArg
}

func (this *RespCommand) GetArgCount() int {
	return this.ArgCount
}
