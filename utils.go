package quickfix

import (
	"fmt"
	"io"
)

// SendMsgViaConn only for sending Logon Reject Message
func SendMsgViaConn(m Messagable, connection io.Writer) error {
	msg := m.ToMessage()

	msgBytes := msg.build()

	if _, err := connection.Write(msgBytes); err != nil {
		return fmt.Errorf(err.Error())
	}

	return nil
}
