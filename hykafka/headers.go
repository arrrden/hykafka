package hykafka

type MsgHeader = string

const (
	MsgHeaderMessageType MsgHeader = "X-Message-Type"
	MsgHeaderMessageName MsgHeader = "X-Message-Name"
	MsgHeaderValueUNK    MsgHeader = "UNK"
)
