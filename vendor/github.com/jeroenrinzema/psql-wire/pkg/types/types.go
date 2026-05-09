package types

// ClientMessage represents a client pgwire message.
type ClientMessage byte

// ServerMessage represents a server pgwire message.
type ServerMessage byte

// DescribeMessage represents a client describe message type.
type DescribeMessage byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	ClientBind        ClientMessage = 'B'
	ClientClose       ClientMessage = 'C'
	ClientCopyData    ClientMessage = 'd'
	ClientCopyDone    ClientMessage = 'c'
	ClientCopyFail    ClientMessage = 'f'
	ClientDescribe    ClientMessage = 'D'
	ClientExecute     ClientMessage = 'E'
	ClientFlush       ClientMessage = 'H'
	ClientParse       ClientMessage = 'P'
	ClientPassword    ClientMessage = 'p'
	ClientSimpleQuery ClientMessage = 'Q'
	ClientSync        ClientMessage = 'S'
	ClientTerminate   ClientMessage = 'X'

	ServerAuth                 ServerMessage = 'R'
	ServerBackendKeyData       ServerMessage = 'K'
	ServerBindComplete         ServerMessage = '2'
	ServerCommandComplete      ServerMessage = 'C'
	ServerCloseComplete        ServerMessage = '3'
	ServerCopyInResponse       ServerMessage = 'G'
	ServerDataRow              ServerMessage = 'D'
	ServerEmptyQuery           ServerMessage = 'I'
	ServerErrorResponse        ServerMessage = 'E'
	ServerNoticeResponse       ServerMessage = 'N'
	ServerNoData               ServerMessage = 'n'
	ServerParameterDescription ServerMessage = 't'
	ServerParameterStatus      ServerMessage = 'S'
	ServerParseComplete        ServerMessage = '1'
	ServerPortalSuspended      ServerMessage = 's'
	ServerReady                ServerMessage = 'Z'
	ServerRowDescription       ServerMessage = 'T'

	DescribePortal    DescribeMessage = 'P'
	DescribeStatement DescribeMessage = 'S'
)

func (m ClientMessage) String() string {
	switch m {
	case ClientBind:
		return "Bind"
	case ClientClose:
		return "Close"
	case ClientCopyData:
		return "CopyData"
	case ClientCopyDone:
		return "CopyDone"
	case ClientCopyFail:
		return "CopyFail"
	case ClientDescribe:
		return "Describe"
	case ClientExecute:
		return "Execute"
	case ClientFlush:
		return "Flush"
	case ClientParse:
		return "Parse"
	case ClientPassword:
		return "Password"
	case ClientSimpleQuery:
		return "SimpleQuery"
	case ClientSync:
		return "Sync"
	case ClientTerminate:
		return "Terminate"
	default:
		return "Unknown"
	}
}

func (m ServerMessage) String() string {
	switch m {
	case ServerAuth:
		return "Auth"
	case ServerBackendKeyData:
		return "BackendKeyData"
	case ServerBindComplete:
		return "BindComplete"
	case ServerCommandComplete:
		return "CommandComplete"
	case ServerCloseComplete:
		return "CloseComplete"
	case ServerCopyInResponse:
		return "CopyInResponse"
	case ServerDataRow:
		return "DataRow"
	case ServerEmptyQuery:
		return "EmptyQuery"
	case ServerErrorResponse:
		return "ErrorResponse"
	case ServerNoticeResponse:
		return "NoticeResponse"
	case ServerNoData:
		return "NoData"
	case ServerParameterDescription:
		return "ParameterDescription"
	case ServerParameterStatus:
		return "ParameterStatus"
	case ServerParseComplete:
		return "ParseComplete"
	case ServerPortalSuspended:
		return "PortalSuspended"
	case ServerReady:
		return "Ready"
	case ServerRowDescription:
		return "RowDescription"
	default:
		return "Unknown"
	}
}

func (m DescribeMessage) String() string {
	switch m {
	case DescribePortal:
		return "Portal"
	case DescribeStatement:
		return "Statement"
	default:
		return "Unknown"
	}
}
