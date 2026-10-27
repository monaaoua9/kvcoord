package kvpb

// Stable, string error codes. They travel in gRPC ErrorInfo.Reason and in
// CommandResult.ErrorCode, clients switch on them rather than on messages.
const (
	ErrInvalidArgument = "INVALID_ARGUMENT"
	ErrLeaseNotFound   = "LEASE_NOT_FOUND"
	ErrLeaseExpired    = "LEASE_EXPIRED"
	ErrSessionExpired  = "SESSION_EXPIRED"
	ErrStaleRequest    = "STALE_REQUEST"
	ErrNotLeader       = "NOT_LEADER"
	ErrNoLeader        = "NO_LEADER"
	ErrCompacted       = "COMPACTED"
	ErrFutureRevision  = "FUTURE_REVISION"
	ErrWatchOverflow   = "WATCH_OVERFLOW"
	ErrTimeout         = "TIMEOUT"
	ErrBusy            = "BUSY"
	ErrUnavailable     = "UNAVAILABLE"
	ErrInternal        = "INTERNAL"
	ErrConfPending     = "CONF_CHANGE_PENDING"
	ErrLearnerLagging  = "LEARNER_NOT_CAUGHT_UP"

	// ErrorDomain is the ErrorInfo.Domain used for every error we return.
	ErrorDomain = "kvcoord.v1"

	// MetaLeaderID / MetaLeaderAddr ride along in ErrorInfo.Metadata on NOT_LEADER.
	MetaLeaderID   = "leader_id"
	MetaLeaderAddr = "leader_addr"
)
