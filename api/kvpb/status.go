package kvpb

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewError builds a gRPC status error carrying a stable reason code.
func NewError(c codes.Code, reason, msg string, md map[string]string) error {
	st := status.New(c, msg)
	if withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorDomain, Metadata: md}); err == nil {
		st = withInfo
	}
	return st.Err()
}

func errorInfo(err error) *errdetails.ErrorInfo {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Domain == ErrorDomain {
			return info
		}
	}
	return nil
}

// Reason extracts the stable code from an error returned by the service, "" if none.
func Reason(err error) string {
	if info := errorInfo(err); info != nil {
		return info.Reason
	}
	return ""
}

func Metadata(err error) map[string]string {
	if info := errorInfo(err); info != nil {
		return info.Metadata
	}
	return nil
}
