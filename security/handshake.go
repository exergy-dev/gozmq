package security

import (
	"fmt"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// The helpers below give the shipped mechanisms one spelling for the errors
// and input guards every handshake step needs. CURVE, in its own package,
// uses them too, which is why they are exported. mech names the mechanism
// ("plain", "curve", ...) in the error text.

// Malformed returns an error wrapping gozmq.ErrMalformedFrame.
func Malformed(mech, format string, args ...any) error {
	return fmt.Errorf("gozmq/security/"+mech+": "+format+": %w", append(args, zmqerr.ErrMalformedFrame)...)
}

// AuthFailed returns an error wrapping gozmq.ErrAuthFailed.
func AuthFailed(mech, format string, args ...any) error {
	return fmt.Errorf("gozmq/security/"+mech+": "+format+": %w", append(args, zmqerr.ErrAuthFailed)...)
}

// RequireInput returns a malformed-frame error if in is nil: a handshake
// step awaiting what from the peer was driven without it.
func RequireInput(mech string, in []byte, what string) error {
	if in == nil {
		return Malformed(mech, "nil peer input awaiting %s", what)
	}
	return nil
}

// RequireNoInput returns a malformed-frame error if in is not nil: a seed
// step, which has nothing to consume, was handed input.
func RequireNoInput(mech string, in []byte, what string) error {
	if in != nil {
		return Malformed(mech, "unexpected input on %s", what)
	}
	return nil
}

// DecodeReply parses in as the peer's handshake command want and returns its
// body. A peer that rejects the handshake answers with ERROR instead; that
// is reported as an error wrapping gozmq.ErrAuthFailed carrying the peer's
// reason (ErrMalformedFrame if the reason is truncated). Any other command
// wraps ErrMalformedFrame.
func DecodeReply(in []byte, want string) ([]byte, error) {
	cmd, err := zmtp.DecodeCommand(in)
	if err != nil {
		return nil, fmt.Errorf("gozmq/security: parse %s: %w", want, err)
	}
	switch cmd.Name {
	case want:
		return cmd.Body, nil
	case zmtp.CmdError:
		reason, err := zmtp.DecodeErrorReason(cmd.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("gozmq/security: peer rejected handshake: %q: %w", reason, zmqerr.ErrAuthFailed)
	}
	return nil, fmt.Errorf("gozmq/security: expected %s or ERROR, got %q: %w", want, cmd.Name, zmqerr.ErrMalformedFrame)
}
