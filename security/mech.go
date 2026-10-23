// Shared mechanism plumbing. Every ZMTP security mechanism carries the same
// local/peer metadata state and (server-side) RFC 27 ZAP authorization hook,
// so MechBase centralizes that state plus the corresponding
// Mech accessor methods. NULL and PLAIN embed it directly;
// CURVE (security/curve) embeds it across the package boundary, which is why
// the type and the helpers a mechanism needs are exported. The dependency
// points strictly from curve to security — nothing in this package may
// reference CURVE types.

package security

import (
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/exergy-dev/gozmq/internal/zmqerr"
)

// MechBase is the state shared by every security mechanism: the role, the
// local metadata advertised during the handshake, the peer metadata learned
// from it, and the optional server-side ZAP handler with its verdict.
// Mechanisms embed it by value; like the mechanisms themselves it is not safe
// for concurrent use.
type MechBase struct {
	role      Role
	localMeta map[string]string
	peerMeta  map[string]string
	zap       ZAPHandler
	zapDomain string
	zapAddr   string
	zapUserID string
	zapMeta   map[string]string
}

// userIDProperty is the peer property carrying the ZAP user-id, named as
// libzmq's ZMQ_MSG_PROPERTY_USER_ID.
const userIDProperty = "User-Id"

// NewMechBase returns the shared mechanism state for role, holding a private
// copy of localMeta. A nil localMeta is treated as an empty property list.
func NewMechBase(role Role, localMeta map[string]string) MechBase {
	return MechBase{role: role, localMeta: maps.Clone(localMeta)}
}

// Role reports which side of the handshake this mechanism drives.
func (b *MechBase) Role() Role { return b.role }

// AsServer reports the value of the greeting as-server byte.
func (b *MechBase) AsServer() bool { return b.role == RoleServer }

// Metadata returns a copy of the local metadata properties sent in the
// mechanism's metadata-bearing handshake command (READY or INITIATE). The
// copy guards against external mutation after construction.
func (b *MechBase) Metadata() map[string]string { return maps.Clone(b.localMeta) }

// PeerMetadata returns a copy of the peer metadata stored by
// [MechBase.MergePeerMetadata], or nil before then.
func (b *MechBase) PeerMetadata() map[string]string { return maps.Clone(b.peerMeta) }

// ConfigureZAP installs a server-side ZAP handler. The socket calls it, before
// the handshake, on any server-side mechanism that has this method when its
// Context has a handler, so a mechanism embedding MechBase only needs to call
// [MechBase.AuthorizeZAP]. Client-side mechanisms ignore it.
func (b *MechBase) ConfigureZAP(handler ZAPHandler, domain, address string) {
	if b.role != RoleServer {
		return
	}
	b.zap = handler
	b.zapDomain = domain
	b.zapAddr = address
}

// AuthorizeZAP consults the configured handler with the mechanism-specific
// credentials. A rejection is an authorization failure whatever the handler
// returned, so its error is wrapped with ErrAuthFailed unless it already
// wraps it (the handler's error stays in the chain). It is a no-op
// (returning nil) when no handler is configured. On success the handler's
// user-id and metadata are kept for [MechBase.MergePeerMetadata].
func (b *MechBase) AuthorizeZAP(mech string, credentials [][]byte) error {
	if b.zap == nil {
		return nil
	}
	userID, meta, err := b.zap(b.zapDomain, b.zapAddr, mech, credentials)
	if err != nil {
		if !errors.Is(err, zmqerr.ErrAuthFailed) {
			err = fmt.Errorf("%w: ZAP handler: %w", zmqerr.ErrAuthFailed, err)
		}
		return err
	}
	b.zapUserID, b.zapMeta = userID, maps.Clone(meta)
	return nil
}

// MergePeerMetadata stores props as the peer metadata, adding what ZAP
// returned. Mechanisms call it from the handshake step that parses the peer's
// metadata-bearing command. ZAP metadata only fills in property names the
// peer did not send (names compare case-insensitively, as in ZMTP), so a
// policy cannot rewrite the peer's Identity or Socket-Type. A non-empty ZAP
// user-id becomes the "User-Id" property, as in libzmq, replacing any the peer
// claimed for itself.
func (b *MechBase) MergePeerMetadata(props map[string]string) {
	peer := make(map[string]string, len(props)+len(b.zapMeta)+1)
	maps.Copy(peer, props)
	for name, value := range b.zapMeta {
		if !hasProperty(props, name) {
			peer[name] = value
		}
	}
	if b.zapUserID != "" {
		for name := range peer {
			if strings.EqualFold(name, userIDProperty) {
				delete(peer, name)
			}
		}
		peer[userIDProperty] = b.zapUserID
	}
	b.peerMeta = peer
}

func hasProperty(props map[string]string, name string) bool {
	for k := range props {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

// identityFraming is embedded by the mechanisms whose per-frame transform is
// the identity (NULL, PLAIN); it opts them into the session's zero-copy
// framing path. It is deliberately not part of MechBase: a custom mechanism
// must declare identity framing itself, never inherit it.
type identityFraming struct{}

// IdentityFraming reports that frames cross the wire unmodified.
func (identityFraming) IdentityFraming() bool { return true }

// EncodeFrame returns its input: the identity transform.
func (identityFraming) EncodeFrame(flags uint8, plain []byte) (uint8, []byte, error) {
	return flags, plain, nil
}

// DecodeFrame returns its input: the identity transform.
func (identityFraming) DecodeFrame(flags uint8, wire []byte) (uint8, []byte, error) {
	return flags, wire, nil
}
