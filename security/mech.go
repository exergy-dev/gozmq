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
	"maps"

	"github.com/exergy-dev/gozmq/internal/zmtp"
)

// MechBase is the state shared by every security mechanism: the role, the
// local metadata advertised during the handshake, the peer metadata learned
// from it, and the optional server-side ZAP authorizer. Mechanisms embed it
// by value; like the mechanisms themselves it is not safe for concurrent
// use.
type MechBase struct {
	role      Role
	localMeta map[string]string
	peerMeta  map[string]string
	zapAuth   ZAPAuthorizer
	zapDomain string
	zapAddr   string
	zapMeta   map[string]string
}

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

// SetPeerMetadata stores the peer's parsed metadata-bearing command
// properties. Mechanisms perform no validation here; Socket-Type
// compatibility checks live in the socket engine layer per design §5.3.
func (b *MechBase) SetPeerMetadata(props map[string]string) error {
	b.peerMeta = maps.Clone(props)
	return nil
}

// PeerMetadata returns the parsed peer metadata, or nil before the handshake
// completes. Not part of Mech but used by the session layer
// during metadata forwarding and by tests.
func (b *MechBase) PeerMetadata() map[string]string { return maps.Clone(b.peerMeta) }

// ConfigureZAP wires a server-side RFC 27 authorizer into the mechanism.
// Client-side mechanisms ignore the hook.
func (b *MechBase) ConfigureZAP(authorizer ZAPAuthorizer, domain, address string) {
	if b.role != RoleServer {
		return
	}
	b.zapAuth = authorizer
	b.zapDomain = domain
	b.zapAddr = address
}

// AuthorizeZAP consults the configured authorizer with the
// mechanism-specific credentials. It is a no-op (returning nil) when no
// authorizer is configured. On success the metadata from the ZAP reply is
// retained for MergePeerMetadata to overlay onto the peer metadata.
func (b *MechBase) AuthorizeZAP(mech string, credentials [][]byte) error {
	if b.zapAuth == nil {
		return nil
	}
	_, meta, err := b.zapAuth(b.zapDomain, b.zapAddr, mech, credentials)
	if err != nil {
		return err
	}
	b.zapMeta = maps.Clone(meta)
	return nil
}

// MergePeerMetadata stores props, overlaid with any ZAP-reply metadata, as
// the peer metadata. Mechanisms call it from the handshake step that parses
// the peer's metadata-bearing command.
func (b *MechBase) MergePeerMetadata(props map[string]string) {
	b.peerMeta = zmtp.MergeMeta(props, b.zapMeta)
}
