// Package codecjson provides an encoding/json-backed implementation of
// typed.Codec. It lives in a subpackage so that the typed package itself
// expresses no preference for a serialization format.
package codecjson

import "encoding/json"

// Codec marshals and unmarshals values with encoding/json. The zero
// value is ready to use and safe for concurrent use.
type Codec struct{}

// Marshal implements typed.Codec.
func (Codec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements typed.Codec.
func (Codec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
