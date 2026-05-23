// Package codecjson provides an encoding/json-backed implementation of
// typed.Codec. It lives in a subpackage so callers who use a different
// codec never pull in encoding/json transitively.
package codecjson

import "encoding/json"

// Codec marshals and unmarshals values with encoding/json. The zero
// value is ready to use and safe for concurrent use.
type Codec struct{}

// Marshal implements typed.Codec.
func (Codec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements typed.Codec.
func (Codec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
