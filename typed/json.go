package typed

import "encoding/json"

// JSONCodec is a Codec backed by encoding/json. The zero value is ready to
// use and safe for concurrent use.
type JSONCodec struct{}

// Marshal implements Codec.
func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements Codec.
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
