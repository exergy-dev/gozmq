package typed_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/exergy-dev/gozmq/typed"
)

type thing struct {
	N    int    `json:"n"`
	Name string `json:"name"`
}

func TestCodec_Roundtrip(t *testing.T) {
	t.Parallel()
	c := typed.JSONCodec{}
	want := thing{N: 7, Name: "lucky"}
	b, err := c.Marshal(want)
	require.NoError(t, err, "marshal")
	var got thing
	require.NoError(t, c.Unmarshal(b, &got), "unmarshal")
	require.Equal(t, want, got)
}

func TestCodec_UnmarshalError(t *testing.T) {
	t.Parallel()
	c := typed.JSONCodec{}
	var got thing
	require.Error(t, c.Unmarshal([]byte("not json"), &got), "want error on malformed json")
}
