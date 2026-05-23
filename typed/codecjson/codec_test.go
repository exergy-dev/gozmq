package codecjson_test

import (
	"testing"

	"github.com/rkm/gozmq/typed/codecjson"
)

type thing struct {
	N    int    `json:"n"`
	Name string `json:"name"`
}

func TestCodec_Roundtrip(t *testing.T) {
	c := codecjson.Codec{}
	want := thing{N: 7, Name: "lucky"}
	b, err := c.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got thing
	if err := c.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
}

func TestCodec_UnmarshalError(t *testing.T) {
	c := codecjson.Codec{}
	var got thing
	if err := c.Unmarshal([]byte("not json"), &got); err == nil {
		t.Fatal("want error on malformed json, got nil")
	}
}
