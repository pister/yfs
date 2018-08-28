package name

import (
	"testing"
)

func TestNewNameFromValue(t *testing.T) {
	var v1 uint64 = 1234567890123456
	name := NewNameFromValue(v1)
	v2 := name.ToInt64()
	if v1 != v2 {
		t.Fatalf("except: %d, but %d", v1, v2)
	}
}
