package name

import (
	"testing"
	"fmt"
)

func TestNewNameFromValue(t *testing.T) {
	n := new(Name)
	n.Position = 1234
	n.BlockId = 55
	n.RegionId = 42
	s := n.ToString()
	fmt.Println(s)
	n1, err := ParseNameFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	if n1.Position != n.Position ||
		n1.BlockId != n.BlockId ||
		n1.RegionId != n.RegionId {
			t.Fatal("fail")
	}
}