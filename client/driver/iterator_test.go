package driver

import "testing"

func testIterator(t *testing.T, it Iterator) {
}

func TestHTTPIterator(t *testing.T) {
	testIterator(t, nil)
}

func TestGRPCIterator(t *testing.T) {
	testIterator(t, nil)
}
