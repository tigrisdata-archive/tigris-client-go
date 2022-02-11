package driver

import "io"

type Iterator interface {
	More() bool
	Next() (Document, error)
}

type streamReader interface {
	read() (Document, error)
}

type readIterator struct {
	streamReader
	eof bool
	doc Document
	err error
}

func (i *readIterator) More() bool {
	if i.eof {
		return false
	}
	// document or error from previous call wasn't popped yet
	if i.doc != nil || i.err != nil {
		return true
	}
	doc, err := i.read()
	if err == io.EOF {
		i.eof = true
		return false
	}
	if err != nil {
		i.err = err
		return true
	}
	i.doc = doc
	return true
}

func (i *readIterator) Next() (Document, error) {
	if i.err != nil {
		i.eof = true
		return nil, i.err
	}
	d := i.doc
	i.doc = nil
	return d, nil
}
