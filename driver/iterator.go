package driver

import "io"

type Iterator interface {
	Next(d *Document) bool
	Err() error
}

type streamReader interface {
	read() (Document, error)
	close() error
}

type readIterator struct {
	streamReader
	eof bool
	err error
}

func (i *readIterator) Next(d *Document) bool {
	if i.eof {
		return false
	}

	doc, err := i.read()
	if err == io.EOF {
		i.eof = true
		_ = i.close()
		return false
	}
	if err != nil {
		i.eof = true
		i.err = err
		_ = i.close()
		return false
	}

	*d = doc
	return true
}

func (i *readIterator) Err() error {
	return i.err
}
