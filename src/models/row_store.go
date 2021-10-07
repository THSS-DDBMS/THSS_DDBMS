package models

import (
	"container/list"
)

// Row is just an array of objects
type Row []interface{}

// Equals compares two rows by their length and each element
func (r *Row) Equals(another *Row) bool {
	if len(*r) != len(*another) {
		return false
	}
	for i, val := range *r {
		if val != (*another)[i] {
			return false
		}
	}
	return true
}

// EqualsWithColumnMapping compares two rows each element with the provided columnMapping, which indicate the index of
// each column of this row in another row. This method assumes, as the columnMapping is provided, the two rows have the
// same length.
func (r *Row) EqualsWithColumnMapping(another *Row, columnMapping []int) bool {
	for i, column := range *r {
		if column != (*another)[columnMapping[i]] {
			return false
		}
	}
	return true
}

// RowStore manages the storage of rows and provide simple read-write interfaces.
// Notice that the store does not guarantee any constraints, and it is the responsibility of the caller to check
// constraints like primary key and uniqueness before calling the methods in RowStore.
type RowStore interface {
	count() int
	iterator() RowIterator
	// the row will be copied into the store instead of directly store the reference
	insert(row *Row)
	// only removes the first row that equals to the argument
	remove(row *Row)
}

// RowIterator iterates rows in a RowStore.
type RowIterator interface {
	HasNext() bool
	Next() *Row
}

// MemoryListRowStore uses a linked list to store rows in memory.
type MemoryListRowStore struct {
	rows *list.List
}

func NewMemoryListRowStore() *MemoryListRowStore {
	return &MemoryListRowStore{rows: list.New()}
}

func (s *MemoryListRowStore) count() int {
	return s.rows.Len()
}

func (s *MemoryListRowStore) iterator() RowIterator {
	return NewMemoryListRowIterator(s.rows)
}

func (s *MemoryListRowStore) insert(row *Row) {
	s.rows.PushBack(*row)
}

func (s *MemoryListRowStore) remove(row *Row) {
	curr := s.rows.Front()
	for curr != nil {
		// find the first row that equals the argument
		r,_ := curr.Value.(Row)
		if r.Equals(row) {
			s.rows.Remove(curr)
			return
		}
		curr = curr.Next()
	}
}

type MemoryListRowIterator struct {
	next *list.Element
	rows *list.List
}

func NewMemoryListRowIterator(rows *list.List) RowIterator{
	iter := &MemoryListRowIterator{rows.Front(), rows}
	return iter
}

func (iter *MemoryListRowIterator) HasNext() bool {
	return iter.next != nil
}

func (iter *MemoryListRowIterator) Next() *Row {
	if iter.next == nil {
		return nil
	} else {
		t,_ := iter.next.Value.(Row)
		iter.next = iter.next.Next()
		return &t
	}
}





