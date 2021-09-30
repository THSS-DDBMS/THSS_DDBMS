package models

// Table is an in-memory two-dimensional table which consists of a table schema and a row store
// it is not yet a relational table as it does not support primary keys or other constraints.
type Table struct {
	schema *TableSchema
	rowStore RowStore
}

func NewTable(schema *TableSchema, rowStore RowStore) *Table {
	return &Table{schema: schema, rowStore: rowStore}
}

// GetColumnCount returns the number of columns in the table.
func (t *Table) GetColumnCount() int {
	return len(t.schema.ColumnSchemas)
}

// GetColumnName returns the name of the ith column, or an empty string if the index is invalid.
func (t *Table) GetColumnName(i int) string  {
	if i < 0 || i >= len(t.schema.ColumnSchemas) {
		return ""
	}
	return t.schema.ColumnSchemas[i].Name
}

// GetColumnType the return value is one in datatype.go, or -1 if the index is invalid.
func (t *Table) GetColumnType(i int) int {
	if i < 0 || i >= len(t.schema.ColumnSchemas) {
		return -1
	}
	return t.schema.ColumnSchemas[i].DataType
}

func (t *Table) RowIterator() RowIterator {
	return t.rowStore.iterator()
}

// Insert inserts a row into the store. The row will be copied by the store.
func (t *Table) Insert(row *Row) {
	t.rowStore.insert(row)
}

// Remove removes a row from the store, and does not concern whether it exists.
func (t *Table) Remove(row *Row) {
	t.rowStore.remove(row)
}

// Count returns how many rows are in the table.
func (t *Table) Count() int {
	return t.rowStore.count()
}