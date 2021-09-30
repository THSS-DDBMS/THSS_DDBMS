package models

// TableSchema contains the name of the table and the definition of each column
type TableSchema struct {
	TableName string
	ColumnSchemas []ColumnSchema
}
