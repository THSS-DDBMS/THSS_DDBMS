package models

// compare two datasets, ignoring the names of them and the order of columns and rows
func compareDataset(a Dataset, b Dataset) bool {
	columnMapping := compareDatasetSchema(a.Schema, b.Schema)
	if columnMapping == nil {
		return false
	}

	return compareRows(a.Rows, b.Rows, columnMapping)
}

func compareRows(a []Row, b []Row, columnMapping []int) bool {
	if len(a) != len(b) {
		return false
	}
	// ensure that each row in a can be found in b, and each row in b can be found in a
	for _, rowA := range a {
		matched := false
		for _, rowB := range b {
			if rowA.EqualsWithColumnMapping(&rowB, columnMapping) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	for _, rowB := range b {
		matched := false
		for _, rowA := range a {
			if rowA.EqualsWithColumnMapping(&rowB, columnMapping) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// compare two TableSchemas, ignoring the names of them and the order of rows
// if the two do not equal, nil will be returned;
// otherwise, a column mapping will be returned,
// for example, if TableSchema a contains 3 columns [c1, c2, c3], and TableSchema b contains 3 columns [c3, c2, c1],
// then [3, 2, 1] will be returned,
// indicating that the first column in TableSchema a is the third column in b and so on.
func compareDatasetSchema(a TableSchema, b TableSchema) []int {
	if len(a.ColumnSchemas) != len(b.ColumnSchemas) {
		return nil
	}

	columnMapping := make([]int, len(a.ColumnSchemas))
	// find the position of a's each column in b
	for i, columnSchemaA := range a.ColumnSchemas {
		matched := false
		for j, columnSchemaB := range b.ColumnSchemas {
			if columnSchemaA.Name == columnSchemaB.Name {
				matched = true
				columnMapping[i] = j
				break
			}
		}
		if !matched {
			return nil
		}
	}
	return columnMapping
}