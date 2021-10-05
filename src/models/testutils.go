package models

// compare two arrays of Rows, the order of rows matters. For example,
// if expected = {{1} {2} {3}} and actual = {{3} {2} {1}}, then expected != actual.
func compareRowsOrdered(expected []Row, actual []Row) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i, row := range expected {
		if !row.Equals(&actual[i]) {
			return false
		}
	}
	return true
}

// compare two arrays of Rows, the order of rows does not matter. For example,
// if expected = {{1} {2} {3}} and actual = {{3} {2} {1}}, then expected == actual.
func compareRowsDisordered(expected []Row, actual []Row) bool {
	if len(expected) != len(actual) {
		return false
	}
	// make sure each row in expected exists in actual and the opposite also holds
	for _, expectedRow := range expected {
		matched := false
		for _, actualRow := range actual {
			if expectedRow.Equals(&actualRow) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	for _, actualRow := range actual {
		matched := false
		for _, expectedRow := range expected {
			if expectedRow.Equals(&actualRow) {
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