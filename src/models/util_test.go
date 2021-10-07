package models

import "testing"

func TestCompareDataset(t *testing.T) {
	a := Dataset{
		Schema: TableSchema{
			"a",
			[]ColumnSchema {
				{"c1", TypeInt32},
				{"c2", TypeFloat},
				{"c3", TypeString},
			},
		},

		Rows: []Row{
			{1, 1.0, "1.0"},
			{2, 2.0, "2.0"},
			{3, 3.0, "3.0"},
		},
	}

	b := Dataset{
		Schema: TableSchema{
			"b",
			[]ColumnSchema {
				{"c3", TypeString},
				{"c2", TypeFloat},
				{"c1", TypeInt32},
			},
		},

		Rows: []Row{
			{"3.0", 3.0, 3},
			{"2.0", 2.0, 2},
			{"1.0", 1.0, 1},
		},
	}

	caseNum := 0
	if !compareDataset(a, b) {
		t.Errorf("Two datasets should be equal, caseNum: %d", caseNum)
	}

	// change a column
	caseNum ++
	b.Schema.ColumnSchemas[1].Name = "c4"
	if compareDataset(a, b) {
		t.Errorf("Two datasets should not be equal, caseNum: %d", caseNum)
	}

	// change a row
	caseNum ++
	b.Schema.ColumnSchemas[1].Name = "c2"
	b.Rows[0][0] = "4.0"
	if compareDataset(a, b) {
		t.Errorf("Two datasets should not be equal, caseNum: %d", caseNum)
	}

	// add a column
	caseNum ++
	b.Rows[0][0] = "3.0"
	b.Schema.ColumnSchemas = []ColumnSchema {
		{"c3", TypeString},
		{"c2", TypeFloat},
		{"c1", TypeInt32},
		{"c4", TypeBoolean},
	}
	if compareDataset(a, b) {
		t.Errorf("Two datasets should not be equal, caseNum: %d", caseNum)
	}

	// add a row
	caseNum ++
	b.Schema.ColumnSchemas = []ColumnSchema {
		{"c3", TypeString},
		{"c2", TypeFloat},
		{"c1", TypeInt32},
	}
	b.Rows = []Row{
		{"4.0", 4.0, 4},
		{"3.0", 3.0, 3},
		{"2.0", 2.0, 2},
		{"1.0", 1.0, 1},
	}
	if compareDataset(a, b) {
		t.Errorf("Two datasets should not be equal, caseNum: %d", caseNum)
	}
}
