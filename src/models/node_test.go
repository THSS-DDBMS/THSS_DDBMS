package models

import (
	"fmt"
	"strconv"
	"testing"
)

func TestBasic(t *testing.T) {
	n := NewNode(strconv.Itoa(0))
	ts := &TableSchema{TableName: "table1", ColumnSchemas: []ColumnSchema{
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}
	err := n.CreateTable(ts)
	if err != nil {
		t.Error(err.Error())
	}

	rows := []Row{
		{"John", 22, 4.0},
		{"Smith", 23, 3.6},
		{"Hana", 21, 4.0},
	}
	for _, row := range rows {
		err := n.Insert("table1", &row)
		if err != nil {
			t.Error(err.Error())
		}
	}

	count, err := n.count("table1")
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Printf("%d records\n", count)

	iter, err := n.IterateTable("table1")
	if err != nil {
		t.Error(err.Error())
	}
	for iter.HasNext() {
		fmt.Printf("%v\n", *iter.Next())
	}

	err = n.Remove("table1", &rows[1])
	if err != nil {
		t.Error(err.Error())
	}

	count, err = n.count("table1")
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Printf("%d records\n", count)

	iter, err = n.IterateTable("table1")
	if err != nil {
		t.Error(err.Error())
	}
	for iter.HasNext() {
		fmt.Printf("%v\n", *iter.Next())
	}
}

func TestEmptyTable(t *testing.T) {
	n := NewNode(strconv.Itoa(0))
	ts := &TableSchema{TableName: "table1", ColumnSchemas: []ColumnSchema{
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}
	err := n.CreateTable(ts)
	if err != nil {
		t.Error(err.Error())
	}

	iter, err := n.IterateTable("table1")
	if err != nil {
		t.Error(err.Error())
	}
	for iter.HasNext() {
		fmt.Printf("%v\n", *iter.Next())
	}
}
