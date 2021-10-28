package models

import (
	"THSS_DDBMS/src/labrpc"
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

func TestScanTable(t *testing.T) {
	network := labrpc.MakeNetwork()
	n := NewNode(strconv.Itoa(0))
	service := labrpc.MakeService(n)
	server := labrpc.MakeServer()
	server.AddService(service)
	network.AddServer("server0", server)

	ts := &TableSchema{TableName: "table0", ColumnSchemas: []ColumnSchema{
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
		err := n.Insert("table0", &row)
		if err != nil {
			t.Error(err.Error())
		}
	}

	end := network.MakeEnd("client0")
	network.Connect("client0", "server0")
	network.Enable("client0", true)

	result := Dataset{}
	end.Call("Node.ScanTable", "table0", &result)
	if len(result.Rows) != 3 {
		println("Table content is incorrect")
	}
	fmt.Printf("%s\n", result.Schema.TableName)
	headers := ""
	for _, schema := range result.Schema.ColumnSchemas {
		headers = headers + schema.Name + " "
	}
	fmt.Printf(headers + "\n")

	for _, row := range result.Rows {
		fmt.Printf("%v\n", row)
	}
}
