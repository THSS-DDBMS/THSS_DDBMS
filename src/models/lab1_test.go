package models

import (
	"fmt"
	"testing"
)
import "../labrpc"
import "encoding/json"

func TestLab1Basic(t *testing.T) {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{"0": {"predicate": {"BUDGET":[{"op": "<=", "val": 250000}]}, "column": ["PNO", "BUDGET"]},"1": {"predicate": {"BUDGET":[{"op": "<=", "val": 250000}]}, "column": ["PNO", "PNAME", "LOC"]},"2": {"predicate": {"BUDGET":[{"op": ">", "val": 250000}]}, "column": ["PNO", "PNAME", "BUDGET", "LOC"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "budget"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "PNO", DataType: TypeString},
		{Name: "PNAME", DataType: TypeString},
		{Name: "BUDGET", DataType: TypeDouble},
		{Name: "LOC", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{"P1", "Instrumentation", 150000, "Montreal"},
		{"P2", "Database Develop.", 135000, "New York"},
		{"P3", "CAD/CAM", 250000, "New York"},
		{"P4", "Maintenance", 310000, "Paris"},
	}
	replyMsg = ""
	for _, row := range budgetRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{budgetTableName, row}, &replyMsg)
	}

	end0 := network.MakeEnd("client0")
	network.Connect("client0", "Node0")
	network.Enable("client0", true)

	table0 := Dataset{}
	end0.Call("Node.ScanTable", budgetTableName, &table0)

	end1 := network.MakeEnd("client1")
	network.Connect("client1", "Node1")
	network.Enable("client1", true)

	table1 := Dataset{}
	end1.Call("Node.ScanTable", budgetTableName, &table1)

	end2 := network.MakeEnd("client2")
	network.Connect("client2", "Node2")
	network.Enable("client2", true)

	table2 := Dataset{}
	end2.Call("Node.ScanTable", budgetTableName, &table2)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "PNO", DataType: TypeString},
				{Name: "BUDGET", DataType: TypeDouble},
			},
		},
		Rows:   []Row{
			{"P1", 150000},
			{"P2", 135000},
			{"P3", 250000},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "PNO", DataType: TypeString},
				{Name: "PNAME", DataType: TypeString},
				{Name: "LOC", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"P1", "Instrumentation", "Montreal"},
			{"P2", "Database Develop.", "New York"},
			{"P3", "CAD/CAM", "New York"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "PNO", DataType: TypeString},
				{Name: "PNAME", DataType: TypeString},
				{Name: "BUDGET", DataType: TypeDouble},
				{Name: "LOC", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"P4", "Maintenance", 310000, "Paris"},
		},
	}

	if !compareDataset(expectedDataset0, table0) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset0, table0)
	}
	if !compareDataset(expectedDataset1, table1) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset1, table1)
	}
	if !compareDataset(expectedDataset2, table2) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset2, table2)
	}
}
