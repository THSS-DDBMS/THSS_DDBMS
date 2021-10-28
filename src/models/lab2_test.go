package models

import (
	"encoding/json"
	"testing"
)
import "THSS_DDBMS/src/labrpc"

func ignoredTestLab2Basic(t *testing.T) {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// use the client to create table and insert
	studentTableName := "student"
	m := map[string]interface{}{
		"0":map[string]interface{}{
			"predicate":map[string]interface{}{
				"grade":[...]map[string]interface{}{{
					"op": "<=",
					"val": 3.6,
				},
				},
			},
			"column":[...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1":map[string]interface{}{
			"predicate":map[string]interface{}{
				"grade":[...]map[string]interface{}{{
					"op": ">",
					"val": 3.6,
				},
				},
			},
			"column":[...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ := json.Marshal(m)
	ts := &TableSchema{TableName: studentTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, studentTablePartitionRules}, &replyMsg)

	studentRows := []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}
	replyMsg = ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}

	courseRegistrationTableName := "courseRegistration"
	m = map[string]interface{}{
		"2":map[string]interface{}{
			"predicate":map[string]interface{}{
				"courseId":[...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				},
				},
			},
			"column":[...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ := json.Marshal(m)
	ts = &TableSchema{TableName: courseRegistrationTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, courseRegistrationTablePartitionRules}, &replyMsg)

	courseRegistrationRows := []Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}
	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{"sid", TypeInt32},
				{"name", TypeString},
				{"age", TypeInt32},
				{"grade", TypeFloat},
				{"courseId", TypeInt32},
			},
		},
		Rows:   []Row{
			{0, "John", 22, 4.0, 0},
			{0, "John", 22, 4.0, 1},
			{1, "Smith", 23, 3.6, 0},
			{2, "Hana", 21, 4.0, 2},
		},
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}
