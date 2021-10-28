package models

import (
	"encoding/json"
	"fmt"
	"testing"

	"../labrpc"
)

func ignoredTestLab3Basic(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0|1": {"predicate": {"courseId":[{"op": "=", "val": 1}]}, "column": ["sid", "courseId"]},"1|2": {"predicate": {"courseId":[{"op": "=", "val": 2}]}, "column": ["sid", "courseId"]},"2|0": {"predicate": {"courseId":[{"op": "=", "val": 2}]}, "column": ["sid", "courseId"]}}`), &i)
	if err != nil {
		return
	}
	m := i.(map[string]interface{})
	rules, _ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	studentTableName := "student"
	studentTablePartitionRules := rules
	ts := &TableSchema{TableName: studentTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}
	replyMsg := ""
	cli.Call("Cluster.CreateTable", []interface{}{ts, studentTablePartitionRules}, replyMsg)

	studentRows := []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}
	replyMsg = ""
	for _, row := range studentRows {
		cli.Call("Cluster.Insert", []interface{}{studentTableName, row}, &replyMsg)
	}

	// create fragment rules
	jsonCourse := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2|0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	rulesCourse, _ := json.Marshal(jsonCourse)

	courseRegistrationTableName := "courseRegistration"
	courseRegistrationTablePartitionRules := rulesCourse
	ts = &TableSchema{TableName: courseRegistrationTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}
	replyMsg = ""
	cli.Call("Cluster.CreateTable", []interface{}{ts, courseRegistrationTablePartitionRules}, replyMsg)

	courseRegistrationRows := []Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}
	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.Insert", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}

	// perform a join and check the result
	results := Dataset{}
	// disconnect node 2 and test the duplication

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
		Rows: []Row{
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
