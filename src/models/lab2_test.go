package models

import "testing"
import "../labrpc"

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
	studentTablePartitionRules := "some rules for student table"
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

	courseRegistrationTableName := "courseRegistration"
	courseRegistrationTablePartitionRules := "some rules for course registration table"
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
