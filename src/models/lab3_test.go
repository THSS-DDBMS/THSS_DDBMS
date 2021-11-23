package models

import (
	"../labrpc"
	"encoding/json"
	"testing"
)
func defineTablesLab3() {
	studentTableSchema = &TableSchema{TableName: studentTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}

	courseRegistrationTableSchema = &TableSchema{TableName: courseRegistrationTableName, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}

	studentRows = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}

	courseRegistrationRows = []Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}

	joinedTableSchema = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"name", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
		},
	}

	joinedTableContent = []Row{
		{0, "John", 22, 4.0, 0},
		{0, "John", 22, 4.0, 1},
		{1, "Smith", 23, 3.6, 0},
		{2, "Hana", 21, 4.0, 2},
	}
}


func setupLab3() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = NewCluster(5, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	defineTablesLab3()
}

// student table is held by three nodes and courseRegistration table is held by the last node
func TestLab3NonOverlapping(t *testing.T) {
	setupLab3()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node2
	m = map[string]interface{}{
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// student table is held by node 0, 1, 2 and courseRegistration is held by node 0, 1, 2
func TestLab3FullyOverlapping(t *testing.T) {
	setupLab3()

	// use the client to create table and insert
	// divide student table into two partitions and assign their replica to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node 0, 1
	m = map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// two tables are distributed to node0
func TestLab3FullyCentralized(t *testing.T) {
	setup()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0.0,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node0
	m = map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}


// student table is distributed to node0 and node1, courseRegistration table is distributed to node1 and node2
func TestLab3PartiallyOverlapping(t *testing.T) {
	setupLab3()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0, node1, node 2 and node 3
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"0|3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"3|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// courseRegistration table is empty in this test
func TestLab3EmptyTable(t *testing.T) {
	setupLab3()

	courseRegistrationRows = []Row {}
	joinedTableContent = []Row {}

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1|3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
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
					"op":  ">",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// there is no matching tuple in this test
func TestLab3NoMatching(t *testing.T) {
	setupLab3()

	courseRegistrationRows = []Row{
		{10, 0},
		{10, 1},
		{11, 0},
		{12, 2},
	}
	joinedTableContent = []Row {}

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2|3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	buildTablesLab3(cli)
	insertDataLab3(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema,
		Rows: joinedTableContent,
	}
	if !datasetDuplicateChecking(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}


// compare two datasets with replica, ignoring the names of them and the order of columns and rows
func datasetDuplicateChecking(a Dataset, b Dataset) bool {
	columnMapping := compareDatasetSchema(a.Schema, b.Schema)
	if columnMapping == nil {
		return false
	}
	if len(a.Rows) == len(b.Rows) {
		return compareRows(a.Rows, b.Rows, columnMapping)
	}
	return false
}

func buildTablesLab3(cli *labrpc.ClientEnd)  {
	replyMsg := ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{courseRegistrationTableSchema, courseRegistrationTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{studentTableSchema, studentTablePartitionRules}, &replyMsg)
}

func insertDataLab3(cli *labrpc.ClientEnd) {
	replyMsg := ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}
}