package models

import (
	"fmt"
	"testing"
)
import "THSS_DDBMS/src/labrpc"
import "encoding/json"

func TestLab1Case0(t *testing.T) {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(6, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{"0": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": "<", "val": 80000}]},"column": ["object_id", "address", "sale_price"]},"1": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": "<", "val": 80000}]},"column": ["object_id", "sale_terms", "verified_by"]},"2": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": ">=", "val": 80000},{"op": "<=", "val": 200000}]},"column": ["object_id", "address", "sale_price", "sale_terms", "verified_by"]},"3": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": ">", "val": 200000}]},"column": ["object_id", "address", "sale_price", "sale_terms", "verified_by"]},"4": {"predicate": {"sale_terms":[{"op": "!=", "val": "19-MULTI PARCEL ARM'S LENGTH"}]},"column": ["object_id", "address", "sale_price"]},"5": {"predicate": {"sale_terms":[{"op": "!=", "val": "19-MULTI PARCEL ARM'S LENGTH"}]},"column": ["object_id", "sale_terms", "verified_by"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "sales"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "object_id", DataType: TypeInt32},
		{Name: "address", DataType: TypeString},
		{Name: "sale_price", DataType: TypeDouble},
		{Name: "sale_terms", DataType: TypeString},
		{Name: "verified_by", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{11,"58 ADELAIDE ST 56/6",90000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{12,"58 ADELAIDE ST 56/6",169610,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{13,"58 ADELAIDE ST 56/6",1,"21-NOT USED","TITLE COMPANY"},
		{14,"15 E KIRBY 815",0,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{15,"6514 WOODWARD AVENUE",0,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{16,"158 W NEVADA",1,"21-NOT USED","TITLE COMPANY"},
		{17,"94 E GOLDEN GATE",1,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{18,"444 W WILLIS 78/102",213000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{19,"611 WOODWARD AVENUE",16000000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{20,"611 WOODWARD AVENUE",10,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{21,"611 WOODWARD AVENUE",0,"21-NOT USED","DEED"},
		{22,"1420 CHRYSLER",14000000,"03-ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{23,"1510 ST ANTOINE",14000000,"03-ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{24,"870 LOTHROP 06/1",133779,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{25,"729 PINGREE",1,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{26,"729 PINGREE",1,"21-NOT USED","OTHER"},
		{27,"6437 RUSSELL",2245,"13-GOVERNMENT","PROPERTY TRANSFER AFFIDAVIT"},
		{28,"9133 GOODWIN",0,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{29,"9676 DELMAR",14001,"21-NOT USED","TITLE COMPANY"},
		{30,"1001 W JEFFERSON 300/15H",25200,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
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

	end3 := network.MakeEnd("client3")
	network.Connect("client3", "Node3")
	network.Enable("client3", true)

	table3 := Dataset{}
	end3.Call("Node.ScanTable", budgetTableName, &table3)

	end4 := network.MakeEnd("client4")
	network.Connect("client4", "Node4")
	network.Enable("client4", true)

	table4 := Dataset{}
	end4.Call("Node.ScanTable", budgetTableName, &table4)

	end5 := network.MakeEnd("client5")
	network.Connect("client5", "Node5")
	network.Enable("client5", true)

	table5 := Dataset{}
	end5.Call("Node.ScanTable", budgetTableName, &table5)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
			},
		},
		Rows:   []Row{
			{1,"7729 GRANDVILLE",12125},
			{2,"19158 MALLINA",74687},
			{3,"19158 MALLINA",19000},
			{4,"19325 HARTWELL",0},
			{9,"2446 WOODWARD AVENUE 04/1",68500},
			{10,"44 ADELAIDE ST 49/6",75000},
			{30,"1001 W JEFFERSON 300/15H",25200},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{1,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{2,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{3,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{4,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{9,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{10,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{30,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{11,"58 ADELAIDE ST 56/6",90000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset3 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{18,"444 W WILLIS 78/102",213000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset4 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
			},
		},
		Rows:   []Row{
			{5,"4625 W FORT",40260},
			{6,"9520 W GRAND RIVER",2520000},
			{8,"111 CHANDLER",100},
			{12,"58 ADELAIDE ST 56/6",169610},
			{13,"58 ADELAIDE ST 56/6",1},
			{14,"15 E KIRBY 815",0},
			{15,"6514 WOODWARD AVENUE",0},
			{16,"158 W NEVADA",1},
			{17,"94 E GOLDEN GATE",1},
			{19,"611 WOODWARD AVENUE",16000000},
			{20,"611 WOODWARD AVENUE",10},
			{21,"611 WOODWARD AVENUE",0},
			{22,"1420 CHRYSLER",14000000},
			{23,"1510 ST ANTOINE",14000000},
			{24,"870 LOTHROP 06/1",133779},
			{25,"729 PINGREE",1},
			{26,"729 PINGREE",1},
			{27,"6437 RUSSELL",2245},
			{28,"9133 GOODWIN",0},
			{29,"9676 DELMAR",14001},
		},
	}

	expectedDataset5 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{5,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{6,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{8,"21-NOT USED","DEED"},
			{12,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{13,"21-NOT USED","TITLE COMPANY"},
			{14,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{15,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{16,"21-NOT USED","TITLE COMPANY"},
			{17,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{19,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{20,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{21,"21-NOT USED","DEED"},
			{22,"03-ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{23,"03-ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{24,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{25,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{26,"21-NOT USED","OTHER"},
			{27,"13-GOVERNMENT","PROPERTY TRANSFER AFFIDAVIT"},
			{28,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{29,"21-NOT USED","TITLE COMPANY"},
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
	if !compareDataset(expectedDataset3, table3) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset3, table3)
	}
	if !compareDataset(expectedDataset4, table4) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset4, table4)
	}
	if !compareDataset(expectedDataset5, table5) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset5, table5)
	}
}

func TestLab1Case1(t *testing.T)  {
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

func TestLab1Case2(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"age":[{"op":"<=","val":25}]},"column":["name","age","sex"]},"1":{"predicate":{"age":[{"op":">","val":25},{"op":"<=","val":50}]},"column":["name","age","sex"]},"2":{"predicate":{"age":[{"op":">","val":50}]},"column":["name","age","sex"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	classTableName := "class"
	ts := &TableSchema{TableName: classTableName, ColumnSchemas: []ColumnSchema{
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "sex", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{"Alan", 20, "Female"},
		{"Bob", 25, "Male"},
		{"Peter", 23, "Female"},
		{"Cathy", 26, "Male"},
		{"Danny", 30, "Male"},
		{"Jenny", 42, "Female"},
		{"Smith", 28, "Female"},
		{"Bush", 51, "Male"},
		{"Tiger", 18, "Male"},
		{"Franklin", 55, "Female"},
	}
	replyMsg = ""
	for _, row := range budgetRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{classTableName, row}, &replyMsg)
	}
	end0 := network.MakeEnd("client0")
	network.Connect("client0", "Node0")
	network.Enable("client0", true)

	table0 := Dataset{}
	end0.Call("Node.ScanTable", classTableName, &table0)

	end1 := network.MakeEnd("client1")
	network.Connect("client1", "Node1")
	network.Enable("client1", true)

	table1 := Dataset{}
	end1.Call("Node.ScanTable", classTableName, &table1)

	end2 := network.MakeEnd("client2")
	network.Connect("client2", "Node2")
	network.Enable("client2", true)

	table2 := Dataset{}
	end2.Call("Node.ScanTable", classTableName, &table2)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Alan", 20, "Female"},
			{"Bob", 25, "Male"},
			{"Peter", 23, "Female"},
			{"Tiger", 18, "Male"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Cathy", 26, "Male"},
			{"Danny", 30, "Male"},
			{"Jenny", 42, "Female"},
			{"Smith", 28, "Female"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Bush", 51, "Male"},
			{"Franklin", 55, "Female"},
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

func TestLab1Case3(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"sex":[{"op":"==","val":"Male"}]},"column":["name","age","sex"]},"1":{"predicate":{"sex":[{"op":"==","val":"Female"}]},"column":["name","sex"]},"2":{"predicate":{"sex":[{"op":"==","val":"Female"}]},"column":["name","age"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	classTableName := "class"
	ts := &TableSchema{TableName: classTableName, ColumnSchemas: []ColumnSchema{
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "sex", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{"Alan", 20, "Female"},
		{"Bob", 25, "Male"},
		{"Peter", 23, "Female"},
		{"Cathy", 26, "Male"},
		{"Danny", 30, "Male"},
		{"Jenny", 42, "Female"},
		{"Smith", 28, "Female"},
		{"Bush", 51, "Male"},
		{"Tiger", 18, "Male"},
		{"Franklin", 55, "Female"},
	}
	replyMsg = ""
	for _, row := range budgetRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{classTableName, row}, &replyMsg)
	}
	end0 := network.MakeEnd("client0")
	network.Connect("client0", "Node0")
	network.Enable("client0", true)

	table0 := Dataset{}
	end0.Call("Node.ScanTable", classTableName, &table0)

	end1 := network.MakeEnd("client1")
	network.Connect("client1", "Node1")
	network.Enable("client1", true)

	table1 := Dataset{}
	end1.Call("Node.ScanTable", classTableName, &table1)

	end2 := network.MakeEnd("client2")
	network.Connect("client2", "Node2")
	network.Enable("client2", true)

	table2 := Dataset{}
	end2.Call("Node.ScanTable", classTableName, &table2)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Bob", 25, "Male"},
			{"Cathy", 26, "Male"},
			{"Danny", 30, "Male"},
			{"Bush", 51, "Male"},
			{"Tiger", 18, "Male"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Alan", "Female"},
			{"Peter", "Female"},
			{"Jenny", "Female"},
			{"Smith", "Female"},
			{"Franklin", "Female"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
			},
		},
		Rows:   []Row{
			{"Alan", 20},
			{"Peter", 23},
			{"Jenny", 42},
			{"Smith", 28},
			{"Franklin", 55},
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

func TestLab1Case4(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"age":[{"op":"<=","val":50}],"sex":[{"op":"==","val":"Female"}]},"column":["name","age","sex"]},"1":{"predicate":{"age":[{"op":"<=","val":50}],"sex":[{"op":"==","val":"Male"}]},"column":["name","age","sex"]},"2":{"predicate":{"age":[{"op":">","val":50}]},"column":["name","age","sex"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	classTableName := "class"
	ts := &TableSchema{TableName: classTableName, ColumnSchemas: []ColumnSchema{
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "sex", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{"Alan", 20, "Female"},
		{"Bob", 25, "Male"},
		{"Peter", 23, "Female"},
		{"Cathy", 26, "Male"},
		{"Danny", 30, "Male"},
		{"Jenny", 42, "Female"},
		{"Smith", 28, "Female"},
		{"Bush", 51, "Male"},
		{"Tiger", 18, "Male"},
		{"Franklin", 55, "Female"},
	}
	replyMsg = ""
	for _, row := range budgetRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{classTableName, row}, &replyMsg)
	}
	end0 := network.MakeEnd("client0")
	network.Connect("client0", "Node0")
	network.Enable("client0", true)

	table0 := Dataset{}
	end0.Call("Node.ScanTable", classTableName, &table0)

	end1 := network.MakeEnd("client1")
	network.Connect("client1", "Node1")
	network.Enable("client1", true)

	table1 := Dataset{}
	end1.Call("Node.ScanTable", classTableName, &table1)

	end2 := network.MakeEnd("client2")
	network.Connect("client2", "Node2")
	network.Enable("client2", true)

	table2 := Dataset{}
	end2.Call("Node.ScanTable", classTableName, &table2)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Alan", 20, "Female"},
			{"Peter", 23, "Female"},
			{"Jenny", 42, "Female"},
			{"Smith", 28, "Female"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Bob", 25, "Male"},
			{"Cathy", 26, "Male"},
			{"Danny", 30, "Male"},
			{"Tiger", 18, "Male"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "name", DataType: TypeString},
				{Name: "age", DataType: TypeInt32},
				{Name: "sex", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"Bush", 51, "Male"},
			{"Franklin", 55, "Female"},
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

func TestLab1Case5(t *testing.T)  {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(4, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"LOC":[{"op":"==","val":"Montreal"}]},"column":["PNO","PNAME","LOC"]},"1":{"predicate":{"LOC":[{"op":"==","val":"New York"}]},"column":["PNO","PNAME","LOC"]},"2":{"predicate":{"BUDGET":[{"op":"<=","val":250000}]},"column":["PNO","BUDGET"]},"3":{"predicate":{"BUDGET":[{"op":">","val":250000}]},"column":["PNO","PNAME","BUDGET","LOC"]}}`), &i)
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
		{"P5", "Operating System", 260000, "Tokyo"},
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

	end3 := network.MakeEnd("client3")
	network.Connect("client3", "Node3")
	network.Enable("client3", true)

	table3 := Dataset{}
	end3.Call("Node.ScanTable", budgetTableName, &table3)

	expectedDataset0 := Dataset{
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
			{"P2", "Database Develop.", "New York"},
			{"P3", "CAD/CAM", "New York"},
		},
	}

	expectedDataset2 := Dataset{
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

	expectedDataset3 := Dataset{
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
			{"P5", "Operating System", 260000, "Tokyo"},
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
	if !compareDataset(expectedDataset3, table3) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset3, table3)
	}
}

func TestLab1Case6(t *testing.T)  {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"BUDGET":[{"op":"<=","val":500000}]},"column":["PNO","PNAME"]},"1":{"predicate":{"BUDGET":[{"op":"<=","val":500000}]},"column":["PNO","BUDGET"]},"2":{"predicate":{"BUDGET":[{"op":"<=","val":500000}]},"column":["PNO","LOC"]}}`), &i)
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
		{"P5", "Operating System", 260000, "Tokyo"},
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
				{Name: "PNAME", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"P1", "Instrumentation"},
			{"P2", "Database Develop."},
			{"P3", "CAD/CAM"},
			{"P4", "Maintenance"},
			{"P5", "Operating System"},
		},
	}

	expectedDataset1 := Dataset{
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
			{"P4", 310000},
			{"P5", 260000},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "PNO", DataType: TypeString},
				{Name: "LOC", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{"P1", "Montreal"},
			{"P2", "New York"},
			{"P3", "New York"},
			{"P4", "Paris"},
			{"P5", "Tokyo"},
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

func TestLab1Case7(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"sale_terms":[{"op":"==","val":"21-NOT USED"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"1":{"predicate":{"sale_terms":[{"op":"==","val":"19-MULTI PARCEL ARM'S LENGTH"}],"verified_by":[{"op":"==","val":"TITLE COMPANY"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"2":{"predicate":{"sale_terms":[{"op":"==","val":"19-MULTI PARCEL ARM'S LENGTH"}],"verified_by":[{"op":"==","val":"PROPERTY TRANSFER AFFIDAVIT"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "sales"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "object_id", DataType: TypeInt32},
		{Name: "address", DataType: TypeString},
		{Name: "sale_price", DataType: TypeDouble},
		{Name: "sale_terms", DataType: TypeString},
		{Name: "verified_by", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
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
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
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

func TestLab1Case8(t *testing.T) {
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
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"sale_price":[{"op":"<=","val":200000}],"address":[{"op":"==","val":"19158 MALLINA"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"1":{"predicate":{"sale_price":[{"op":"<=","val":200000}],"address":[{"op":"!=","val":"19158 MALLINA"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"2":{"predicate":{"sale_price":[{"op":">","val":200000}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "sales"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "object_id", DataType: TypeInt32},
		{Name: "address", DataType: TypeString},
		{Name: "sale_price", DataType: TypeDouble},
		{Name: "sale_terms", DataType: TypeString},
		{Name: "verified_by", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
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
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
			{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
			{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
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

func TestLab1Case9(t *testing.T) {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(5, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{"0":{"predicate":{"sale_price":[{"op":"<=","val":50000}],"verified_by":[{"op":"==","val":"TITLE COMPANY"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"1":{"predicate":{"sale_price":[{"op":"<=","val":50000}],"verified_by":[{"op":"==","val":"PROPERTY TRANSFER AFFIDAVIT"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"2":{"predicate":{"sale_price":[{"op":"<=","val":50000}],"verified_by":[{"op":"==","val":"DEED"}]},"column":["object_id","address","sale_price","sale_terms","verified_by"]},"3":{"predicate":{"sale_price":[{"op":">","val":50000}]},"column":["object_id","address","verified_by"]},"4":{"predicate":{"sale_price":[{"op":">","val":50000}]},"column":["object_id","sale_price","sale_terms"]}}`), &i)
	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "sales"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "object_id", DataType: TypeInt32},
		{Name: "address", DataType: TypeString},
		{Name: "sale_price", DataType: TypeDouble},
		{Name: "sale_terms", DataType: TypeString},
		{Name: "verified_by", DataType: TypeString},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{2,"19158 MALLINA",74687,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{6,"9520 W GRAND RIVER",2520000,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		{7,"2527 JOHN R 25",80000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
		{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		{9,"2446 WOODWARD AVENUE 04/1",68500,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		{10,"44 ADELAIDE ST 49/6",75000,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
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

	end3 := network.MakeEnd("client3")
	network.Connect("client3", "Node3")
	network.Enable("client3", true)

	table3 := Dataset{}
	end3.Call("Node.ScanTable", budgetTableName, &table3)

	end4 := network.MakeEnd("client4")
	network.Connect("client4", "Node4")
	network.Enable("client4", true)

	table4 := Dataset{}
	end4.Call("Node.ScanTable", budgetTableName, &table4)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{1,"7729 GRANDVILLE",12125,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
			{3,"19158 MALLINA",19000,"19-MULTI PARCEL ARM'S LENGTH","TITLE COMPANY"},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{4,"19325 HARTWELL",0,"19-MULTI PARCEL ARM'S LENGTH","PROPERTY TRANSFER AFFIDAVIT"},
			{5,"4625 W FORT",40260,"21-NOT USED","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{8,"111 CHANDLER",100,"21-NOT USED","DEED"},
		},
	}

	expectedDataset3 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "address", DataType: TypeString},
				{Name: "verified_by", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{2,"19158 MALLINA","PROPERTY TRANSFER AFFIDAVIT"},
			{6,"9520 W GRAND RIVER","PROPERTY TRANSFER AFFIDAVIT"},
			{7,"2527 JOHN R 25","PROPERTY TRANSFER AFFIDAVIT"},
			{9,"2446 WOODWARD AVENUE 04/1","TITLE COMPANY"},
			{10,"44 ADELAIDE ST 49/6","PROPERTY TRANSFER AFFIDAVIT"},
		},
	}

	expectedDataset4 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "sale_terms", DataType: TypeString},
			},
		},
		Rows:   []Row{
			{2,74687,"19-MULTI PARCEL ARM'S LENGTH"},
			{6,2520000,"21-NOT USED"},
			{7,80000,"19-MULTI PARCEL ARM'S LENGTH"},
			{9,68500,"19-MULTI PARCEL ARM'S LENGTH"},
			{10,75000,"19-MULTI PARCEL ARM'S LENGTH"},
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
	if !compareDataset(expectedDataset3, table3) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset3, table3)
	}
	if !compareDataset(expectedDataset4, table4) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset4, table4)
	}
}
