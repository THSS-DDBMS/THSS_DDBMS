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
	c := NewCluster(6, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{"0": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": "<", "val": 80000}]},"column": ["object_id", "address", "sale_price"]},"1": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": "<", "val": 80000}]},"column": ["object_id", "sale_terms", "verified_by"]},"2": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": ">=", "val": 80000},{"op": "<=", "val": 200000}]},"column": ["object_id", "address", "sale_price", "sale_terms", "verified_by"]},"3": {"predicate": {"sale_terms":[{"op": "==", "val": "19-MULTI PARCEL ARM'S LENGTH"}],"sale_price":[{"op": ">", "val": 200000}]},"column": ["object_id", "address", "sale_price", "sale_terms", "verified_by"]},"4": {"predicate": {"sale_terms":[{"op": "!=", "val": "19-MULTI PARCEL ARM'S LENGTH"}],},"column": ["object_id", "address", "sale_price"]},"5": {"predicate": {"sale_terms":[{"op": "!=", "val": "19-MULTI PARCEL ARM'S LENGTH"}],},"column": ["object_id", "sale_terms", "verified_by"]}}`), &i)
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
		Rows:  []Row{
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
