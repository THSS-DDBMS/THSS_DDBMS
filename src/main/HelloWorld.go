package main

import (
	"../labrpc"
	"../models"
	"fmt"
)

// main is an example about how to create a cluster, visit the cluster from outside it, and inject some errors to the
// cluster. We will test your implementation using similar approaches.
func main() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := models.NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// send a request to the cluster
	fmt.Println("Sending a greet to the cluster...")
	reply := ""
	cli.Call("Cluster.SayHello", clientName, &reply)
	fmt.Println("The coordinator returns a reply:")
	fmt.Println(reply)

	// disable some nodes and resend the request
	fmt.Println()
	fmt.Println("Sending a greet with two nodes disabled")
	network.DeleteServer("Node1")
	network.DeleteServer("Node2")
	reply = ""
	cli.Call("Cluster.SayHello", clientName, &reply)
	fmt.Println("The coordinator returns a reply:")
	fmt.Println(reply)
}

