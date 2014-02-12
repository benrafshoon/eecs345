package kademlia
// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

import (
	"net"
	"net/http"
	"net/rpc"
	"log"
	"strconv"
	"errors"
)

//Kademlia contains methods that are remotely accessable via rpc
// Core Kademlia type. You can put whatever state you want in this.
type Kademlia struct {
    //selfContact *Contact
    //kBuckets []*Bucket
    RoutingTable *KBucketTable
    Data *KeyValueStore
}

//KademliaServer contains methods that are accessible by the client program
type KademliaServer struct {
	Kademlia
}

func NewKademliaServer() *KademliaServer {
	kademliaServer := new(KademliaServer)
    kademliaServer.RoutingTable = NewKBucketTable()
    kademliaServer.RoutingTable.SelfContact.NodeID = NewRandomID()

    /*kademliaServer.kBuckets = make([]*Bucket, bucketSize, bucketSize)
    for i := 0; i < bucketSize; i++ {
        kademliaServer.kBuckets[i] = NewBucket()
    }*/
    
    kademliaServer.Data = NewKeyValueStore()
	return kademliaServer
}

func NewTestKademliaServer(nodeID ID) *KademliaServer {
    kademliaServer := NewKademliaServer()
    kademliaServer.RoutingTable.SelfContact.NodeID = nodeID
    return kademliaServer
}

func (kademliaServer *KademliaServer) StartKademliaServer(address string) error {
	error := rpc.Register(&kademliaServer.Kademlia)
    if error != nil {
        return error
    }
    rpc.HandleHTTP()

    listener, error := net.Listen("tcp", address)
    if error != nil {
        return error
    }

    host, port, error := net.SplitHostPort(listener.Addr().String())
    if error != nil {
    	return error
    }

    hostIP := net.ParseIP(host)
    if hostIP == nil {
    	return errors.New("Invalid host")
    }
	kademliaServer.RoutingTable.SelfContact.Host = hostIP

    portInt, error := strconv.ParseUint(port, 10, 16)
    if error != nil {
    	return error
    }
    kademliaServer.RoutingTable.SelfContact.Port = uint16(portInt)

    

    // Serve forever.
    go http.Serve(listener, nil)

    log.Printf("Starting kademlia server listening on %v:%v\n", hostIP, portInt)
    log.Printf("Self NodeID: %v", kademliaServer.RoutingTable.SelfContact.NodeID.AsString())
    return nil
}


func (kademliaServer *KademliaServer) GetNodeID() ID {
	return kademliaServer.RoutingTable.SelfContact.NodeID
}

func (kademliaServer *KademliaServer) SendPing(address string) error {
	
	client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        return err
    }
    log.Printf("Sending ping to %v\n", address)

    ping := new(Ping)
    ping.Sender = *kademliaServer.RoutingTable.SelfContact
    ping.MsgID = NewRandomID()
    var pong Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        return err
    }
    if ping.MsgID.Equals(pong.MsgID) {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
    	log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
        kademliaServer.RoutingTable.MarkAlive(&pong.Sender)
        
    } else {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
		log.Printf("Incorrect MsgID\n");
		log.Printf("  Ping Message ID: %v\n", ping.MsgID.AsString())
		log.Printf("  Pong Message ID: %v\n", pong.MsgID.AsString())
        //Probably should mark dead
    }

    //Not sure if we should close the connection
    client.Close()
    return nil
}

func (kademliaServer *KademliaServer) SendStore(address string, key ID, value []byte) error {
    client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        return err
    }
    log.Printf("Sending store to %v\n", address)
    storeRequest := new(StoreRequest)
    storeRequest.Sender = *kademliaServer.RoutingTable.SelfContact
    storeRequest.MsgID = NewRandomID()
    storeRequest.Key = key
    storeRequest.Value = value

    storeResult := new(StoreResult)
    err = client.Call("Kademlia.Store", storeRequest, storeResult)
    if err != nil {
        return err
    }

    if storeRequest.MsgID.Equals(storeResult.MsgID) {
        log.Printf("Received response from %v:%v\n", storeRequest.Sender.Host, storeRequest.Sender.Port)
        log.Printf("              Node ID: %v\n", storeRequest.Sender.NodeID.AsString())
        kademliaServer.RoutingTable.MarkAlive(&storeRequest.Sender)
    } else {
        log.Printf("Received response from %v:%v\n", storeRequest.Sender.Host, storeRequest.Sender.Port)
        log.Printf("              Node ID: %v\n", storeRequest.Sender.NodeID.AsString())
        log.Printf("Incorrect MsgID\n");
        log.Printf("      Request Message ID: %v\n", storeRequest.MsgID.AsString())
        log.Printf("       Result Message ID: %v\n", storeResult.MsgID.AsString())
        //Probably should mark dead
    }
    client.Close()
    return nil
}
