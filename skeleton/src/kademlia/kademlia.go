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
    selfContact *Contact
    NodeID ID
    buckets [bucketSize]Bucket
}

func NewKademlia() *Kademlia {
    // TODO: Assign yourself a random ID and prepare other state here.
    newNode := new(Kademlia)
    newNode.selfContact = new(Contact)
    newNode.selfContact.NodeID = NewRandomID()
    return newNode
}

func (k *Kademlia) getSelfContact() *Contact {
	return k.selfContact
}


//KademliaServer contains methods that are accessible by the client program
type KademliaServer struct {
	kademlia *Kademlia
}

func NewKademliaServer() *KademliaServer {
	kademliaServer := new(KademliaServer)
	kademliaServer.kademlia = NewKademlia()
	return kademliaServer
}

func (kademliaServer *KademliaServer) StartKademliaServer(address string) error {
	rpc.Register(kademliaServer.kademlia)
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
	kademliaServer.kademlia.selfContact.Host = hostIP

    portInt, error := strconv.ParseUint(port, 10, 16)
    if error != nil {
    	return error
    }
    kademliaServer.kademlia.selfContact.Port = uint16(portInt)

    

    // Serve forever.
    go http.Serve(listener, nil)

    log.Printf("Starting kademlia server listening on %v:%v\n", hostIP, portInt)
    return nil
}


func (kademliaServer *KademliaServer) GetNodeID() ID {
	return kademliaServer.kademlia.selfContact.NodeID
}

func (kademliaServer *KademliaServer) Ping(address string) {
	
	client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        log.Fatal("DialHTTP: ", err)
    }
    log.Printf("Sending ping to %v\n", address)

    ping := new(Ping)
    ping.Sender = *kademliaServer.kademlia.getSelfContact()
    ping.MsgID = NewRandomID()
    var pong Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        log.Fatal("Call: ", err)
    }
    if ping.MsgID.Equals(pong.MsgID) {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
    	log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
    } else {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
		log.Printf("Incorrect MsgID\n");
		log.Printf("  Ping Message ID: %v\n", ping.MsgID.AsString())
		log.Printf("  Pong Message ID: %v\n", pong.MsgID.AsString())

    }

    //Not sure if we should close the connection
    client.Close()
}

func AddNode()
