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
    kBuckets []*Bucket
    Data *KeyValueStore
}


func (k *Kademlia) getSelfContact() *Contact {
	return k.selfContact
}

func (k *Kademlia) updateContact(contact Contact) {
    bucketIndex := k.selfContact.NodeID.DistanceBucket(contact.NodeID)
    if bucketIndex != -1 {
        log.Printf("Adding to k-bucket index %v\n", bucketIndex)
        k.kBuckets[bucketIndex].PingBucket(contact)
    } else {
        //Case where received a message from self, so don't add to kbuckets
        log.Printf("Attempting to add self to k-bucket\n")
    }
}


//KademliaServer contains methods that are accessible by the client program
type KademliaServer struct {
	Kademlia
}

func NewKademliaServer() *KademliaServer {
	kademliaServer := new(KademliaServer)
    kademliaServer.selfContact = new(Contact)
    kademliaServer.selfContact.NodeID = NewRandomID()

    kademliaServer.kBuckets = make([]*Bucket, bucketSize, bucketSize)
    for i := 0; i < bucketSize; i++ {
        kademliaServer.kBuckets[i] = NewBucket()
    }
    kademliaServer.Data = NewKeyValueStore()
	return kademliaServer
}

func NewTestKademliaServer(nodeID ID) *KademliaServer {
    kademliaServer := NewKademliaServer()
    kademliaServer.selfContact.NodeID = nodeID
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
	kademliaServer.selfContact.Host = hostIP

    portInt, error := strconv.ParseUint(port, 10, 16)
    if error != nil {
    	return error
    }
    kademliaServer.selfContact.Port = uint16(portInt)

    

    // Serve forever.
    go http.Serve(listener, nil)

    log.Printf("Starting kademlia server listening on %v:%v\n", hostIP, portInt)
    log.Printf("Self NodeID: %v", kademliaServer.GetNodeID().AsString())
    return nil
}


func (kademliaServer *KademliaServer) GetNodeID() ID {
	return kademliaServer.selfContact.NodeID
}

func (kademliaServer *KademliaServer) SendPing(address string) error {
	
	client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        return err
    }
    log.Printf("Sending ping to %v\n", address)

    ping := new(Ping)
    ping.Sender = *kademliaServer.getSelfContact()
    ping.MsgID = NewRandomID()
    var pong Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        return err
    }
    if ping.MsgID.Equals(pong.MsgID) {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
    	log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
        kademliaServer.updateContact(pong.Sender)
        
    } else {
    	log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
		log.Printf("Incorrect MsgID\n");
		log.Printf("  Ping Message ID: %v\n", ping.MsgID.AsString())
		log.Printf("  Pong Message ID: %v\n", pong.MsgID.AsString())

    }

    //Not sure if we should close the connection
    client.Close()
    return nil
}

/*
func (kademliaServer *KademliaServer) FindNode(NodeID ID, key int) {
    distance := NodeID.Distance(kademliaServer.GetNodeID())
    log.Printf("The distance is: %v", distance)
}*/

/*
func (kademliaServer *KademliaServer) Store(NodeID ID, key ID, value []byte) {

}*/
