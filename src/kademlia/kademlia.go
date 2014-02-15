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


const const_alpha = 3
const const_B = 160
const const_k = 20

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

func (kademliaServer *KademliaServer) markAliveAndPossiblyPing(contact *Contact) {
    needToPing, contactToPing := kademliaServer.RoutingTable.MarkAlive(contact)
    if needToPing {
        go kademliaServer.SendPing(contactToPing)
    }
}

//The contact to send a ping to is not required to have a NodeID
func (kademliaServer *KademliaServer) SendPing(contact *Contact) error {
	client, err := rpc.DialHTTP("tcp", contact.GetAddress())
    if err != nil {
        log.Printf("Connection error, marking node dead")
        kademliaServer.RoutingTable.MarkDead(contact)
        return err
    }

    log.Printf("Sending ping to %v\n", contact.GetAddress())

    ping := new(Ping)
    ping.Sender = *kademliaServer.RoutingTable.SelfContact
    ping.MsgID = NewRandomID()
    var pong Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        log.Printf("Error in remote node response, marking node dead")
        kademliaServer.RoutingTable.MarkDead(contact)
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
        
        kademliaServer.RoutingTable.MarkDead(contact)
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

func (kademliaServer *KademliaServer) SendFindNode(address string, nodeToFind ID) (error, []*Contact) {
    client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        return err, nil
    }
    log.Printf("Sending find node to %v\n", address)
    findNodeRequest := new(FindNodeRequest)
    findNodeRequest.Sender = *kademliaServer.RoutingTable.SelfContact
    findNodeRequest.MsgID = NewRandomID()
    findNodeRequest.NodeID = nodeToFind

    findNodeResult := new(FindNodeResult)
    err = client.Call("Kademlia.FindNode", findNodeRequest, findNodeResult)
    if err != nil {
        return err, nil
    }

    log.Printf("Received response\n")

    if findNodeRequest.MsgID.Equals(findNodeResult.MsgID) {
        kademliaServer.RoutingTable.MarkAlive(&findNodeRequest.Sender)
        contacts := make([]*Contact, len(findNodeResult.Nodes), len(findNodeResult.Nodes))

        for i := 0; i < len(findNodeResult.Nodes); i++ {
            contacts[i] = findNodeResult.Nodes[i].ToContact()
        }
        return nil, contacts

    } else {
        log.Printf("Incorrect MsgID\n");
        log.Printf("      Request Message ID: %v\n", findNodeRequest.MsgID.AsString())
        log.Printf("       Result Message ID: %v\n", findNodeResult.MsgID.AsString())
        return errors.New("Incorrect MsgID"), nil
    }
    client.Close()
    return nil, nil
}

func (kademliaServer *KademliaServer) SendFindValue(address string, key ID) (error, []byte, []*Contact) {
    client, err := rpc.DialHTTP("tcp", address)
    if err != nil {
        return err, nil, nil
    }
    log.Printf("Sending find value to %v\n", address)
    log.Printf("         Key to find: %v\n", key.AsString())
    findValueRequest := new(FindValueRequest)
    findValueRequest.Sender = *kademliaServer.RoutingTable.SelfContact
    findValueRequest.MsgID = NewRandomID()
    findValueRequest.Key = key

    findValueResult := new(FindValueResult)
    err = client.Call("Kademlia.FindValue", findValueRequest, findValueResult)
    if err != nil {
        return err, nil, nil
    }

    log.Printf("Received response\n")
    if findValueRequest.MsgID.Equals(findValueResult.MsgID) {
        kademliaServer.RoutingTable.MarkAlive(&findValueRequest.Sender)
        if findValueResult.Value != nil {
            return nil, findValueResult.Value, nil
        } else {
            contacts := make([]*Contact, len(findValueResult.Nodes), len(findValueResult.Nodes))
            for i := 0; i < len(findValueResult.Nodes); i++ {
                contacts[i] = findValueResult.Nodes[i].ToContact()
            }
            return nil, nil, contacts
        }

    } else {
        log.Printf("Incorrect MsgID\n");
        log.Printf("      Request Message ID: %v\n", findValueRequest.MsgID.AsString())
        log.Printf("       Result Message ID: %v\n", findValueResult.MsgID.AsString())
        return errors.New("Incorrect MsgID"), nil, nil
    }
    client.Close()
    return nil, nil, nil
}
