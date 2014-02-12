package kademlia
// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import (
    "net"
    "log"
    "fmt"
)


// Host identification.
type Contact struct {
    NodeID ID
    Host net.IP
    Port uint16
}

func (contact *Contact) GetAddress() string {
    return fmt.Sprintf("%v:%v", contact.Host.String(), contact.Port)
}

func (contact *Contact) Equals(contactToCheck *Contact) bool {
    return contact.NodeID.Equals(contactToCheck.NodeID)
}

func NewContact(nodeID ID, host net.IP, port uint16) *Contact {
    contact := new(Contact)
    contact.NodeID = nodeID
    contact.Host = host
    contact.Port = port
    return contact
}

// PING
type Ping struct {
    Sender Contact
    MsgID ID
}

type Pong struct {
    MsgID ID
    Sender Contact
}

func (k *Kademlia) Ping(ping Ping, pong *Pong) error {
    log.Printf("Received ping from %v:%v\n", ping.Sender.Host, ping.Sender.Port)
    log.Printf("          Node ID: %v\n", ping.Sender.NodeID.AsString())
    log.Printf("       Message ID: %v\n", ping.MsgID.AsString())
    log.Printf("Sending pong back\n")
    
    

    pong.MsgID = CopyID(ping.MsgID)
    pong.Sender = *k.RoutingTable.SelfContact

    k.RoutingTable.MarkAlive(&ping.Sender)
    return nil
}


// STORE
type StoreRequest struct {
    Sender Contact
    MsgID ID
    Key ID
    Value []byte
}

type StoreResult struct {
    MsgID ID
    Err error
}

func (k *Kademlia) Store(req StoreRequest, res *StoreResult) error {
    k.Data.InsertValue(req.Key, req.Value)
    log.Printf("Received store from %v:%v\n", req.Sender.Host, req.Sender.Port)
    log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())
    log.Printf("       Message ID: %v\n", req.MsgID.AsString())
    log.Printf("              Key: %v\n", req.Key.AsString())
    log.Printf("            Value: %v\n", string(req.Value))
    log.Printf("Sending pong back\n")

    k.Data.InsertValue(req.Key, req.Value)

    res.MsgID = req.MsgID
    res.Err = nil

    k.RoutingTable.MarkAlive(&req.Sender)
    return nil
}


// FIND_NODE
type FindNodeRequest struct {
    Sender Contact
    MsgID ID
    NodeID ID
}

type FoundNode struct {
    IPAddr string
    Port uint16
    NodeID ID
}

type FindNodeResult struct {
    MsgID ID
    Nodes []FoundNode
    Err error
}

func (k *Kademlia) FindNode(req FindNodeRequest, res *FindNodeResult) error {
    // TODO: Implement.
    return nil
}


// FIND_VALUE
type FindValueRequest struct {
    Sender Contact
    MsgID ID
    Key ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
    MsgID ID
    Value []byte
    Nodes []FoundNode
    Err error
}

func (k *Kademlia) FindValue(req FindValueRequest, res *FindValueResult) error {
    // TODO: Implement.
    return nil
}

