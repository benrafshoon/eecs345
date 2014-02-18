package kademlia

// Contains definitions mirroring the Kademlia spec. You will need to stick
// strictly to these to be compatible with the reference implementation and
// other groups' code.

import (
	//"fmt"
	"log"
	"net"
	"strconv"
)

// Host identification.
type Contact struct {
	NodeID ID
	Host   net.IP
	Port   uint16
}

func (contact *Contact) ToFoundNode() FoundNode {
	return FoundNode{contact.Host.String(), contact.Port, contact.NodeID}
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

func NewContactFromAddressString(addressString string) (*Contact, error) {
	contact := new(Contact)
	hostStr, port, error := net.SplitHostPort(addressString)
	if error != nil {
		return nil, error
	}
	ipAddrStrings, error := net.LookupHost(hostStr)
	var host net.IP
	for i := 0; i < len(ipAddrStrings); i++ {
		host = net.ParseIP(ipAddrStrings[i])
		if host.To4() != nil {
			break
		}
	}
	contact.Host = host

	portuint64, error := strconv.ParseInt(port, 10, 16)
	if error != nil {
		return nil, error
	}
	contact.Port = uint16(portuint64)
	return contact, nil

}

// PING
type Ping struct {
	Sender Contact
	MsgID  ID
}

type Pong struct {
	MsgID  ID
	Sender Contact
}

func (k *Kademlia) Ping(ping Ping, pong *Pong) error {
	log.Printf("Received ping from %v:%v\n", ping.Sender.Host, ping.Sender.Port)
	log.Printf("          Node ID: %v\n", ping.Sender.NodeID.AsString())
	log.Printf("       Message ID: %v\n", ping.MsgID.AsString())
	log.Printf("Sending pong back\n")

	pong.MsgID = CopyID(ping.MsgID)
	pong.Sender = *k.RoutingTable.SelfContact

	k.markAliveAndPossiblyPing(&ping.Sender)
	return nil
}

// STORE
type StoreRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
	Value  []byte
}

type StoreResult struct {
	MsgID ID
	Err   error
}

func (k *Kademlia) Store(req StoreRequest, res *StoreResult) error {
	k.Data.InsertValue(req.Key, req.Value)
	log.Printf("Received store from %v:%v\n", req.Sender.Host, req.Sender.Port)
	log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())
	log.Printf("       Message ID: %v\n", req.MsgID.AsString())
	log.Printf("              Key: %v\n", req.Key.AsString())
	log.Printf("            Value: %v\n", string(req.Value))

	k.Data.InsertValue(req.Key, req.Value)

	res.MsgID = req.MsgID
	res.Err = nil

	k.markAliveAndPossiblyPing(&req.Sender)
	return nil
}

// FIND_NODE
type FindNodeRequest struct {
	Sender Contact
	MsgID  ID
	NodeID ID
}

type FoundNode struct {
	IPAddr string
	Port   uint16
	NodeID ID
}

func (f FoundNode) ToContact() *Contact {
	contact := new(Contact)
	contact.NodeID = f.NodeID
	contact.Host = net.ParseIP(f.IPAddr)
	contact.Port = f.Port
	return contact
}

type FindNodeResult struct {
	MsgID ID
	Nodes []FoundNode
	Err   error
}

func (k *Kademlia) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	log.Printf("Received Find Node from %v:%v\n", req.Sender.Host, req.Sender.Port)
	log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())
	log.Printf("  Node ID To Find: %v\n", req.NodeID.AsString())

	contacts := k.RoutingTable.FindKClosestNodes(const_k, req.NodeID, &req.Sender)
	log.Printf("  Responding with %v nodes\n", len(contacts))
	for i := 0; i < len(contacts); i++ {
		log.Printf("  %v: %v\n", i, contacts[i].NodeID.AsString())
	}

	res.MsgID = req.MsgID
	res.Nodes = make([]FoundNode, len(contacts), len(contacts))

	for i := 0; i < len(res.Nodes); i++ {
		res.Nodes[i] = contacts[i].ToFoundNode()
	}

	res.Err = nil

	k.markAliveAndPossiblyPing(&req.Sender)

	return nil
}

// FIND_VALUE
type FindValueRequest struct {
	Sender Contact
	MsgID  ID
	Key    ID
}

// If Value is nil, it should be ignored, and Nodes means the same as in a
// FindNodeResult.
type FindValueResult struct {
	MsgID ID
	Value []byte
	Nodes []FoundNode
	Err   error
}

func (k *Kademlia) FindValue(req FindValueRequest, res *FindValueResult) error {
	log.Printf("Received Find Value from %v:%v\n", req.Sender.Host, req.Sender.Port)
	log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())
	log.Printf("      Key To Find: %v\n", req.Key.AsString())

	res.MsgID = req.MsgID
	res.Value = k.Data.RetrieveValue(req.Key)
	if res.Value == nil {
		log.Printf("   Could not find value with that key, returning k closest nodes instead\n")
		contacts := k.RoutingTable.FindKClosestNodes(const_k, req.Key, &req.Sender)

		log.Printf("  Responding with %v nodes\n", len(contacts))
		for i := 0; i < len(contacts); i++ {
			log.Printf("  %v: %v\n", i, contacts[i].NodeID.AsString())
		}

		res.Nodes = make([]FoundNode, len(contacts), len(contacts))
		for i := 0; i < len(res.Nodes); i++ {
			res.Nodes[i] = contacts[i].ToFoundNode()
		}
	} else {
		log.Printf("     Found value: %v\n", string(res.Value))
	}

	k.markAliveAndPossiblyPing(&req.Sender)

	return nil
}
