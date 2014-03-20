package kademlia

// Contains the core kademlia type. In addition to core state, this type serves
// as a receiver for the RPC methods, which is required by that package.

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const const_alpha = 3
const const_B = 160
const const_k = 20
const timeout = 300 * time.Millisecond
const primitiveTimeout = 10 * time.Second

type Kademlia struct {
	RoutingTable *KBucketTable
	Data         *KeyValueStore
	Groups       map[string]*Group
}

func NewKademlia() *Kademlia {
	kademlia := new(Kademlia)
	kademlia.RoutingTable = NewKBucketTable()
	kademlia.RoutingTable.SelfContact.NodeID = NewRandomID()
	kademlia.Data = NewKeyValueStore()
	kademlia.Groups = make(map[string]*Group)
	return kademlia
}

func NewTestKademlia(nodeID ID) *Kademlia {
	kademlia := NewKademlia()
	kademlia.RoutingTable.SelfContact.NodeID = nodeID
	return kademlia
}

func (kademlia *Kademlia) InitializeRoutingTable(firstNode *Contact) error {
	log.Printf("Initializing routing table")
	log.Printf("Pinging first contact to get its NodeID")
	firstNode, error := kademlia.SendPing(firstNode)
	if error != nil {
		return error
	}

	log.Printf("Performing an iterative find node on self")
	kademlia.SendIterativeFindNode(kademlia.RoutingTable.SelfContact.NodeID)

	closestNeighborBucketFound := false

	//Refresh all buckets that aren't the bucket of the closest neighbor
	//aka all buckets except the first occupied
	for i := 0; i < const_B; i++ {
		if closestNeighborBucketFound {
			kademlia.refreshBucket(i)
		} else {
			if !kademlia.RoutingTable.kBuckets[i].IsEmpty() {
				closestNeighborBucketFound = true
			}
		}
	}
	return nil
}

func (kademlia *Kademlia) refreshBucket(bucket int) {
	randomID := kademlia.RoutingTable.SelfContact.NodeID.RandomIDInBucket(bucket)
	log.Printf("Refreshing bucket %v using random ID in bucket: %v", bucket, randomID.AsString())
	kademlia.SendIterativeFindNode(randomID) //All results will be marked alive in the routing table
}

func (kademlia *Kademlia) StartKademliaServer(address string) error {
	error := rpc.RegisterName("Kademlia", NewKademliaRPCWrapper(kademlia))
	if error != nil {
		return error
	}
	rpc.HandleHTTP()

	listener, error := net.Listen("tcp", address)
	if error != nil {
		return error
	}

	//Get our IPv4 address
	hostname, error := os.Hostname()
	if error != nil {
		return error
	}
	ipAddrStrings, error := net.LookupHost(hostname)
	var host net.IP
	for i := 0; i < len(ipAddrStrings); i++ {
		host = net.ParseIP(ipAddrStrings[i])
		if host.To4() != nil {
			break
		}
	}
	kademlia.RoutingTable.SelfContact.Host = host

	//Get our port number
	_, port, error := net.SplitHostPort(listener.Addr().String())
	if error != nil {
		return error
	}
	portInt, error := strconv.ParseUint(port, 10, 16)
	if error != nil {
		return error
	}
	kademlia.RoutingTable.SelfContact.Port = uint16(portInt)

	// Serve forever.
	go http.Serve(listener, nil)

	log.Printf("Starting kademlia server listening on %v:%v\n", host, portInt)
	log.Printf("Self NodeID: %v", kademlia.RoutingTable.SelfContact.NodeID.AsString())
	return nil
}

func (kademlia *Kademlia) GetNodeID() ID {
	return kademlia.RoutingTable.SelfContact.NodeID
}

//If a k-bucket is full, mark alive will return a node that needs to be pinged
//This is the head of the k-bucket, and if it turns out it is dead, the node
//we are adding will replace the dead node in the k-bucket
func (kademlia *Kademlia) markAliveAndPossiblyPing(contact *Contact) {
	needToPing, contactToPing := kademlia.RoutingTable.MarkAlive(contact)
	if needToPing {
		go kademlia.SendPing(contactToPing)
	}
	//If self is the current rendezvous point and become aware of a closer node, transfer RV responsibility to newly discovered node
	for _, group := range kademlia.Groups {
		log.Printf("Checking group %s", group.GroupID.AsString())
		log.Printf("New node distance %f", group.GroupID.DistanceBucketUnique(contact.NodeID))
		log.Printf("Self distance %f", group.GroupID.DistanceBucketUnique(kademlia.RoutingTable.SelfContact.NodeID))
		if group.IsRendezvousPoint && group.GroupID.DistanceBucketUnique(contact.NodeID) < group.GroupID.DistanceBucketUnique(kademlia.RoutingTable.SelfContact.NodeID) {
			log.Printf("Need to transfer RV point to node %s", contact.NodeID.AsString())
			group.RendezvousPoint = contact
			group.Parent = contact
			group.IsRendezvousPoint = false
			kademlia.SendCreateGroup(group, kademlia.RoutingTable.SelfContact)
		}
	}
}

type sendPingResult struct {
	contact *Contact
	err     error
}

//The contact to send a ping to is not required to have a NodeID
//The NodeID is returned if the contact responded
func (kademlia *Kademlia) SendPing(contact *Contact) (*Contact, error) {

	resultChannel := make(chan sendPingResult)

	go func() {
		log.Printf("Sending ping to %v\n", kademlia.GetContactAddress(contact))
		client, err := rpc.DialHTTP("tcp", kademlia.GetContactAddress(contact))
		if err != nil {
			log.Printf("Connection error, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendPingResult{nil, err}
			return
		}

		ping := new(Ping)
		ping.Sender = *kademlia.RoutingTable.SelfContact
		ping.MsgID = NewRandomID()
		var pong Pong

		err = client.Call("Kademlia.Ping", ping, &pong)
		if err != nil {
			log.Printf("Error in remote node response, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendPingResult{nil, err}
			return
		}

		//log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		//log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())

		if ping.MsgID.Equals(pong.MsgID) {
			kademlia.markAliveAndPossiblyPing(&pong.Sender)
		} else {
			log.Printf("Incorrect MsgID\n")
			log.Printf("  Ping Message ID: %v\n", ping.MsgID.AsString())
			log.Printf("  Pong Message ID: %v\n", pong.MsgID.AsString())
			kademlia.RoutingTable.MarkDead(contact)
		}

		client.Close()
		resultChannel <- sendPingResult{&pong.Sender, nil}
		return
	}()

	select {
	case result := <-resultChannel:
		return result.contact, result.err
	case <-time.NewTimer(primitiveTimeout).C:
		return nil, errors.New("Timed out")
	}
}

func (kademlia *Kademlia) SendStore(contact *Contact, key ID, value []byte) error {

	resultChannel := make(chan error)

	go func() {
		log.Printf("Sending store to %v\n", kademlia.GetContactAddress(contact))
		client, err := rpc.DialHTTP("tcp", kademlia.GetContactAddress(contact))
		if err != nil {
			log.Printf("Connection error, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- err
			return
		}

		storeRequest := new(StoreRequest)
		storeRequest.Sender = *kademlia.RoutingTable.SelfContact
		storeRequest.MsgID = NewRandomID()
		storeRequest.Key = key
		storeRequest.Value = value

		storeResult := new(StoreResult)
		err = client.Call("Kademlia.Store", storeRequest, storeResult)
		if err != nil {
			log.Printf("Error in remote node response, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- err
			return
		}

		log.Printf("Received response to SendStore from %v:%v\n", storeRequest.Sender.Host, storeRequest.Sender.Port)
		log.Printf("                           Node ID: %v\n", storeRequest.Sender.NodeID.AsString())

		if storeRequest.MsgID.Equals(storeResult.MsgID) {
			kademlia.markAliveAndPossiblyPing(contact)
		} else {
			log.Printf("Incorrect MsgID\n")
			log.Printf("      Request Message ID: %v\n", storeRequest.MsgID.AsString())
			log.Printf("       Result Message ID: %v\n", storeResult.MsgID.AsString())
			kademlia.RoutingTable.MarkDead(contact)
		}
		client.Close()
		resultChannel <- nil
		return
	}()

	select {
	case result := <-resultChannel:
		return result
	case <-time.NewTimer(primitiveTimeout).C:
		return errors.New("Timed out")
	}

}

func (kademlia *Kademlia) SendFindNode(contact *Contact, nodeToFind ID) (error, []*Contact) {

	resultChannel := make(chan sendFindNodeResult)

	go func() {
		log.Printf("Sending FindNode to %v\n", kademlia.GetContactAddress(contact))
		client, err := rpc.DialHTTP("tcp", kademlia.GetContactAddress(contact))

		if err != nil {
			log.Printf("Connection error, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendFindNodeResult{err, nil}
			return
		}
		findNodeRequest := new(FindNodeRequest)
		findNodeRequest.Sender = *kademlia.RoutingTable.SelfContact
		findNodeRequest.MsgID = NewRandomID()
		findNodeRequest.NodeID = nodeToFind

		findNodeResult := new(FindNodeResult)
		err = client.Call("Kademlia.FindNode", findNodeRequest, findNodeResult)
		if err != nil {
			log.Printf("Error in remote node response, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendFindNodeResult{err, nil}
			return
		}
		log.Printf("Received response FindNode from %v:%v\n", contact.Host, contact.Port)
		log.Printf("                           Node ID: %v\n", contact.NodeID.AsString())

		var contacts []*Contact = nil

		if findNodeRequest.MsgID.Equals(findNodeResult.MsgID) {
			kademlia.markAliveAndPossiblyPing(contact)
			contacts = make([]*Contact, len(findNodeResult.Nodes), len(findNodeResult.Nodes))

			for i := 0; i < len(findNodeResult.Nodes); i++ {
				log.Printf("    Found node %v in response: %v", i, findNodeResult.Nodes[i].NodeID.AsString())
				contacts[i] = findNodeResult.Nodes[i].ToContact()
			}

		} else {
			log.Printf("Incorrect MsgID\n")
			log.Printf("      Request Message ID: %v\n", findNodeRequest.MsgID.AsString())
			log.Printf("       Result Message ID: %v\n", findNodeResult.MsgID.AsString())
			kademlia.RoutingTable.MarkDead(contact)
			err = errors.New("Incorrect MsgID")
		}
		client.Close()
		resultChannel <- sendFindNodeResult{err, contacts}
		return
	}()

	select {
	case result := <-resultChannel:
		return result.err, result.contacts
	case <-time.NewTimer(primitiveTimeout).C:
		return errors.New("Timed out"), nil
	}

}

type sendFindValueResult struct {
	err      error
	value    []byte
	contacts []*Contact
}

func (kademlia *Kademlia) SendFindValue(contact *Contact, key ID) (error, []byte, []*Contact) {

	resultChannel := make(chan sendFindValueResult)

	go func() {
		log.Printf("Sending FindValue to %v\n", kademlia.GetContactAddress(contact))
		log.Printf("         Key to find: %v\n", key.AsString())
		client, err := rpc.DialHTTP("tcp", kademlia.GetContactAddress(contact))
		if err != nil {
			log.Printf("Connection error, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendFindValueResult{err, nil, nil}
			return
		}

		findValueRequest := new(FindValueRequest)
		findValueRequest.Sender = *kademlia.RoutingTable.SelfContact
		findValueRequest.MsgID = NewRandomID()
		findValueRequest.Key = key

		findValueResult := new(FindValueResult)
		err = client.Call("Kademlia.FindValue", findValueRequest, findValueResult)
		if err != nil {
			log.Printf("Error in remote node response, marking node dead")
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendFindValueResult{err, nil, nil}
			return
		}

		log.Printf("Received response FindValue from %v:%v\n", contact.Host, contact.Port)
		log.Printf("                           Node ID: %v\n", contact.NodeID.AsString())
		if findValueRequest.MsgID.Equals(findValueResult.MsgID) {
			kademlia.markAliveAndPossiblyPing(contact)
			if findValueResult.Value != nil {
				log.Printf("     Found value %v", string(findValueResult.Value))
				resultChannel <- sendFindValueResult{nil, findValueResult.Value, nil}
				return
			} else {
				contacts := make([]*Contact, len(findValueResult.Nodes), len(findValueResult.Nodes))
				for i := 0; i < len(findValueResult.Nodes); i++ {
					log.Printf("    Found node %v in response: %v", i, findValueResult.Nodes[i].NodeID.AsString())
					contacts[i] = findValueResult.Nodes[i].ToContact()
				}
				resultChannel <- sendFindValueResult{nil, nil, contacts}
				return
			}

		} else {
			log.Printf("Incorrect MsgID\n")
			log.Printf("      Request Message ID: %v\n", findValueRequest.MsgID.AsString())
			log.Printf("       Result Message ID: %v\n", findValueResult.MsgID.AsString())
			kademlia.RoutingTable.MarkDead(contact)
			resultChannel <- sendFindValueResult{errors.New("Incorrect MsgID"), nil, nil}
			return
		}
		client.Close()
		resultChannel <- sendFindValueResult{nil, nil, nil}
		return
	}()

	select {
	case result := <-resultChannel:
		return result.err, result.value, result.contacts
	case <-time.NewTimer(primitiveTimeout).C:
		return errors.New("Timed out"), nil, nil
	}

}

//net.Dial doesn't seem to like when you put the local machine's external ip address
//so use 'localhost' instead
func (kademlia *Kademlia) GetContactAddress(contact *Contact) string {
	if contact.Host.Equal(kademlia.RoutingTable.SelfContact.Host) {
		return fmt.Sprintf("%v:%v", "localhost", contact.Port)
	} else {
		return fmt.Sprintf("%v:%v", contact.Host.String(), contact.Port)
	}
}

func (kademlia *Kademlia) PrintRoutingTable() {
	for i := 159; i >= 0; i-- {
		if !kademlia.RoutingTable.kBuckets[i].IsEmpty() {
			log.Printf("Bucket %v", i)
			element := kademlia.RoutingTable.kBuckets[i].list.Front()
			j := 1
			for element != nil {
				contact := element.Value.(*Contact)
				log.Printf("  %v: %v", j, contact.NodeID.AsString())
				j++
				element = element.Next()
			}
		}
	}
}
