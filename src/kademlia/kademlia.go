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
	"strconv"
	"time"
)

const const_alpha = 3
const const_B = 160
const const_k = 20
const timeout = 300 * 1000 //milliseconds

type Kademlia struct {
	RoutingTable *KBucketTable
	Data         *KeyValueStore
}

//
type IterativeContact struct {
	contact *Contact
	checked bool
}

func NewKademlia() *Kademlia {
	kademlia := new(Kademlia)
	kademlia.RoutingTable = NewKBucketTable()
	kademlia.RoutingTable.SelfContact.NodeID = NewRandomID()
	kademlia.Data = NewKeyValueStore()
	return kademlia
}

func NewTestKademlia(nodeID ID) *Kademlia {
	kademlia := NewKademlia()
	kademlia.RoutingTable.SelfContact.NodeID = nodeID
	return kademlia
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

	host, port, error := net.SplitHostPort(listener.Addr().String())
	if error != nil {
		return error
	}

	hostIP := net.ParseIP(host)
	if hostIP == nil {
		return errors.New("Invalid host")
	}
	kademlia.RoutingTable.SelfContact.Host = hostIP

	portInt, error := strconv.ParseUint(port, 10, 16)
	if error != nil {
		return error
	}
	kademlia.RoutingTable.SelfContact.Port = uint16(portInt)

	// Serve forever.
	go http.Serve(listener, nil)

	log.Printf("Starting kademlia server listening on %v:%v\n", hostIP, portInt)
	log.Printf("Self NodeID: %v", kademlia.RoutingTable.SelfContact.NodeID.AsString())
	return nil
}

func (kademlia *Kademlia) GetNodeID() ID {
	return kademlia.RoutingTable.SelfContact.NodeID
}

func (kademlia *Kademlia) markAliveAndPossiblyPing(contact *Contact) {
	needToPing, contactToPing := kademlia.RoutingTable.MarkAlive(contact)
	if needToPing {
		go kademlia.SendPing(contactToPing)
	}
}

//The contact to send a ping to is not required to have a NodeID
func (kademlia *Kademlia) SendPing(contact *Contact) error {
	client, err := rpc.DialHTTP("tcp", contact.GetAddress())
	if err != nil {
		log.Printf("Connection error, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err
	}

	log.Printf("Sending ping to %v\n", contact.GetAddress())

	ping := new(Ping)
	ping.Sender = *kademlia.RoutingTable.SelfContact
	ping.MsgID = NewRandomID()
	var pong Pong
	err = client.Call("Kademlia.Ping", ping, &pong)
	if err != nil {
		log.Printf("Error in remote node response, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err
	}
	if ping.MsgID.Equals(pong.MsgID) {
		log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
		kademlia.RoutingTable.MarkAlive(&pong.Sender)

	} else {
		log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())
		log.Printf("Incorrect MsgID\n")
		log.Printf("  Ping Message ID: %v\n", ping.MsgID.AsString())
		log.Printf("  Pong Message ID: %v\n", pong.MsgID.AsString())

		kademlia.RoutingTable.MarkDead(contact)
	}

	//Not sure if we should close the connection
	client.Close()
	return nil
}

func (kademlia *Kademlia) SendStore(contact *Contact, key ID, value []byte) error {
	client, err := rpc.DialHTTP("tcp", contact.GetAddress())
	if err != nil {
		log.Printf("Connection error, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err
	}
	log.Printf("Sending store to %v\n", contact.GetAddress())
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
		return err
	}

	if storeRequest.MsgID.Equals(storeResult.MsgID) {
		log.Printf("Received response from %v:%v\n", storeRequest.Sender.Host, storeRequest.Sender.Port)
		log.Printf("              Node ID: %v\n", storeRequest.Sender.NodeID.AsString())
		kademlia.markAliveAndPossiblyPing(contact)
	} else {
		log.Printf("Received response from %v:%v\n", storeRequest.Sender.Host, storeRequest.Sender.Port)
		log.Printf("              Node ID: %v\n", storeRequest.Sender.NodeID.AsString())
		log.Printf("Incorrect MsgID\n")
		log.Printf("      Request Message ID: %v\n", storeRequest.MsgID.AsString())
		log.Printf("       Result Message ID: %v\n", storeResult.MsgID.AsString())
		kademlia.RoutingTable.MarkDead(contact)
	}
	client.Close()
	return nil
}

func printShortList(shortList []*IterativeContact, nodeToFind ID, closestIndex int, furthestIndex int) {
	log.Printf("Short list: searching for nodes near %v\n", nodeToFind.AsString())
	for i := 0; i < len(shortList); i++ {
		line := fmt.Sprintf("  %v: ", i)
		if shortList[i].checked {
			line = fmt.Sprintf("%v     Checked ", line)
		} else {
			line = fmt.Sprintf("%v NOT Checked ", line)
		}
		line = fmt.Sprintf("%vD%v ", line, nodeToFind.DistanceBucket(shortList[i].contact.NodeID))
		if i == closestIndex {
			line = fmt.Sprintf("%vC ", line)
		} else {
			line = fmt.Sprintf("%v  ", line)
		}
		if i == furthestIndex {
			line = fmt.Sprintf("%vF ", line)
		} else {
			line = fmt.Sprintf("%v  ", line)
		}
		line = fmt.Sprintf("%v%v\n", line, shortList[i].contact.NodeID.AsString())
		log.Printf("%v", line)
	}
}

func shortListContains(shortList []*IterativeContact, contact *Contact) bool {
	for i := 0; i < len(shortList); i++ {
		if contact.Equals(shortList[i].contact) {
			return true
		}
	}
	return false
}

func (kademlia *Kademlia) SendIterativeFindNode(nodeToFind ID) (error, []*Contact) {
	//This function should return a list of k closest contacts to the specified node
	shortList := make([]*IterativeContact, 0, const_k) //slice - array with 0 things now and a capacity of const_k
	//have to do at least one call to kick it off
	log.Printf("My node id is %v", kademlia.RoutingTable.SelfContact.NodeID.AsString())
	foundContacts := kademlia.RoutingTable.FindKClosestNodes(const_alpha, nodeToFind, kademlia.RoutingTable.SelfContact.NodeID)
	log.Printf("Found initial %v nodes: ", len(foundContacts))

	var closestPosition, farthestPosition int = 0, 0

	for i := 0; i < len(foundContacts); i++ {
		newContact := new(IterativeContact) //convert them to this data struct
		newContact.checked = false
		newContact.contact = foundContacts[i]
		shortList = shortList[0 : i+1]
		/*tempList:= make([]*IterativeContact, i+1, const_k)
		  copy(tempList, shortList)
		  shortList = tempList*/
		shortList[i] = newContact
		distance := foundContacts[i].NodeID.DistanceBucket(nodeToFind)
		closestDistance := shortList[closestPosition].contact.NodeID.DistanceBucket(nodeToFind)
		farthestDistance := shortList[farthestPosition].contact.NodeID.DistanceBucket(nodeToFind)
		if distance < closestDistance {
			closestPosition = i
		}
		if distance > farthestDistance {
			farthestPosition = i
		}

	}

	printShortList(shortList, nodeToFind, closestPosition, farthestPosition)

	log.Printf("Looking at the closestPosition:%v for a shortList of len:%v", closestPosition, len(shortList))

	nothingCloser := false
	triedAll := false
	for !triedAll && !nothingCloser { //we will keep looping until we hit one of two conditions:

		log.Printf("Beginning of iteration\n")
		printShortList(shortList, nodeToFind, closestPosition, farthestPosition)

		//there are k active nodes in the short list (tried everything) or nothing returned is closer than before
		findNodeResponseChannel := make(chan []*Contact, const_alpha) //Up to alpha going at the same time
		timer := time.NewTimer(timeout)                               //create a new timer
		for i := 0; i < const_alpha; i++ {
			triedAll = true //let's assume we've tried everything
			//let's pick the first three things that haven't been checked
			for j := 0; j < len(shortList); j++ {
				if shortList[j].checked == false {
					shortList[j].checked = true
					if j != len(shortList)-1 {
						triedAll = false
						log.Printf("Tried all in short list")
					}
					//worrying about the address later
					go func() {
						error, result := kademlia.SendFindNode(shortList[j].contact, shortList[j].contact.NodeID) //send out the separate threads
						if error == nil {
							findNodeResponseChannel <- result
						}
					}()
					break
				}
			}
		}
		<-timer.C //stop executing until the timer runs out
		log.Printf("Looking at the closestPosition:%v", closestPosition)
		closestDistance := shortList[closestPosition].contact.NodeID.DistanceBucket(nodeToFind)
		farthestDistance := shortList[farthestPosition].contact.NodeID.DistanceBucket(nodeToFind)
		//Collect everything
		nothingCloser = true //true until proven guilty

		checkedAllChannelsWithResponses := false
		for !checkedAllChannelsWithResponses {
			select {
			case newNodes := <-findNodeResponseChannel:

				for j := 0; j < len(newNodes); j++ { //look through every item we found

					//Ignore nodes we already have in the shortlist
					if shortListContains(shortList, newNodes[j]) {
						continue
					}

					distance := newNodes[j].NodeID.DistanceBucket(nodeToFind)

					if len(shortList) != cap(shortList) { //we aren't at capacity
						newContact := new(IterativeContact) //convert them to this data struct
						newContact.checked = false
						newContact.contact = newNodes[j]

						shortList = shortList[0 : len(shortList)+1]
						shortList[len(shortList)-1] = newContact

						if distance > farthestDistance {
							farthestPosition = len(shortList) - 1
						} else if distance < closestDistance {
							closestPosition = len(shortList) - 1
							nothingCloser = false
						}

					} else if distance < farthestDistance { //we have no room so we only want to add things that are closer
						newContact := new(IterativeContact) //convert them to this data struct
						newContact.checked = false
						newContact.contact = newNodes[j]
						shortList[farthestPosition] = newContact //kick out the furthest thing
						if distance < closestDistance {
							closestPosition = farthestPosition
							nothingCloser = false
						}
						farthestDistance = 0
						for k := 0; k < len(shortList); k++ {
							distance = shortList[k].contact.NodeID.DistanceBucket(nodeToFind)
							farthestDistance = shortList[farthestDistance].contact.NodeID.DistanceBucket(nodeToFind)
							if distance > farthestDistance {
								farthestDistance = k
							}
						}
					}
				}
			default:
				checkedAllChannelsWithResponses = true
			}
		}

		log.Printf("Iteration complete\n")
		printShortList(shortList, nodeToFind, closestPosition, farthestPosition)
	}

	returnContacts := make([]*Contact, len(shortList))
	for i := 0; i < len(shortList); i++ {
		returnContacts[i] = shortList[i].contact
	}
	return nil, returnContacts
}

func (kademlia *Kademlia) SendFindNode(contact *Contact, nodeToFind ID) (error, []*Contact) {
	client, err := rpc.DialHTTP("tcp", contact.GetAddress())

	if err != nil {
		log.Printf("Connection error, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err, nil
	}
	log.Printf("Sending find node to %v\n", contact.GetAddress())
	findNodeRequest := new(FindNodeRequest)
	findNodeRequest.Sender = *kademlia.RoutingTable.SelfContact
	findNodeRequest.MsgID = NewRandomID()
	findNodeRequest.NodeID = nodeToFind

	findNodeResult := new(FindNodeResult)
	err = client.Call("Kademlia.FindNode", findNodeRequest, findNodeResult)
	if err != nil {
		log.Printf("Error in remote node response, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err, nil
	}

	log.Printf("Received response\n")

	if findNodeRequest.MsgID.Equals(findNodeResult.MsgID) {
		kademlia.markAliveAndPossiblyPing(contact)
		contacts := make([]*Contact, len(findNodeResult.Nodes), len(findNodeResult.Nodes))

		for i := 0; i < len(findNodeResult.Nodes); i++ {
			contacts[i] = findNodeResult.Nodes[i].ToContact()
		}
		return nil, contacts

	} else {
		log.Printf("Incorrect MsgID\n")
		log.Printf("      Request Message ID: %v\n", findNodeRequest.MsgID.AsString())
		log.Printf("       Result Message ID: %v\n", findNodeResult.MsgID.AsString())
		kademlia.RoutingTable.MarkDead(contact)
		return errors.New("Incorrect MsgID"), nil
	}
	client.Close()
	return nil, nil
}

func (kademlia *Kademlia) SendFindValue(contact *Contact, key ID) (error, []byte, []*Contact) {
	client, err := rpc.DialHTTP("tcp", contact.GetAddress())
	if err != nil {
		log.Printf("Connection error, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err, nil, nil
	}
	log.Printf("Sending find value to %v\n", contact.GetAddress())
	log.Printf("         Key to find: %v\n", key.AsString())
	findValueRequest := new(FindValueRequest)
	findValueRequest.Sender = *kademlia.RoutingTable.SelfContact
	findValueRequest.MsgID = NewRandomID()
	findValueRequest.Key = key

	findValueResult := new(FindValueResult)
	err = client.Call("Kademlia.FindValue", findValueRequest, findValueResult)
	if err != nil {
		log.Printf("Error in remote node response, marking node dead")
		kademlia.RoutingTable.MarkDead(contact)
		return err, nil, nil
	}

	log.Printf("Received response\n")
	if findValueRequest.MsgID.Equals(findValueResult.MsgID) {
		kademlia.markAliveAndPossiblyPing(contact)
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
		log.Printf("Incorrect MsgID\n")
		log.Printf("      Request Message ID: %v\n", findValueRequest.MsgID.AsString())
		log.Printf("       Result Message ID: %v\n", findValueResult.MsgID.AsString())
		kademlia.RoutingTable.MarkDead(contact)
		return errors.New("Incorrect MsgID"), nil, nil
	}
	client.Close()
	return nil, nil, nil
}
