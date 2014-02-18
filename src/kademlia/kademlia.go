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
}

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

		log.Printf("Received pong from %v:%v\n", pong.Sender.Host, pong.Sender.Port)
		log.Printf("          Node ID: %v\n", pong.Sender.NodeID.AsString())

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

func shortListDelete(shortList []*IterativeContact, toDelete *Contact) []*IterativeContact {
	for i := 0; i < len(shortList); i++ {
		if shortList[i].contact.Equals(toDelete) {
			for j := i; j < len(shortList)-1; j++ {
				shortList[j+1] = shortList[j]
			}
			return shortList[0 : len(shortList)-1]
		}
	}
	return shortList
}

func shortListClosestTo(shortList []*IterativeContact, closestTo ID) *Contact {
	var closest *Contact = nil
	closestDistance := const_B

	for i := 0; i < len(shortList); i++ {
		currentDistance := shortList[i].contact.NodeID.DistanceBucket(closestTo)
		if currentDistance < closestDistance {
			closestDistance = currentDistance
			closest = shortList[i].contact
		}
	}
	return closest
}

const iterativeFindNodeOperation = "IterativeFindNodeOperation"
const iterativeFindValueOperation = "IterativeFindValueOperation"

type iterativeStepResult struct {
	Contact       *Contact
	FoundValue    []byte
	FoundContacts []*Contact
}

func (kademlia *Kademlia) iterativeOperation(toFind ID, operationType string) (error, *Contact, []byte, []*Contact) {
	log.Printf("Iterative operation for ID %v", toFind.AsString())

	//Check to see if we have the value locally first
	if operationType == iterativeFindValueOperation {
		localValue := kademlia.Data.RetrieveValue(toFind)
		if localValue != nil {
			return nil, kademlia.RoutingTable.SelfContact, localValue, nil
		}
	}

	//This function should return a list of k closest contacts to the specified node
	shortList := make([]*IterativeContact, 0, const_k) //slice - array with 0 things now and a capacity of const_k
	//have to do at least one call to kick it off
	foundContacts := kademlia.RoutingTable.FindKClosestNodes(const_k, toFind, nil)

	var closestPosition, farthestPosition int = 0, 0

	for i := 0; i < len(foundContacts); i++ {
		newContact := new(IterativeContact) //convert them to this data struct
		newContact.checked = foundContacts[i].Equals(kademlia.RoutingTable.SelfContact)
		newContact.contact = foundContacts[i]
		shortList = shortList[0 : i+1]
		shortList[i] = newContact
		distance := foundContacts[i].NodeID.DistanceBucket(toFind)
		closestDistance := shortList[closestPosition].contact.NodeID.DistanceBucket(toFind)
		farthestDistance := shortList[farthestPosition].contact.NodeID.DistanceBucket(toFind)
		if distance < closestDistance {
			closestPosition = i
		}
		if distance > farthestDistance {
			farthestPosition = i
		}
	}

	nothingCloser := false
	triedAll := false
	numResponsesSent := 0

	for !triedAll && !nothingCloser { //we will keep looping until we hit one of two conditions:

		//printShortList(shortList, toFind, closestPosition, farthestPosition)

		//there are k active nodes in the short list (tried everything) or nothing returned is closer than before
		iterativeStepResultChannel := make(chan iterativeStepResult, const_alpha) //Up to alpha going at the same time
		timer := time.NewTimer(timeout)                                           //create a new timer
		for i := 0; i < const_alpha; i++ {
			//let's pick the first three things that haven't been checked
			for j := 0; j < len(shortList); j++ {
				if shortList[j].checked == false {
					shortList[j].checked = true
					log.Printf("Finding node %v", shortList[j].contact.NodeID.AsString())
					//worrying about the address later

					go func(toContact *Contact) { //send out the separate threads
						var error error = nil
						var foundValue []byte = nil
						var foundContacts []*Contact = nil

						switch operationType {
						case iterativeFindNodeOperation:
							error, foundContacts = kademlia.SendFindNode(toContact, toFind)
						case iterativeFindValueOperation:
							error, foundValue, foundContacts = kademlia.SendFindValue(toContact, toFind)
						}
						if error == nil {
							iterativeStepResultChannel <- iterativeStepResult{Contact: toContact, FoundValue: foundValue, FoundContacts: foundContacts}
						} else {
							iterativeStepResultChannel <- iterativeStepResult{Contact: toContact, FoundValue: nil, FoundContacts: nil}
						}
					}(shortList[j].contact)
					//Capture shortList[j] in argument because j changes
					numResponsesSent++
					break
				}
			}
		}
		log.Printf("Looking at the closestPosition:%v", closestPosition)
		closestDistance := shortList[closestPosition].contact.NodeID.DistanceBucket(toFind)
		farthestDistance := shortList[farthestPosition].contact.NodeID.DistanceBucket(toFind)
		//Collect everything
		nothingCloser = true //true until proven guilty

		timedOut := false
		numResponsesReceived := 0
		//Terminate once we have received responses from all the FindNodes we sent out, or when we timeout
		for !timedOut && numResponsesReceived != numResponsesSent {
			//Read from response channel and check if timed out
			select {
			case result := <-iterativeStepResultChannel:
				if result.FoundValue != nil {
					log.Printf("   Found the value")
					shortList = shortListDelete(shortList, result.Contact)
					closest := shortListClosestTo(shortList, toFind) //This could be nil if the shortList only had one contact
					if closest != nil && closest.NodeID.DistanceBucket(toFind) < result.Contact.NodeID.DistanceBucket(toFind) {
						log.Printf("Node %v is closer than node that found value %v", closest.NodeID.AsString, result.Contact.NodeID.AsString())
						log.Printf("Storing key value pair in closer node")
						go kademlia.SendStore(closest, toFind, result.FoundValue)
					}
					return nil, result.Contact, result.FoundValue, nil
				} else if result.FoundContacts != nil {
					log.Printf("   Found some contacts")
					for j := 0; j < len(result.FoundContacts); j++ { //look through every item we found
						log.Printf("   Adding node %v", result.FoundContacts[j].NodeID.AsString())
						//Ignore nodes we already have in the shortlist
						if shortListContains(shortList, result.FoundContacts[j]) {
							log.Printf("   Node already in shortlist")
							continue
						}

						distance := result.FoundContacts[j].NodeID.DistanceBucket(toFind)

						if len(shortList) != cap(shortList) { //we aren't at capacity
							newContact := new(IterativeContact) //convert them to this data struct
							newContact.checked = false
							newContact.contact = result.FoundContacts[j]

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
							newContact.contact = result.FoundContacts[j]
							shortList[farthestPosition] = newContact //kick out the furthest thing
							if distance < closestDistance {
								closestPosition = farthestPosition
								nothingCloser = false
							}
							farthestDistance = 0
							for k := 0; k < len(shortList); k++ {
								distance = shortList[k].contact.NodeID.DistanceBucket(toFind)
								farthestDistance = shortList[farthestPosition].contact.NodeID.DistanceBucket(toFind)
								if distance > farthestDistance {
									farthestDistance = k
								}
							}
						}
					}
				} else {
					log.Printf("Node dead, removing from shortlist")
					shortList = shortListDelete(shortList, result.Contact)
				}

				numResponsesReceived++
				if numResponsesReceived == numResponsesSent {
					log.Printf("All responses received")
				}
			case <-timer.C: //stop executing until the timer runs out
				log.Printf("timed out")
				timedOut = true
			}
		}

		triedAll = true
		for i := 0; i < len(shortList); i++ {
			if shortList[i].checked == false {
				triedAll = false
				break
			}
		}

		if triedAll {
			log.Printf("Search terminated because tried all in short list")
		}
		if nothingCloser {
			log.Printf("Search terminated because nothing closer found in iteration")
		}
	}

	returnContacts := make([]*Contact, len(shortList))
	for i := 0; i < len(shortList); i++ {
		//Not 100% sure we should be adding nodes from iterativeFindNode, but it seems to make routing work better
		kademlia.markAliveAndPossiblyPing(shortList[i].contact)
		returnContacts[i] = shortList[i].contact

	}
	return nil, nil, nil, returnContacts
}

func (kademlia *Kademlia) SendIterativeFindNode(nodeToFind ID) (error, []*Contact) {
	error, _, _, contacts := kademlia.iterativeOperation(nodeToFind, iterativeFindNodeOperation)
	kademlia.PrintRoutingTable()
	return error, contacts
}

func (kademlia *Kademlia) SendIterativeFindValue(keyToFind ID) (error, *Contact, []byte, []*Contact) {
	return kademlia.iterativeOperation(keyToFind, iterativeFindValueOperation)
}

func (kademlia *Kademlia) SendIterativeStore(key ID, value []byte) (*Contact, error) {
	error, contacts := kademlia.SendIterativeFindNode(key)
	if error != nil {
		return nil, error
	}

	for i := 0; i < len(contacts); i++ {
		kademlia.SendStore(contacts[i], key, value)
	}

	return contacts[len(contacts)-1], nil
}

type sendFindNodeResult struct {
	err      error
	contacts []*Contact
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
