package kademlia

import (
	"container/list"
	"fmt"
	"log"
	"time"
)

type IterativeContact struct {
	contact *Contact
	foundBy *IterativeContact //Forms a list
	checked bool
}

//Given an iterative contact, returns the path traversed from the source to the contact
//The front of the list is the source node
//The back of the list is the node this is called on
//Each node between is on the path
func (c *IterativeContact) collectPath(source *Contact) *list.List {
	path := list.New()
	current := c
	for !current.contact.Equals(source) {
		path.PushFront(current.contact)
		current = current.foundBy
	}
	path.PushFront(source)
	return path
}

type ShortList struct {
	list          []*IterativeContact
	closestIndex  int
	farthestIndex int
	toFind        ID
}

func newShortList(toFind ID) *ShortList {
	shortList := new(ShortList)
	shortList.toFind = toFind
	shortList.list = make([]*IterativeContact, 0, const_k) //slice - array with 0 things now and a capacity of const_k
	shortList.closestIndex = -1
	shortList.farthestIndex = -1
	return shortList
}

func (s *ShortList) closestDistance() int {
	return s.list[s.closestIndex].contact.NodeID.DistanceBucket(s.toFind)
}

func (s *ShortList) farthestDistance() int {
	return s.list[s.farthestIndex].contact.NodeID.DistanceBucket(s.toFind)
}

func (s *ShortList) insert(toInsert *Contact, foundBy *IterativeContact) {
	distance := toInsert.NodeID.DistanceBucket(s.toFind)

	if len(s.list) != cap(s.list) { //we aren't at capacity
		newContact := new(IterativeContact) //convert them to this data struct
		newContact.checked = false
		newContact.contact = toInsert
		newContact.foundBy = foundBy

		s.list = s.list[0 : len(s.list)+1]
		s.list[len(s.list)-1] = newContact

		if s.farthestIndex == -1 || distance > s.farthestDistance() {
			s.farthestIndex = len(s.list) - 1
		} else if s.closestIndex == -1 || distance < s.closestDistance() {
			s.closestIndex = len(s.list) - 1
		}

	} else if distance < s.farthestDistance() { //we have no room so we only want to add things that are closer
		newContact := new(IterativeContact) //convert them to this data struct
		newContact.checked = false
		newContact.contact = toInsert
		s.list[s.farthestIndex] = newContact //kick out the furthest thing
		if s.closestIndex == -1 || distance < s.closestDistance() {
			s.closestIndex = s.farthestIndex
		}
		s.farthestIndex = 0
		for k := 0; k < len(s.list); k++ {
			distance = s.list[k].contact.NodeID.DistanceBucket(s.toFind)
			if distance > s.farthestDistance() {
				s.farthestIndex = k
			}
		}
	}
}

func (s *ShortList) contains(contact *Contact) bool {
	for i := 0; i < len(s.list); i++ {
		if contact.Equals(s.list[i].contact) {
			return true
		}
	}
	return false
}

func (s *ShortList) delete(toDelete *Contact) {
	for i := 0; i < len(s.list); i++ {
		if s.list[i].contact.Equals(toDelete) {
			for j := i; j < len(s.list)-1; j++ {
				s.list[j+1] = s.list[j]
			}
			s.list = s.list[0 : len(s.list)-1]
		}
	}
}

func (s *ShortList) closestToToFind() *Contact {
	var closest *Contact = nil
	closestDistance := const_B

	for i := 0; i < len(s.list); i++ {
		currentDistance := s.list[i].contact.NodeID.DistanceBucket(s.toFind)
		if currentDistance < closestDistance {
			closestDistance = currentDistance
			closest = s.list[i].contact
		}
	}
	return closest
}

func (s *ShortList) triedAll() bool {
	triedAll := true
	for i := 0; i < len(s.list); i++ {
		if s.list[i].checked == false {
			triedAll = false
			break
		}
	}
	return triedAll
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

const iterativeFindNodeOperation = "IterativeFindNodeOperation"
const iterativeFindValueOperation = "IterativeFindValueOperation"

type iterativeStepResult struct {
	Contact       *IterativeContact
	FoundValue    []byte
	FoundContacts []*Contact
}

type iterativeOperationResult struct {
	Error         error
	WhereFound    *Contact
	FoundValue    []byte
	FoundContacts []*Contact
	Path          *list.List
}

func (kademlia *Kademlia) iterativeOperation(toFind ID, operationType string) iterativeOperationResult {
	returnValue := iterativeOperationResult{nil, nil, nil, nil, nil}

	log.Printf("Iterative operation for ID %v", toFind.AsString())

	//Check to see if we have the value locally first
	if operationType == iterativeFindValueOperation {
		localValue := kademlia.Data.RetrieveValue(toFind)
		if localValue != nil {
			returnValue.WhereFound = kademlia.RoutingTable.SelfContact
			returnValue.FoundValue = localValue
			return returnValue
		}
	}

	//This function should return a list of k closest contacts to the specified node
	shortList := newShortList(toFind)
	//have to do at least one call to kick it off
	foundContacts := kademlia.RoutingTable.FindKClosestNodes(const_k, toFind, nil)

	selfIterativeContact := new(IterativeContact)
	selfIterativeContact.contact = kademlia.RoutingTable.SelfContact

	for i := 0; i < len(foundContacts); i++ {
		shortList.insert(foundContacts[i], selfIterativeContact)
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
			for j := 0; j < len(shortList.list); j++ {
				if shortList.list[j].checked == false {
					shortList.list[j].checked = true
					log.Printf("Finding node %v", shortList.list[j].contact.NodeID.AsString())
					//worrying about the address later

					go func(toContact *IterativeContact) { //send out the separate threads
						var error error = nil
						var foundValue []byte = nil
						var foundContacts []*Contact = nil

						switch operationType {
						case iterativeFindNodeOperation:
							error, foundContacts = kademlia.SendFindNode(toContact.contact, toFind)
						case iterativeFindValueOperation:
							error, foundValue, foundContacts = kademlia.SendFindValue(toContact.contact, toFind)
						}
						if error == nil {
							iterativeStepResultChannel <- iterativeStepResult{Contact: toContact, FoundValue: foundValue, FoundContacts: foundContacts}
						} else {
							iterativeStepResultChannel <- iterativeStepResult{Contact: toContact, FoundValue: nil, FoundContacts: nil}
						}
					}(shortList.list[j])
					//Capture shortList[j] in argument because j changes
					numResponsesSent++
					break
				}
			}
		}

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
					shortList.delete(result.Contact.contact)
					closest := shortList.closestToToFind() //This could be nil if the shortList only had one contact
					if closest != nil && closest.NodeID.DistanceBucket(toFind) < result.Contact.contact.NodeID.DistanceBucket(toFind) {
						log.Printf("Node %v is closer than node that found value %v", closest.NodeID.AsString, result.Contact.contact.NodeID.AsString())
						log.Printf("Storing key value pair in closer node")
						go kademlia.SendStore(closest, toFind, result.FoundValue)
					}
					returnValue.Path = result.Contact.collectPath(kademlia.RoutingTable.SelfContact)
					returnValue.WhereFound = result.Contact.contact
					returnValue.FoundValue = result.FoundValue

					return returnValue
				} else if result.FoundContacts != nil {
					log.Printf("   Found some contacts")
					for j := 0; j < len(result.FoundContacts); j++ { //look through every item we found
						log.Printf("   Adding node %v", result.FoundContacts[j].NodeID.AsString())
						//Ignore nodes we already have in the shortlist
						if shortList.contains(result.FoundContacts[j]) {
							log.Printf("   Node already in shortlist")
						} else {
							shortList.insert(result.FoundContacts[j], result.Contact)
							//If the new closest index is the one we just inserted
							if shortList.list[shortList.closestIndex].contact.Equals(result.FoundContacts[j]) {
								nothingCloser = false
							}
						}
					}
				} else {
					log.Printf("Node dead, removing from shortlist")
					shortList.delete(result.Contact.contact)
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

		triedAll = shortList.triedAll()

		if triedAll {
			log.Printf("Search terminated because tried all in short list")
		}
		if nothingCloser {
			log.Printf("Search terminated because nothing closer found in iteration")
		}
	}

	returnContacts := make([]*Contact, len(shortList.list))
	for i := 0; i < len(shortList.list); i++ {
		//Not 100% sure we should be adding nodes from iterativeFindNode, but it seems to make routing work better
		kademlia.markAliveAndPossiblyPing(shortList.list[i].contact)
		returnContacts[i] = shortList.list[i].contact

	}

	returnValue.FoundContacts = returnContacts
	returnValue.Path = shortList.list[shortList.closestIndex].collectPath(kademlia.RoutingTable.SelfContact)
	log.Printf("Path to closest found node")
	current := returnValue.Path.Front()
	for current != nil {
		log.Printf(current.Value.(*Contact).NodeID.AsString())
		current = current.Next()
	}
	return returnValue
}

func (kademlia *Kademlia) SendIterativeFindNode(nodeToFind ID) (error, []*Contact) {
	returnValue := kademlia.iterativeOperation(nodeToFind, iterativeFindNodeOperation)
	return returnValue.Error, returnValue.FoundContacts
}

func (kademlia *Kademlia) SendIterativeFindValue(keyToFind ID) (error, *Contact, []byte, []*Contact) {
	returnValue := kademlia.iterativeOperation(keyToFind, iterativeFindValueOperation)
	return returnValue.Error, returnValue.WhereFound, returnValue.FoundValue, returnValue.FoundContacts
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
