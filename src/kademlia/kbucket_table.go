package kademlia

import (
	"log"
)

type kBucketTableRequest interface {
	RequestType() string
}

type KBucketTable struct {
	kBuckets    []*kBucket
	SelfContact *Contact
	requests    chan kBucketTableRequest
}

func NewKBucketTable() *KBucketTable {
	table := new(KBucketTable)
	table.SelfContact = new(Contact)
	table.kBuckets = make([]*kBucket, const_B, const_B)
	for i := 0; i < const_B; i++ {
		table.kBuckets[i] = newKBucket()
	}
	table.requests = make(chan kBucketTableRequest)
	go table.processKBucketTableRequests()
	return table
}

//Thread-safe
//Returns needToPing, contactToPing
//needToPing will be true if the contact being marked alive is new in a k bucket
//and the k bucket is full.  The contact returned is the least recently seen contact
//Need to send a ping request to contactToPing if needToPing is true
func (kBucketTable *KBucketTable) MarkAlive(contact *Contact) (bool, *Contact) {
	log.Printf("Sending MarkAlive request")
	request := markAliveRequest{contact, make(chan markAliveResult)}
	kBucketTable.requests <- request
	result := <-request.result
	return result.toPing != nil, result.toPing
}

//Thread-safe
func (kBucketTable *KBucketTable) MarkDead(contact *Contact) {
	log.Printf("Sending MarkDead request")
	request := markDeadRequest{contact}
	kBucketTable.requests <- request
}

//Thread-safe
func (kBucketTable *KBucketTable) FindKClosestNodes(k int, closestTo ID, exclude ID) []*Contact {
	log.Printf("Sending FindKClosestNodes request")
	request := findKClosestNodesRequest{k, closestTo, exclude, make(chan findKClosestNodesResult)}
	kBucketTable.requests <- request
	result := <-request.result
	return result.kClosestNodes
}

//Thread-safe
//Returns hasContact?, isSelf?, contact
func (kBucketTable *KBucketTable) LookupContactByNodeID(lookupID ID) (bool, bool, *Contact) {
	log.Printf("Sending LookupContactByNodeID request")
	request := lookupContactByNodeIDRequest{lookupID, make(chan lookupContactByNodeIDResult)}
	kBucketTable.requests <- request
	result := <-request.result
	return result.hasContact, result.isSelf, result.contact
}

func (kBucketTable *KBucketTable) processKBucketTableRequests() {
	log.Printf("Processing k-bucket table requests")
	for {
		request := <-kBucketTable.requests
		switch request.RequestType() {
		case "MarkAlive":
			log.Printf("Received MarkAlive request")
			kBucketTable.markAliveInternal(request.(markAliveRequest))
		case "MarkDead":
			log.Printf("Received MarkDead request")
			kBucketTable.markDeadInternal(request.(markDeadRequest))
		case "FindKClosestNodes":
			log.Printf("Received FindKClosestNodes request")
			kBucketTable.findKClosestNodesInternal(request.(findKClosestNodesRequest))
		case "LookupContactByNodeID":
			log.Printf("Received LookupContactByNodeID request")
			kBucketTable.lookupContactByNodeIDInternal(request.(lookupContactByNodeIDRequest))
		default:
			log.Printf("Invalid request to k-bucket table")
		}
	}
}

type markAliveRequest struct {
	contact *Contact
	result  chan markAliveResult
}

func (r markAliveRequest) RequestType() string {
	return "MarkAlive"
}

type markAliveResult struct {
	toPing *Contact
}

func (kBucketTable *KBucketTable) markAliveInternal(request markAliveRequest) {
	var toPing *Contact = nil
	distanceBucket := kBucketTable.SelfContact.NodeID.DistanceBucket(request.contact.NodeID)
	if distanceBucket != -1 {
		kBucket := kBucketTable.kBuckets[distanceBucket]
		log.Printf("Marking node in bucket %v as alive", distanceBucket)
		if kBucket.AddOrMoveToTail(request.contact) {
			//Could add or already in table
			if kBucket.pending != nil && kBucket.GetHead().Equals(request.contact) {
				log.Printf("Head node ", request.contact.NodeID.AsString(), " responded, ignoring other new node")
				kBucket.pending = nil
			}
		} else {
			//Could not add, so we'll put the node as pending in the kbucket and ping the head
			log.Printf("Tried to add node, but k bucket was full")

			//If pending is not nil, then we already have a pending node and pinged the head
			//Because we want the least recently seen nodes, we will have to ignore the request's node
			if kBucket.pending == nil {
				kBucket.pending = request.contact
				toPing = kBucket.GetHead()
				log.Printf("Need to ping node ", toPing.NodeID.AsString())
			}
		}
	} else {
		log.Printf("Marking self as alive")
	}
	request.result <- markAliveResult{toPing}
}

type markDeadRequest struct {
	contact *Contact
}

func (r markDeadRequest) RequestType() string {
	return "MarkDead"
}

//No response

func (kBucketTable *KBucketTable) markDeadInternal(request markDeadRequest) {
	if len(request.contact.NodeID) == 0 {
		log.Printf("Node ID nil")
		return
	}
	distanceBucket := kBucketTable.SelfContact.NodeID.DistanceBucket(request.contact.NodeID)
	if distanceBucket == -1 {
		log.Printf("Marking self dead")
		return
	}
	kBucket := kBucketTable.kBuckets[distanceBucket]
	kBucket.Delete(request.contact)
	if kBucket.pending != nil {
		log.Printf("Now there is space for node ", kBucket.pending.NodeID.AsString())
		kBucket.AddToTail(kBucket.pending)
		kBucket.pending = nil
	}
}

type findKClosestNodesRequest struct {
	k         int
	closestTo ID
	exclude   ID
	result    chan findKClosestNodesResult
}

func (r findKClosestNodesRequest) RequestType() string {
	return "FindKClosestNodes"
}

type findKClosestNodesResult struct {
	kClosestNodes []*Contact
}

//Exact search that exhaustivly searches through the entire k_bucket table
//runtime: O(number of nodes in the routing table * k)
func (kBucketTable *KBucketTable) findKClosestNodesInternal(request findKClosestNodesRequest) {

	kClosest := make([]*Contact, 0, request.k)

	kClosest = insertIntoClosestSoFar(kClosest, kBucketTable.SelfContact, request.closestTo, request.exclude)
	for i := 0; i < len(kBucketTable.kBuckets); i++ {
		kBucketList := kBucketTable.kBuckets[i].list
		element := kBucketList.Front()
		for element != nil {
			kClosest = insertIntoClosestSoFar(kClosest, element.Value.(*Contact), request.closestTo, request.exclude)
			element = element.Next()
		}

	}

	request.result <- findKClosestNodesResult{kClosest}

}

type lookupContactByNodeIDRequest struct {
	lookupID ID
	result   chan lookupContactByNodeIDResult
}

func (r lookupContactByNodeIDRequest) RequestType() string {
	return "LookupContactByNodeID"
}

type lookupContactByNodeIDResult struct {
	hasContact bool
	isSelf     bool
	contact    *Contact
}

func (kBucketTable *KBucketTable) lookupContactByNodeIDInternal(request lookupContactByNodeIDRequest) {
	var hasContact bool
	var isSelf bool
	var contact *Contact

	bucketIndex := kBucketTable.SelfContact.NodeID.DistanceBucket(request.lookupID)
	isSelf = bucketIndex == -1
	if isSelf {
		hasContact = true
		contact = kBucketTable.SelfContact
	} else {
		hasContact, contact = kBucketTable.kBuckets[bucketIndex].FindContactByNodeID(request.lookupID)
	}

	request.result <- lookupContactByNodeIDResult{hasContact, isSelf, contact}
}

//Helper function
func intMin(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

//Helper function
func insertIntoClosestSoFar(closestSoFar []*Contact, toInsert *Contact, closestTo ID, exclude ID) []*Contact {
	if toInsert.NodeID.Equals(exclude) {
		return closestSoFar
	}

	for i := 0; i < len(closestSoFar); i++ {
		if toInsert.NodeID.DistanceBucket(closestTo) < closestSoFar[i].NodeID.DistanceBucket(closestTo) {
			//Insert toInsert at position i

			if len(closestSoFar) < cap(closestSoFar) {
				log.Printf("Exapanding capacity from %v to %v", len(closestSoFar), len(closestSoFar)+1)
				closestSoFar = closestSoFar[0 : len(closestSoFar)+1]
			} else {
				log.Printf("ClosestSoFar Full")
			}
			log.Printf("len %v cap %v min %v", len(closestSoFar), cap(closestSoFar), intMin(len(closestSoFar)-1, cap(closestSoFar)-2))
			for j := len(closestSoFar) - 2; j >= i; j-- {
				closestSoFar[j+1] = closestSoFar[j]
			}

			closestSoFar[i] = toInsert

			return closestSoFar
		}
	}
	//if len < cap, append to end
	if len(closestSoFar) < cap(closestSoFar) {
		closestSoFar = closestSoFar[0 : len(closestSoFar)+1]
		closestSoFar[len(closestSoFar)-1] = toInsert
	}

	return closestSoFar
}
