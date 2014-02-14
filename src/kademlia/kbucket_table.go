package kademlia


import (
	"log"
)


type KBucketTableRequest interface {
	RequestType() string
}



type KBucketTable struct {
	kBuckets []*kBucket
	SelfContact *Contact
	requests chan KBucketTableRequest;
}

func NewKBucketTable() *KBucketTable {
	table := new(KBucketTable)
	table.SelfContact = new(Contact)
	table.kBuckets = make([]*kBucket, const_B, const_B)
	for i := 0; i < const_B; i++ {
        table.kBuckets[i] = newKBucket()
    }
    table.requests = make(chan KBucketTableRequest)
    go table.processKBucketTableRequests()
	return table
}

//Thread-safe
func (kBucketTable *KBucketTable) MarkAlive(contact *Contact) {
	log.Printf("Sending MarkAlive request")
	request := MarkAliveRequest{contact}
	kBucketTable.requests <- request
}

//Thread-safe
func (kBucketTable *KBucketTable) MarkDead(contact *Contact) {
	log.Printf("Sending MarkDead request")
	request := MarkDeadRequest{contact}
	kBucketTable.requests <- request
}


//Thread-safe
func (kBucketTable *KBucketTable) FindKClosestNodes(k int, closestTo ID, exclude ID) []*Contact {
	log.Printf("Sending FindKClosestNodes request")
	request := FindKClosestNodesRequest{k, closestTo, exclude, make(chan FindKClosestNodesResult)}
	kBucketTable.requests <- request
	result := <- request.result
	return result.kClosestNodes
}

//Thread-safe
//Returns hasContact?, isSelf?, contact
func (kBucketTable *KBucketTable) LookupContactByNodeID(lookupID ID) (bool, bool, *Contact) {
    log.Printf("Sending LookupContactByNodeID request")
    request := LookupContactByNodeIDRequest{lookupID, make(chan LookupContactByNodeIDResult)}
    kBucketTable.requests <- request
    result := <- request.result
    return result.hasContact, result.isSelf, result.contact
}


func (kBucketTable *KBucketTable) processKBucketTableRequests() {
	log.Printf("Processing k-bucket table requests")
	for {
		request := <- kBucketTable.requests
		switch request.RequestType() {
		case "MarkAlive":
			log.Printf("Received MarkAlive request")
			kBucketTable.markAliveInternal(request.(MarkAliveRequest))
		case "MarkDead":
			log.Printf("Received MarkDead request")
			kBucketTable.markDeadInternal(request.(MarkDeadRequest))
		case "FindKClosestNodes":
			log.Printf("Received FindKClosestNodes request")
			kBucketTable.findKClosestNodesInternal(request.(FindKClosestNodesRequest))
		case "LookupContactByNodeID":
			log.Printf("Received LookupContactByNodeID request")
			kBucketTable.lookupContactByNodeIDInternal(request.(LookupContactByNodeIDRequest))
		default:
			log.Printf("Invalid request to k-bucket table")
		}
	}
}



type MarkAliveRequest struct {
	contact *Contact
}

func (r MarkAliveRequest) RequestType() string {
	return "MarkAlive"
}

//No response

func (kBucketTable *KBucketTable) markAliveInternal(request MarkAliveRequest) {
	distanceBucket := kBucketTable.SelfContact.NodeID.DistanceBucket(request.contact.NodeID)
	if distanceBucket != -1 {
		log.Printf("Marking node in bucket %v as alive", distanceBucket)
		if !kBucketTable.kBuckets[distanceBucket].AddOrMoveToTail(request.contact) {
			kBucketTable.kBuckets[distanceBucket].DeleteFromHead(request.contact)
			kBucketTable.kBuckets[distanceBucket].AddToTail(request.contact)
		}
	} else {
		log.Printf("Marking self as alive")
	}
}




type MarkDeadRequest struct {
	contact *Contact
}

func (r MarkDeadRequest) RequestType() string {
	return "MarkDead"
}
//No response

func (kBucketTable *KBucketTable) markDeadInternal(request MarkDeadRequest) {
	log.Printf("Mark dead not yet implemented")
}



type FindKClosestNodesRequest struct {
	k int
	closestTo ID
	exclude ID
	result chan FindKClosestNodesResult
}

func (r FindKClosestNodesRequest) RequestType() string {
	return "FindKClosestNodes"
}

type FindKClosestNodesResult struct {
	kClosestNodes []*Contact
}

//Exact search that exhaustivly searches through the entire k_bucket table
//runtime: O(number of nodes in the routing table * k)
func (kBucketTable *KBucketTable) findKClosestNodesInternal(request FindKClosestNodesRequest) {

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

	request.result <- FindKClosestNodesResult{kClosest}

}



type LookupContactByNodeIDRequest struct {
	lookupID ID
	result chan LookupContactByNodeIDResult
}

func (r LookupContactByNodeIDRequest) RequestType() string {
	return "LookupContactByNodeID"
}

type LookupContactByNodeIDResult struct {
	hasContact bool
	isSelf bool
	contact *Contact
}

func (kBucketTable *KBucketTable) lookupContactByNodeIDInternal(request LookupContactByNodeIDRequest) {
	var hasContact bool
	var isSelf bool
	var contact *Contact

	bucketIndex := kBucketTable.SelfContact.NodeID.DistanceBucket(request.lookupID)
	isSelf = bucketIndex == -1
    log.Printf("Bucket %v", bucketIndex)
    if isSelf {
    	hasContact = true
        contact = kBucketTable.SelfContact
    } else {
        hasContact, contact = kBucketTable.kBuckets[bucketIndex].FindContactByNodeID(request.lookupID)
    }

    request.result <- LookupContactByNodeIDResult{hasContact, isSelf, contact}
}


//Helper function
func intMin(a int, b int) int {
	if(a < b) {
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
				closestSoFar = closestSoFar[0:len(closestSoFar) + 1]
			}

			for j := intMin(len(closestSoFar) - 1, cap(closestSoFar) - 2); j >= i; j-- {
				closestSoFar[j + 1] = closestSoFar[j]
			}

			closestSoFar[i] = toInsert

			return closestSoFar
		}
	}
	//if len < cap, append to end
	if len(closestSoFar) < cap(closestSoFar) {
		closestSoFar = closestSoFar[0:len(closestSoFar) + 1]
		closestSoFar[len(closestSoFar) - 1] = toInsert
	}

	return closestSoFar
}





