package kademlia
/*
	This file contains the implementation for a k-bucket. 
	The only function that should be needed externally is
	NewBucket and PingBucket. PingBucket will either add
	a contact to the bucket or send that contact to the end
	of the bucket if it has been seen before.

	ContainsNode can be used to check if a nodeid is contained
	in this bucket
*/

import (
	"log"
	"container/list"
)

type KBucketTable struct {
	kBuckets []*kBucket
	SelfContact *Contact
}

func NewKBucketTable() *KBucketTable {
	table := new(KBucketTable)
	table.SelfContact = new(Contact)
	table.kBuckets = make([]*kBucket, const_B, const_B)
	for i := 0; i < const_B; i++ {
        table.kBuckets[i] = newKBucket()
    }
	return table
}

func (kBucketTable *KBucketTable) MarkAlive(contact *Contact) {
	distanceBucket := kBucketTable.SelfContact.NodeID.DistanceBucket(contact.NodeID)
	if distanceBucket != -1 {
		if !kBucketTable.kBuckets[distanceBucket].AddOrMoveToTail(contact) {
			kBucketTable.kBuckets[distanceBucket].DeleteFromHead(contact)
			kBucketTable.kBuckets[distanceBucket].AddToTail(contact)
		}
	} else {
		log.Printf("Marking self as alive")
	}
	


}

func (kBucketTable *KBucketTable) MarkDead(contact *Contact) {

}

//Exact search that exhaustivly searches through the entire k_bucket table
//runtime: O(number of nodes in the routing table * k)
func (kBucketTable *KBucketTable) FindKClosestNodes(k int, closestTo ID, exclude ID) []*Contact {
	kClosest := make([]*Contact, 0, k)

	kClosest = insertIntoClosestSoFar(kClosest, kBucketTable.SelfContact, closestTo, exclude)
	for i := 0; i < len(kBucketTable.kBuckets); i++ {
		kBucketList := kBucketTable.kBuckets[i].list
		element := kBucketList.Front()
		for element != nil {
			kClosest = insertIntoClosestSoFar(kClosest, element.Value.(*Contact), closestTo, exclude)
			element = element.Next()
		}
		
	}

	return kClosest

}

func intMin(a int, b int) int {
	if(a < b) {
		return a
	} else {
		return b
	}
}

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

//Returns hasContact?, isSelf?, contact
func (kBucketTable *KBucketTable) LookupContactByNodeID(lookupID ID) (bool, bool, *Contact) {
    bucketIndex := kBucketTable.SelfContact.NodeID.DistanceBucket(lookupID)
    log.Printf("Bucket %v", bucketIndex)
    if bucketIndex != -1 {
        containsNode, contact := kBucketTable.kBuckets[bucketIndex].FindContactByNodeID(lookupID)
        if containsNode {
            return true, false, contact
        } else {
            return false, false, nil
        }
    } else {
        return true, true, kBucketTable.SelfContact
    }
}


type kBucket struct {
	list *list.List
}

func newKBucket() *kBucket {
	bucket := new(kBucket)
	bucket.list = list.New()
	return bucket
}

func (b *kBucket) AddOrMoveToTail(contact *Contact) bool {
	if b.IsFull() {
		return false
	}
	element := b.list.Front()
	for element != nil {
		if element.Value.(*Contact).NodeID.Equals(contact.NodeID) {
			b.list.MoveToBack(element)
			return true
		}
	}
	b.list.PushBack(contact)
	return true
}

func (b *kBucket) DeleteFromHead(contact *Contact) {
	head := b.list.Front()
	if head != nil {
		 b.list.Remove(head)
	}
}

func (b *kBucket) AddToTail(contact *Contact) bool {
	if !b.IsFull() {
		b.list.PushBack(contact)
		return true
	}
	return false
}

func (b *kBucket) FindContactByNodeID(lookupID ID) (bool, *Contact) {
	element := b.list.Front()
	for element != nil {
		if element.Value.(*Contact).NodeID.Equals(lookupID) {
			return true, element.Value.(*Contact)
		}
	}
	return false, nil
}

func (b *kBucket) IsFull() bool {
	return b.list.Len() >= const_k
}

