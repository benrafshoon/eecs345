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
	"container/list"
)


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

