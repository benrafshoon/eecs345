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
	list    *list.List
	pending *Contact
}

func newKBucket() *kBucket {
	bucket := new(kBucket)
	bucket.list = list.New()
	bucket.pending = nil
	return bucket
}

func (b *kBucket) AddOrMoveToTail(contact *Contact) bool {
	element := b.list.Front()
	for element != nil {
		if element.Value.(*Contact).NodeID.Equals(contact.NodeID) {
			b.list.MoveToBack(element)
			return true
		}
		element = element.Next()
	}
	if b.IsFull() {
		return false
	} else {
		b.list.PushBack(contact)
		return true
	}
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

func (b *kBucket) GetHead() *Contact {
	frontElement := b.list.Front()
	if frontElement == nil {
		return nil
	} else {
		return frontElement.Value.(*Contact)
	}
}

func (b *kBucket) FindContactByNodeID(lookupID ID) (bool, *Contact) {
	element := b.list.Front()
	for element != nil {
		if element.Value.(*Contact).NodeID.Equals(lookupID) {
			return true, element.Value.(*Contact)
		}
		element = element.Next()
	}
	return false, nil
}

func (b *kBucket) IsFull() bool {
	return b.list.Len() >= const_k
}

func (b *kBucket) Delete(contact *Contact) bool {
	element := b.list.Front()
	for element != nil {
		if element.Value.(*Contact).NodeID.Equals(contact.NodeID) {
			b.list.Remove(element)
			return true
		}
		element = element.Next()
	}
	return false
}

func (b *kBucket) IsEmpty() bool {
	return b.list.Len() == 0
}
