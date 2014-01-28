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
)

//need 160 buckets for 160 bit keys
const bucketSize = 160

type KBucketList []*Bucket

func NewKBucketList() KBucketList {
	kBucketList := make([]*Bucket, 160, 160)
	for i := 0; i < 160; i++ {
		kBucketList[i] = NewBucket()
	}
	return kBucketList
}


type Bucket struct {
	head *ContactItem 
	tail *ContactItem
	ItemCount int //will be set to 0 on init
}

type ContactItem struct {
	data Contact
	nextItem *ContactItem
}

func NewContactItem(contact Contact) *ContactItem {
	item := new (ContactItem)
	item.data = contact
	item.nextItem = nil
	return item
}

func NewBucket() *Bucket {
	//create a new bucket, head and tail start at 0
	bucket := new (Bucket)
	bucket.ItemCount = 0
	return bucket
}

func (b *Bucket) PingBucket(contact Contact) {
	//this bucket has been pinged by this contact
	newContact := b.BumpContactToBottom(contact)
	log.Printf("Did we find it? %v", newContact)
	if !newContact {
		//the contact isn't already here!
		(*b).AddContact(contact)
	}
}

func (b *Bucket) AddContact(contact Contact) {
//if the bucket isn't full add the contact and 
//put it at the bottom of the bucket. If it is full
//Then drop the top node
	if b.isFull() {
		b.Pop()
		b.Push(contact)
	} else {
		b.Push(contact)
	}
}

func (b *Bucket) BumpContactToBottom(contact Contact) bool{
	//return true if we did it, false if we didn't find it
	found, foundContact := b.ContainsNode(contact.NodeID)

	if found {
		//clip it from the linked list
		(*b).Push(foundContact.data) //add it to the end
		foundContact.data = foundContact.nextItem.data //now copy the next nodes data over
		foundContact.nextItem = foundContact.nextItem.nextItem //and clip it out of the loop
		(*b).ItemCount--
	}

	return found
}

func (b* Bucket) ContainsNode(nodeID ID) (bool,ContactItem) {
//Takes a node id and locates it in this bucket. Returns a contactitem
// instead of contact to support the BumpContactToBottomFunction
	var foundContact, tempContact ContactItem

	if b.isEmpty() {
		//don't bother if it's empty
		return false,foundContact
	}

	isFound := true
	//find the contact in the linked list
	tempContact = *(b.head)
	for isFound {
		if tempContact.data.NodeID == nodeID {
			foundContact = tempContact
			isFound = false
		}
		if tempContact.nextItem == nil {
			return false,foundContact
		}
		tempContact = *(tempContact.nextItem)
	}

	return true,foundContact

}

func (b *Bucket) Push(contact Contact) {
	//add item to the tail end
	item := NewContactItem(contact)

	if (*b).isEmpty() {
		(*b).head = item
		(*b).tail = item
	} else {
		(*b).tail.nextItem = item //point the tail here
		(*b).tail = item //make this item the tail
	}

	(*b).ItemCount++
	return
}

func (b *Bucket) Pop() Contact{
	//remove item from the head
	if (*b).isEmpty(){
		log.Fatal("Nothing to pop in queue")
	}

	oldHead := b.head.data
	if (*b).ItemCount == 1 { //this is the only thing in the bucket
		b.head = nil
	} else {
		tempNode := b.head.nextItem
		(*b).head = tempNode
	}

	(*b).ItemCount--
	return oldHead
}

func (b Bucket) isEmpty() bool {
	if b.ItemCount == 0 {
		return true
	}
	return false
}

func (b Bucket) isFull() bool {
	if b.ItemCount == bucketSize {
		return true
	}
	return false
}


