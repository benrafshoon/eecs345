package kademlia
// Contains the implementation of a kbucket

//need 160 buckets for 160 bit keys
const bucketSize = 160

type kBucket struct {
	keys [bucketSize]int
}

func NewBucket() *kBucket {
//create a new bucket
}

func MoveToBottom(key int) {
//contact has pinged us
}

func Push(key int) {
//if the bucket isn't full add the key and 
//put it at the bottom of the bucket. If it is full
//Then drop the top node

}

func Pop(key int) {
//remove the node at the top of the bucket
}