package kademlia

import (
	"log"
)

type KeyValueStore struct {
	keyValuePairs map[ID][]byte
	requests chan keyValueStoreRequest
}

type keyValueStoreRequest interface {
	RequestType() string
}

func NewKeyValueStore() *KeyValueStore {
	newKeyValueStore := new(KeyValueStore)
	newKeyValueStore.keyValuePairs = make(map[ID][]byte)

	newKeyValueStore.requests = make(chan keyValueStoreRequest)
	go newKeyValueStore.processKeyValueStoreRequests()

	return newKeyValueStore
}


//Thread-safe
func (store *KeyValueStore) InsertValue(newKey ID, newValue []byte) {
	request := insertValueRequest{newKey, newValue}
	store.requests <- request
}

//Thread-safe
//Returns nil if not in store
func (store *KeyValueStore) RetrieveValue(key ID) []byte {
	request := retrieveValueRequest{key, make(chan retrieveValueResult)}
	store.requests <- request
	response := <- request.result
	return response.value
}



func (store *KeyValueStore) processKeyValueStoreRequests() {
	for {
		request := <- store.requests
		switch request.RequestType() {
		case "InsertValueRequest":
			store.insertValueInternal(request.(insertValueRequest))
		case "RetrieveValueRequest":
			store.retrieveValueInternal(request.(retrieveValueRequest))
		default:
			log.Printf("Invalid request to key value store")
		}

	}
}

type insertValueRequest struct {
	newKey ID
	newValue []byte
}

func (r insertValueRequest) RequestType() string {
	return "InsertValueRequest"
}

func (store *KeyValueStore) insertValueInternal(request insertValueRequest) {
	store.keyValuePairs[request.newKey] = request.newValue
}

type retrieveValueRequest struct {
	key ID
	result chan retrieveValueResult
}

func (r retrieveValueRequest) RequestType() string {
	return "RetrieveValueRequest"
}

type retrieveValueResult struct {
	value []byte
}

func (store *KeyValueStore) retrieveValueInternal(request retrieveValueRequest) {
	request.result <- retrieveValueResult{store.keyValuePairs[request.key]}
}