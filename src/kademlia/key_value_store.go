package kademlia



type KeyValueStore struct {
	keyValuePairs map[ID][]byte
}

func NewKeyValueStore() *KeyValueStore {
	newKeyValueStore := new(KeyValueStore)
	newKeyValueStore.keyValuePairs = make(map[ID][]byte)
	return newKeyValueStore
}

func (store *KeyValueStore) InsertValue(newKey ID, newValue []byte) {
	store.keyValuePairs[newKey] = newValue
}

//Returns nil if not in store
func (store *KeyValueStore) RetrieveValue(key ID) []byte {
	return store.keyValuePairs[key]
}

