package kademlia

type KademliaRPCWrapper struct {
	kademlia *Kademlia
}

func NewKademliaRPCWrapper(kademlia *Kademlia) *KademliaRPCWrapper {
	wrapper := new(KademliaRPCWrapper)
	wrapper.kademlia = kademlia
	return wrapper
}

func (wrapper *KademliaRPCWrapper) Ping(ping Ping, pong *Pong) error {
	return wrapper.kademlia.Ping(ping, pong)
}

func (wrapper *KademliaRPCWrapper) Store(req StoreRequest, res *StoreResult) error {
	return wrapper.kademlia.Store(req, res)
}

func (wrapper *KademliaRPCWrapper) FindNode(req FindNodeRequest, res *FindNodeResult) error {
	return wrapper.kademlia.FindNode(req, res)
}

func (wrapper *KademliaRPCWrapper) FindValue(req FindValueRequest, res *FindValueResult) error {
	return wrapper.kademlia.FindValue(req, res)
}