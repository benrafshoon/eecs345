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

func (wrapper *KademliaRPCWrapper) CreateGroup(req CreateGroupRequest, res *CreateGroupResult) error {
	return wrapper.kademlia.CreateGroup(req, res)
}

func (wrapper *KademliaRPCWrapper) AddPathToGroup(req AddPathToGroupRequest, res *AddPathToGroupResponse) error {
	return wrapper.kademlia.AddPathToGroup(req, res)
}

func (wrapper *KademliaRPCWrapper) BroadcastMessage(req BroadcastMessageRequest, res *BroadcastMessageResponse) error {
	return wrapper.kademlia.BroadcastMessage(req, res)
}

func (wrapper *KademliaRPCWrapper) LeaveGroup(req LeaveGroupRequest, res *LeaveGroupResponse) error {
	return wrapper.kademlia.LeaveGroup(req, res)
}

func (wrapper *KademliaRPCWrapper) CheckForLostMessages(req GetAllMessagesRequest, res *GetAllMessagesResponse) error {
	return wrapper.kademlia.CheckForLostMessages(req, res)
}
