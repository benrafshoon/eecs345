package kademlia

import (
	"container/list"
	"crypto/sha1"
	"log"
	"net/rpc"
)

type Group struct {
	Name            string
	GroupID         ID
	RendezvousPoint *Contact
	Parent          *Contact
	Children        *list.List
	Member          bool
}

//Parent == nil implies RV point

func groupID(name string) ID {
	h := sha1.New()
	groupIDSlice := h.Sum([]byte(name))
	var groupID ID
	for i := 0; i < IDBytes; i++ {
		groupID[i] = groupIDSlice[i]
	}
	return groupID
}
func newGroup() *Group {
	group := new(Group)
	group.Name = ""
	group.Parent = nil
	group.Children = list.New()
	group.Member = false
	group.RendezvousPoint = nil
	return group
}

//Returns the group, and whether the group is new (true) or if it already existed (false)
func (k *Kademlia) AddGroupWithName(name string) (*Group, bool) {
	namedGroupID := groupID(name)
	if k.Groups[namedGroupID] == nil {
		log.Printf("Creating new group")
		group := newGroup()
		group.Name = name
		group.GroupID = namedGroupID
		return group, true
	} else {
		log.Printf("Group already exists")
		return k.Groups[namedGroupID], false
	}
}

func (k *Kademlia) AddGroupWithGroupID(groupID ID) (*Group, bool) {
	if k.Groups[groupID] == nil {
		log.Printf("Creating new group")
		group := newGroup()
		group.GroupID = groupID
		return group, true
	} else {
		log.Printf("Group already exists")
		return k.Groups[groupID], false
	}
}

func (g *Group) PrintGroup() {
	if g.Name != "" {
		log.Printf("Group %s - %s", g.GroupID.AsString(), g.Name)
	} else {
		log.Printf("Group %s", g.GroupID.AsString())
	}
	if g.Parent == nil {
		log.Printf("        Rendezvous Point")
	} else {
		log.Printf("        Parent: %s", g.Parent.NodeID.AsString())
	}
	child := g.Children.Front()
	for child != nil {
		log.Printf("        Child: %s", child.Value.(*Contact).NodeID.AsString())
		child = child.Next()
	}
}

type CreateGroupRequest struct {
	Sender  Contact
	MsgID   ID
	GroupID ID
}

type CreateGroupResult struct {
	MsgID ID
}

//Similar to scribe create
func (k *Kademlia) CreateGroup(req CreateGroupRequest, res *CreateGroupResult) error {
	if k.Groups[req.GroupID] == nil {
		log.Printf("Create new group")
		newGroup := newGroup()
		newGroup.GroupID = req.GroupID
		k.Groups[req.GroupID] = newGroup
	} else {
		log.Printf("Create existing group")
	}
	k.Groups[req.GroupID].PrintGroup()
	res.MsgID = req.MsgID
	return nil
}

type AddPathToGroupRequest struct {
	Sender  Contact
	MsgID   ID
	GroupID ID
	Child   Contact
	Parent  Contact
}

type AddPathToGroupResponse struct {
	MsgID      ID
	IsNewGroup bool
}

//Similar to scribe join
func (k *Kademlia) AddPathToGroup(req AddPathToGroupRequest, res *AddPathToGroupResponse) error {
	return nil
}

type BroadcastMessageRequest struct {
	Sender  Contact
	MsgID   ID
	GroupID ID
	Message string
}

type BroadcastMessageResponse struct {
	MsgID ID
}

func (k *Kademlia) BroadcastMessage(req BroadcastMessageRequest, res *BroadcastMessageResponse) error {
	return nil
}

type LeaveGroupRequest struct {
	Sender  Contact
	MsgID   ID
	GroupID ID
}

type LeaveGroupResponse struct {
	MsgID ID
}

func (k *Kademlia) LeaveGroup(req LeaveGroupRequest, res *LeaveGroupResponse) error {
	return nil
}

func (k *Kademlia) SendCreateGroup(name string) {
	group, isNewGroup := k.AddGroupWithName(name)
	if isNewGroup {
		result := k.iterativeOperation(group.GroupID, iterativeFindNodeOperation)
		closestNode := result.Path.Back().Value.(*Contact)

		log.Printf("Sending create group to %v\n", k.GetContactAddress(closestNode))
		client, err := rpc.DialHTTP("tcp", k.GetContactAddress(closestNode))
		if err != nil {
			log.Printf("Connection error")
			return
		}

		req := new(CreateGroupRequest)
		req.Sender = *k.RoutingTable.SelfContact
		req.MsgID = NewRandomID()
		req.GroupID = group.GroupID
		var res CreateGroupResult

		err = client.Call("Kademlia.CreateGroup", req, &res)
		if err != nil {
			log.Printf("Error in remote node response")
			return
		}

		log.Printf("Received response from %v:%v\n", req.Sender.Host, req.Sender.Port)
		log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())

		client.Close()
	}
}
