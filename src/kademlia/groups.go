package kademlia

import (
	"container/list"
	"crypto/sha1"
)

type Group struct {
	Name     string
	GroupID  ID
	Parent   *Contact
	Children *list.List
	Member   bool
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

func newGroup(name string) *Group {
	group := new(Group)
	group.Name = name
	group.GroupID = groupID(name)
	group.Parent = nil
	group.Children = list.New()
	group.Member = false
	return group
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
	return nil
}

type AddPathToGroupRequest struct {
}

type AddPathToGroupResponse struct {
}

//Similar to scribe join
func (k *Kademlia) AddPathToGroup(req AddPathToGroupRequest, res *AddPathToGroupResponse) error {
	return nil
}

type BroadcastMessageRequest struct {
}

type BroadcastMessageResponse struct {
}

func (k *Kademlia) BroadcastMessage(req BroadcastMessageRequest, res *BroadcastMessageResponse) error {
	return nil
}

type LeaveGroupRequest struct {
}

type LeaveGroupResponse struct {
}

func (k *Kademlia) LeaveGroup(req LeaveGroupRequest, res *LeaveGroupResponse) error {
	return nil
}
