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
