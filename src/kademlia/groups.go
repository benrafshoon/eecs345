package kademlia

import (
	"container/list"
	"crypto/sha1"
	"fmt"
	"log"
	"net/rpc"
	"time"
)

type Group struct {
	Name              string
	GroupID           ID
	RendezvousPoint   *Contact
	Parent            *Contact
	Children          *list.List
	Member            bool
	IsRendezvousPoint bool
	Messages          *list.List
}

type Message struct {
	Message  string
	Username string
	Time     time.Time
	Order    int
	//Time field is NOT a global clock, this is when the user PERCEIVES that they sent a message
	//For UI only, not to be used for causality/ordering
}

type GetAllMessagesRequest struct {
	Sender          Contact
	MsgID           ID
	GroupID         ID
}

type GetAllMessagesResponse struct {
	MsgID			ID
	Messages 		*list.List
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
	group.IsRendezvousPoint = false
	group.Messages = list.New()
	return group
}

func (g *Group) PrintGroup() {
	if g.Name != "" {
		log.Printf("Group %s - %s", g.GroupID.AsString(), g.Name)
	} else {
		log.Printf("Group %s", g.GroupID.AsString())
	}
	if g.Member {
		log.Printf("        Member")
	} else {
		log.Printf("        Not member")
	}
	if g.IsRendezvousPoint {
		log.Printf("        Is Rendezvous Point")
	}
	if g.RendezvousPoint != nil {
		log.Printf("        Rendezvous Point: %s", g.RendezvousPoint.NodeID.AsString())
	} else {
		log.Printf("        Rendezvous Point Unknown")
	}
	if g.Parent != nil {
		log.Printf("        Parent: %s", g.Parent.NodeID.AsString())
	} else {
		log.Printf("        Parent Unknown")
	}

	child := g.Children.Front()
	for child != nil {
		log.Printf("        Child: %s", child.Value.(*Contact).NodeID.AsString())
		child = child.Next()
	}
}

func (g *Group) RemoveChild(toRemove *Contact) bool {
	current := g.Children.Front()
	for current != nil {
		child := current.Value.(*Contact)
		if child.Equals(toRemove) {
			g.Children.Remove(current)
			return true
		}
		current = current.Next()
	}
	return false
}

//Primitive receives

//Returns the group, and whether the group is new (true) or if it already existed (false)
func (k *Kademlia) AddGroupWithName(name string) (*Group, bool) {
	namedGroupID := groupID(name)
	group, groupAlreadyAdded := k.Groups[namedGroupID.AsString()]
	if !groupAlreadyAdded {
		log.Printf("Creating new group named %s with id %s", name, namedGroupID.AsString())
		group := newGroup()
		group.Name = name
		group.GroupID = namedGroupID
		k.Groups[namedGroupID.AsString()] = group
		return group, true
	} else {
		log.Printf("Group already exists")
		return group, false
	}
}

func (k *Kademlia) AddGroupWithGroupID(groupID ID) (*Group, bool) {
	group, groupAlreadyAdded := k.Groups[groupID.AsString()]
	if !groupAlreadyAdded {
		log.Printf("Creating new group")
		group := newGroup()
		group.GroupID = groupID
		k.Groups[groupID.AsString()] = group
		return group, true
	} else {
		log.Printf("Group already exists")
		return group, false
	}
}

func (k *Kademlia) FindGroupWithGroupID(groupID ID) (bool, *Group) {
	group, groupExists := k.Groups[groupID.AsString()]
	if groupExists {
		return true, group
	} else {
		return false, nil
	}
}

func (k *Kademlia) FindGroupWithName(name string) (bool, *Group) {
	group, groupExists := k.Groups[groupID(name).AsString()]
	if groupExists {
		return true, group
	} else {
		return false, nil
	}
}

type CreateGroupRequest struct {
	Sender   Contact
	HasChild bool
	Child    Contact
	MsgID    ID
	GroupID  ID
}

type CreateGroupResult struct {
	MsgID ID
}

//Similar to scribe create
func (k *Kademlia) CreateGroup(req CreateGroupRequest, res *CreateGroupResult) error {
	log.Printf("Create group %s", req.GroupID.AsString())
	group, _ := k.AddGroupWithGroupID(req.GroupID)
	group.IsRendezvousPoint = true
	group.RendezvousPoint = k.RoutingTable.SelfContact
	group.Parent = k.RoutingTable.SelfContact
	if req.HasChild {
		group.Children.PushFront(&req.Child)
	}
	group.PrintGroup()
	res.MsgID = req.MsgID
	return nil
}

func (k *Kademlia) CheckForLostMessages(req GetAllMessagesRequest, res *GetAllMessagesResponse) error {
	log.Printf("checkingforlostmessages")
	didFindGroup, group := k.FindGroupWithGroupID(req.GroupID)
	if didFindGroup {
		res.Messages = group.Messages
	}
	res.MsgID = req.MsgID
	log.Printf("returning!")
	return nil
}

type AddPathToGroupRequest struct {
	Sender          Contact
	MsgID           ID
	GroupID         ID
	Child           Contact
	HasParent       bool
	Parent          Contact
	RendezvousPoint Contact
	GroupName       string
}

type AddPathToGroupResponse struct {
	MsgID                           ID
	AlreadyHasPathToRendezvousPoint bool
}

//Similar to scribe join
func (k *Kademlia) AddPathToGroup(req AddPathToGroupRequest, res *AddPathToGroupResponse) error {
	group, _ := k.AddGroupWithGroupID(req.GroupID)
	log.Printf("Adding forwarding entry to group %s", group.GroupID.AsString())
	group.Children.PushBack(&req.Child)
	group.RendezvousPoint = &req.RendezvousPoint
	if group.Parent == nil {
		if req.HasParent {
			group.Parent = &req.Parent
			k.CheckForHeartbeat(group.Parent, req.GroupName)
			res.AlreadyHasPathToRendezvousPoint = false
			log.Printf("  Adding parent %s - %d", req.Parent.NodeID.AsString(), req.Parent.NodeID.DistanceBucket(group.GroupID))
		}
	} else {
		res.AlreadyHasPathToRendezvousPoint = true
		log.Printf("  Existing parent %s - %d", group.Parent.NodeID.AsString(), group.Parent.NodeID.DistanceBucket(group.GroupID))
	}
	log.Printf("  New Child %s - %d", req.Child.NodeID.AsString(), req.Child.NodeID.DistanceBucket(group.GroupID))

	res.MsgID = req.MsgID
	return nil
}

type BroadcastMessageRequest struct {
	Sender  Contact
	MsgID   ID
	GroupID ID
	Message Message
}

type BroadcastMessageResponse struct {
	MsgID ID
}

func (k *Kademlia) BroadcastMessage(req BroadcastMessageRequest, res *BroadcastMessageResponse) error {
	foundGroup, group := k.FindGroupWithGroupID(req.GroupID)
	if foundGroup {
		log.Printf("Recevied broadcast message %s", req.Message)
		if group.IsRendezvousPoint {
			current := group.Children.Front()
			if current != nil {
				child := current.Value.(*Contact)
				//k.SendCheckForLostMessages(req.GroupID, child)
			}
			lastMessage := group.Messages.Front()
			messageNumber := 0
			if(lastMessage!=nil) {
				messageNumber = lastMessage.Value.(Message).Order + 1
			}
			req.Message.Order = messageNumber //assign it a number
		}
		if group.IsRendezvousPoint || req.Sender.Equals(group.Parent) {
			if group.Member {
				group.Messages.PushFront(req.Message)
				fmt.Printf("%s %s - %s\n Message Number:%i \n", req.Message.Time.Format("3:04pm"), req.Message.Username, req.Message.Message, req.Message.Order)
			}
			current := group.Children.Front()
			for current != nil {
				child := current.Value.(*Contact)
				k.SendBroadcastMessage(child, group, req.Message)
				current = current.Next()
			}
		} else {
			log.Printf("Received broadcast request from non-parent %s - %i", req.Sender.NodeID.AsString(), req.Sender.NodeID.DistanceBucket(group.GroupID))
		}
	} else {
		log.Printf("Received broadcast message for group for which not a forwarder %s", req.GroupID)
	}
	res.MsgID = req.MsgID
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
	log.Printf("Removing %s from group %s", req.Sender.NodeID.AsString(), req.GroupID.AsString())
	didFindGroup, group := k.FindGroupWithGroupID(req.GroupID)
	if didFindGroup {
		wasInGroup := group.RemoveChild(&req.Sender)
		if !wasInGroup {
			log.Printf("Tried to remove child that was not in group")
		}
		//Only leave if a forwarder and have no children to forward to, or not receiving messages from the group (a member)
		if group.Children.Len() == 0 && !group.Member {
			k.SendLeaveGroup(k.RoutingTable.SelfContact, group)
		}
	}
	return nil
}

//Primitive sends

func (k *Kademlia) SendCreateGroup(group *Group, child *Contact) {
	rvPoint := group.RendezvousPoint
	log.Printf("Sending create group to %v\n", k.GetContactAddress(rvPoint))
	client, err := rpc.DialHTTP("tcp", k.GetContactAddress(rvPoint))
	if err != nil {
		log.Printf("Connection error")
		return
	}

	req := new(CreateGroupRequest)
	req.Sender = *k.RoutingTable.SelfContact
	req.MsgID = NewRandomID()
	req.GroupID = group.GroupID
	req.HasChild = false
	if child != nil {
		req.HasChild = true
		req.Child = *child
	}
	var res CreateGroupResult

	err = client.Call("Kademlia.CreateGroup", req, &res)
	if err != nil {
		log.Printf("Error in remote node response")
		return
	}

	log.Printf("Received response to CreateGroup request from %v:%v\n", rvPoint.Host, rvPoint.Port)
	log.Printf("          Node ID: %v\n", rvPoint.NodeID.AsString())

	client.Close()
}

//Should return an error
func (k *Kademlia) SendAddPathToGroup(group *Group, contact *Contact, child *Contact, parent *Contact, rendezvousPoint *Contact) bool {

	log.Printf("Sending add path to group to %v\n", k.GetContactAddress(contact))
	client, err := rpc.DialHTTP("tcp", k.GetContactAddress(contact))
	if err != nil {
		log.Printf("Connection error")
		return false
	}

	req := new(AddPathToGroupRequest)
	req.Sender = *k.RoutingTable.SelfContact
	req.MsgID = NewRandomID()
	req.GroupID = group.GroupID
	req.Child = *child
	req.RendezvousPoint = *rendezvousPoint
	req.GroupName = group.Name
	if parent == nil {
		req.HasParent = false
	} else {
		req.HasParent = true
		req.Parent = *parent
	}
	var res AddPathToGroupResponse

	err = client.Call("Kademlia.AddPathToGroup", req, &res)
	if err != nil {
		log.Printf("Error in remote node response")
		return false
	}

	log.Printf("Received response to AddPathToGroup request from %v:%v\n", contact.Host, contact.Port)
	log.Printf("          Node ID: %v\n", contact.NodeID.AsString())

	client.Close()
	return res.AlreadyHasPathToRendezvousPoint
}

func (k *Kademlia) SendBroadcastMessage(contact *Contact, group *Group, message Message) {
	log.Printf("Sending message to %v\n", k.GetContactAddress(contact))
	client, err := rpc.DialHTTP("tcp", k.GetContactAddress(contact))
	if err != nil {
		log.Printf("Connection error")
		return
	}

	req := new(BroadcastMessageRequest)
	req.Sender = *k.RoutingTable.SelfContact
	req.MsgID = NewRandomID()
	req.GroupID = group.GroupID
	req.Message = message
	var res BroadcastMessageResponse

	err = client.Call("Kademlia.BroadcastMessage", req, &res)
	if err != nil {
		log.Printf("Error in remote node response")
		return
	}

	log.Printf("Received response to BroadcastMessage from %v:%v\n", contact.Host, contact.Port)
	log.Printf("          Node ID: %v\n", contact.NodeID.AsString())

	client.Close()
}

func (k *Kademlia) SendLeaveGroup(contact *Contact, group *Group) {
	log.Printf("Sending leave group to %v\n", k.GetContactAddress(contact))
	client, err := rpc.DialHTTP("tcp", k.GetContactAddress(contact))
	if err != nil {
		log.Printf("Connection error")
		return
	}

	req := new(LeaveGroupRequest)
	req.Sender = *k.RoutingTable.SelfContact
	req.MsgID = NewRandomID()
	req.GroupID = group.GroupID

	var res LeaveGroupResponse

	err = client.Call("Kademlia.LeaveGroup", req, &res)
	if err != nil {
		log.Printf("Error in remote node response")
		return
	}

	log.Printf("Received response to LeaveGroup from %v:%v\n", req.Sender.Host, req.Sender.Port)
	log.Printf("          Node ID: %v\n", req.Sender.NodeID.AsString())

	client.Close()
}

//Compound sends

func (k *Kademlia) DoCreateGroup(groupName string) {
	group, isNewGroup := k.AddGroupWithName(groupName)
	if isNewGroup {
		result := k.iterativeOperation(group.GroupID, iterativeFindNodeOperation)
		group.RendezvousPoint = result.Path.Back().Value.(*Contact)
		k.SendCreateGroup(group, nil)
	}
	group.PrintGroup()
}

func (k *Kademlia) DoJoinGroup(groupName string) {
	group, _ := k.AddGroupWithName(groupName)
	group.Member = true
	if group.IsRendezvousPoint || group.Parent != nil {
		//We already have a path to the RV point
		log.Printf("Already have path %s", groupName)
	} else {
		result := k.iterativeOperation(group.GroupID, iterativeFindNodeOperation)

		group.RendezvousPoint = result.Path.Back().Value.(*Contact)

		//Assume path has at least one entry (self)
		previous := result.Path.Front()
		current := previous.Next() //1st in list is self, no need to do anything with that
		completePath := false

		for current != nil && !completePath {
			next := current.Next()
			var nextNodeInPath *Contact = nil
			if next != nil {
				nextNodeInPath = next.Value.(*Contact)
			}
			previousNodeInPath := previous.Value.(*Contact)
			nodeInPath := current.Value.(*Contact)
			if group.Parent == nil {
				//If there is no parent, this will be filed on the first loop iteartion
				//If there is already a parent, we will never reach this branch
				group.Parent = nodeInPath
				k.CheckForHeartbeat(group.Parent, groupName)
			}
			//Send request to current to add previous to children and next to parent
			//If current already has a path to the rv point, completePath will become true and the loop will terminate
			completePath = k.SendAddPathToGroup(group, nodeInPath, previousNodeInPath, nextNodeInPath, group.RendezvousPoint)
			previous = current
			current = next
		}
	}
	group.PrintGroup()

}

func (k *Kademlia) DoBroadcastMessage(groupName string, message string) bool {
	didFindGroup, group := k.FindGroupWithName(groupName)
	if didFindGroup && group.Member && group.RendezvousPoint != nil {
		messageStruct := Message{Message: message, Username: "A User", Time: time.Now()}
		k.SendBroadcastMessage(group.RendezvousPoint, group, messageStruct)
	}
	return didFindGroup && group.Member
}

func (k *Kademlia) DoLeaveGroup(groupName string) {
	didFindGroup, group := k.FindGroupWithName(groupName)
	if didFindGroup {
		group.Member = false
		if !group.IsRendezvousPoint && group.Children.Len() == 0 {
			k.SendLeaveGroup(group.Parent, group)
			group.Parent = nil //Delete path to parent since parent no longer sends messages to self after sending LeaveGroup request
		}
		group.PrintGroup()
	}
}

func (k *Kademlia) CheckForHeartbeat(parent *Contact, groupName string) {
	if(parent!=nil) { //shouldn't happen but safety first!
		//We want to check to make sure our parent is still alive
		go func(parent *Contact) { //do this away from the main thread
			up := true
			for up { //infinite loop
				time.Sleep(5 * time.Second) //Let's wait initially, when we start it likely will stay up
				log.Printf("Heartbeat: %s", parent.NodeID.AsString())
				_, error := k.SendPing(parent); //Check if it's up
				
				if (error!=nil) { //rut-roh 
					up = false //stop looping
					
					didFindGroup, group := k.FindGroupWithName(groupName)
					if didFindGroup {
						group.Parent = nil //signal we need a new parent
						k.DoJoinGroup(groupName) //rejoin the group
					}
				}
			}
		}(parent)
	}
}

func (k *Kademlia) SendCheckForLostMessages(groupID ID, child *Contact) {

	log.Printf("\n\nSending check for lost messages %v\n", k.GetContactAddress(child))
	client, err := rpc.DialHTTP("tcp", k.GetContactAddress(child))
	if err != nil {
		log.Printf("Connection error")
		return
	}

	req := new(GetAllMessagesRequest)
	req.Sender = *k.RoutingTable.SelfContact
	req.MsgID = NewRandomID()
	req.GroupID = groupID

	var res GetAllMessagesResponse

	err = client.Call("Kademlia.CheckForLostMessages", req, &res)
	if err != nil {
		log.Printf("Error in remote node response: %s", err)
		return
	}

	client.Close()

	log.Printf("leaving the call")
	didFindGroup, group := k.FindGroupWithGroupID(groupID)
	if didFindGroup {
		highestNum := res.Messages.Front().Value.(Message).Order
		ourHighestNum := group.Messages.Front().Value.(Message).Order
		if(highestNum > ourHighestNum) {
			group.Messages = res.Messages
		}
	}

	return
}
