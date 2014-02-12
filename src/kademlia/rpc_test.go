package kademlia

import (
	"testing"
	"net"
)

func Test_Contact_GetAddress_1(t *testing.T) {
	contact := NewContact(NewRandomID(), net.ParseIP("1.2.3.4"), uint16(5678))
	if contact.GetAddress() != "1.2.3.4:5678" {
			t.Fail()
	}
}