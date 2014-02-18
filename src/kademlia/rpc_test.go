package kademlia

import (
	//"net"
	"testing"
)

func Test_Contact_GetAddress_1(t *testing.T) {
	/*contact := NewContact(NewRandomID(), net.ParseIP("1.2.3.4"), uint16(5678))
	if contact.GetAddress() != "1.2.3.4:5678" {
			t.Fail()
	}*/
}

func Test_Contact_NewContactFromAddressString(t *testing.T) {
	contact, error := NewContactFromAddressString("localhost:1234")
	if error != nil {
		t.Error(error)
	}
	if !contact.Host.IsLoopback() {
		t.Fail()
	}
	if contact.Port != uint16(1234) {
		t.Fail()
	}

}
