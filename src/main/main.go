package main

import (
	"bufio"
	"flag"
	"fmt"
	//"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

import (
	"kademlia"
)

func main() {
	// By default, Go seeds its RNG with 1. This would cause every program to
	// generate the same sequence of IDs.
	rand.Seed(time.Now().UnixNano())
	//log.SetOutput(ioutil.Discard)
	// Get the bind and connect connection strings from command-line arguments.

	name := flag.String("name", "untitled", "Enter username")
	testNodeIDString := flag.String("id", "", "Optionally specify the node ID for testing")
	listenStr := flag.String("listen", "", "Enter ip address to listen on")
	firstPeerStr := flag.String("first-peer", "", "Enter ip address of first peer")

	flag.Parse()

	log.Printf(*name)

	if *listenStr == "" {
		fmt.Println("Must enter ip address to listen on")
		return
	}

	if *firstPeerStr == "" {
		fmt.Println("Must enter ip address of first peer")
	}

	var kademliaServer *kademlia.Kademlia

	if *testNodeIDString != "" {
		testNodeID, error := kademlia.FromString(*testNodeIDString)
		if error != nil {
			log.Fatal("Error parsing test node ID: ", error)
		}
		kademliaServer = kademlia.NewTestKademlia(testNodeID)

	} else {
		kademliaServer = kademlia.NewKademlia()
	}

	error := kademliaServer.StartKademliaServer(*listenStr)

	if error != nil {
		log.Fatal("Error starting kademlia server: ", error)
	}

	firstPeer, error := kademlia.NewContactFromAddressString(*firstPeerStr)
	if error != nil {
		log.Fatal("Error parsing first contact ", error)
	}

	error = kademliaServer.InitializeRoutingTable(firstPeer)
	if error != nil {
		log.Fatal("Error initializing routing table: ", error)
	}

	in := bufio.NewReader(os.Stdin)

	quit := false
	for !quit {

		input, err := in.ReadString('\n')
		if err != nil {
			//Still do the last command, but then quit
			quit = true

		}
		input = strings.Replace(input, "\n", "", -1) //use this as our end of input so remove it here
		command := strings.Split(input, " ")
		switch command[0] {
		case "whoami":
			fmt.Printf("%v\n", kademliaServer.GetNodeID().AsString())
		case "local_find_value":
			if len(command) < 2 {
				fmt.Printf("Error in command \"local_find_value\": must enter key, command must be of the form \"local_find_value key\"\n")
				continue
			}
			id, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"local_find_value\": %v\n", error)
				continue
			}

			log.Printf("Finding local value for key %v", id.AsString())
			value := kademliaServer.Data.RetrieveValue(id)
			if value == nil {
				fmt.Printf("ERR\n")
				continue
			}
			fmt.Printf("%v\n", string(value))

		case "get_contact":
			if len(command) < 2 {
				fmt.Printf("Error in command \"get_contact\": must enter ID, command must be of the form \"get_contact ID\"\n")
				continue
			}
			id, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"get_contact\": %v\n", error)
				continue
			}

			hasContact, isSelf, contact := kademliaServer.RoutingTable.LookupContactByNodeID(id)
			if !hasContact {
				fmt.Printf("ERR\n")
				continue
			}
			if isSelf {
				log.Printf("Self Contact")
				fmt.Printf("ERR\n")
				continue
			}
			fmt.Printf("%v %v\n", contact.Host, contact.Port)

		case "iterativeStore":
			if len(command) < 3 {
				fmt.Printf("Error in command \"store\": command must be of the form \"store key value\"\n")
				continue
			}
			key, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"store\": key: %v\n", error)
				continue
			}
			value := []byte(strings.SplitAfterN(input, " ", 3)[2])
			if len(value) > 4095 {
				value = value[0:4094]
			}
			log.Printf("Store %v <- %v\n", key.AsString(), string(value))
			finalContact, error := kademliaServer.SendIterativeStore(key, value)
			if error != nil {
				fmt.Printf("ERR\n")
				continue
			}
			fmt.Printf("%v\n", finalContact.NodeID.AsString())

		case "iterativeFindNode":
			if len(command) < 2 {
				fmt.Printf("Error in command \"iterativeFindNode\": command must be of the form \"iterativeFindNode nodeID\"\n")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"iterativeFindNode\": nodeID: %v\n", error)
				continue
			}

			log.Printf("Finding nodes close to %v", nodeID.AsString())

			error, foundContacts := kademliaServer.SendIterativeFindNode(nodeID)
			if error != nil {
				log.Printf("%v", error)
				fmt.Printf("ERR\n")
				continue
			}

			foundIDs := make([]string, len(foundContacts), len(foundContacts))

			log.Printf("NodeIDs found: ")

			for i := 0; i < len(foundContacts); i++ {
				foundIDs[i] = foundContacts[i].NodeID.AsString()
			}

			fmt.Printf("%v\n", foundIDs)

		case "iterativeFindValue":
			if len(command) < 2 {
				fmt.Printf("Error in command \"iterativeFindValue\": command must be of the form \"iterativeFindValue key\"\n")
				continue
			}
			key, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"iterativeFindValue\": key: %v\n", error)
				continue
			}

			log.Printf("Finding value with key %v", key.AsString())

			error, contactThatFoundValue, foundValue, _ := kademliaServer.SendIterativeFindValue(key)
			if error != nil {
				log.Printf("%v", error)
				fmt.Printf("ERR\n")
				continue
			}
			if foundValue != nil {
				log.Printf("Found value: ")
				fmt.Printf("%v %v\n", contactThatFoundValue.NodeID.AsString(), string(foundValue))
			} else {
				fmt.Printf("ERR\n")
			}

		case "ping":
			if len(command) < 2 {
				fmt.Printf("Error in command \"ping\": must enter address or node if, command must be of the form \"ping nodeID\" or \"ping host:port\"\n")
				continue
			}
			if firstContact, error := kademlia.NewContactFromAddressString(command[1]); error == nil {
				//ping host:port
				_, error := kademliaServer.SendPing(firstContact)
				if error != nil {
					log.Println(error)
					fmt.Printf("ERR\n")
				}
			} else {
				//ping nodeID
				id, error := kademlia.FromString(command[1])
				if error != nil {
					fmt.Printf("Error in command \"ping\": nodeID: %v\n", error)
					continue
				}

				hasContact, isSelf, contact := kademliaServer.RoutingTable.LookupContactByNodeID(id)
				if !hasContact {
					log.Printf("Contact not found")
					fmt.Printf("ERR\n")
					continue
				}
				if isSelf {
					log.Printf("Self contact")
				}
				_, error = kademliaServer.SendPing(contact)
				if error != nil {
					log.Printf("Error: ", error)
					fmt.Printf("ERR\n")
				}
			}
		case "store":
			if len(command) < 4 {
				fmt.Printf("Error in command \"store\": command must be of the form \"store nodeID key value\"\n")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"store\": nodeID: %v\n", error)
				continue
			}
			key, error := kademlia.FromString(command[2])
			if error != nil {
				fmt.Printf("Error in command \"store\": key: %v\n", error)
				continue
			}
			value := []byte(strings.SplitAfterN(input, " ", 4)[3])
			if len(value) > 4095 {
				value = value[0:4094]
			}
			log.Printf("Store in node %v: %v <- %v\n", nodeID.AsString(), key.AsString(), string(value))
			hasContact, isSelf, contact := kademliaServer.RoutingTable.LookupContactByNodeID(nodeID)
			if hasContact {
				if isSelf {
					log.Printf("Storing in self")
				}
				kademliaServer.SendStore(contact, key, value)
				fmt.Printf("\n")
			} else {
				fmt.Printf("ERR\n")
			}
		case "find_node":
			if len(command) < 3 {
				fmt.Printf("Error in command \"find_node\": command must be of the form \"find_node nodeID key\"\n")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"find_node\": nodeID: %v\n", error)
				continue
			}

			nodeToFind, error := kademlia.FromString(command[2])
			if error != nil {
				fmt.Printf("Error in command \"find_node\": key: %v\n", error)
				continue
			}
			log.Printf("Finding nodes close to %v from node %v", nodeToFind.AsString(), nodeID.AsString())

			hasContact, _, contact := kademliaServer.RoutingTable.LookupContactByNodeID(nodeID)
			if !hasContact {
				fmt.Printf("ERR\n")
				continue
			}
			error, foundContacts := kademliaServer.SendFindNode(contact, nodeToFind)
			if error != nil {
				log.Printf("%v", error)
				fmt.Printf("ERR\n")
				continue
			}

			foundIDs := make([]string, len(foundContacts), len(foundContacts))

			log.Printf("NodeIDs found: ")

			for i := 0; i < len(foundContacts); i++ {
				foundIDs[i] = foundContacts[i].NodeID.AsString()
			}

			fmt.Printf("%v\n", foundIDs)

		case "find_value":
			if len(command) < 3 {
				fmt.Printf("Error in command \"find_value\": command must be of the form \"find_node nodeID key\"\n")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				fmt.Printf("Error in command \"find_value\": nodeID: %v\n", error)
				continue
			}

			key, error := kademlia.FromString(command[2])
			if error != nil {
				fmt.Printf("Error in command \"find_value\": key: %v\n", error)
				continue
			}
			log.Printf("Finding value with key %v from node %v", key, nodeID.AsString())

			hasContact, _, contact := kademliaServer.RoutingTable.LookupContactByNodeID(nodeID)
			if !hasContact {
				fmt.Printf("ERR\n")
				continue
			}
			error, foundValue, foundContacts := kademliaServer.SendFindValue(contact, key)
			if error != nil {
				log.Printf("%v", error)
				fmt.Printf("ERR\n")
				continue
			}
			if foundValue != nil {
				log.Printf("Found value: ")
				fmt.Printf("%v %v\n", nodeID.AsString(), string(foundValue))
			} else {
				foundIDs := make([]string, len(foundContacts), len(foundContacts))

				log.Printf("No value, but NodeIDs found: ")

				for i := 0; i < len(foundContacts); i++ {
					foundIDs[i] = foundContacts[i].NodeID.AsString()
				}

				fmt.Printf("%v\n", foundIDs)
			}
		case "create_group":
			if len(command) < 2 {
				fmt.Printf("Error in command \"create_group\": command must be of the form \"create_group group_name\"\n")
				continue
			}
			groupName := command[1]
			kademliaServer.SendCreateGroup(groupName)

		case "join_group":

		case "multicast_group":

		case "leave_group":

		default:
			fmt.Printf("Unrecognized command: %s\n", command[0])
		}
	}
}
