package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	//"net"
	//"net/http"
	//"net/rpc"
	"bufio"
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

	// Get the bind and connect connection strings from command-line arguments.
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		log.Fatal("Must be invoked with at least two arguments!\n")
	}
	listenStr := args[0]
	firstPeerStr := args[1]

	var kademliaServer *kademlia.Kademlia

	if len(args) >= 3 {
		testNodeID, error := kademlia.FromString(args[2])
		if error != nil {
			log.Fatal("Error parsing test node ID: ", error)
		}
		kademliaServer = kademlia.NewTestKademlia(testNodeID)

	} else {
		kademliaServer = kademlia.NewKademlia()
	}

	error := kademliaServer.StartKademliaServer(listenStr)

	if error != nil {
		log.Fatal("Error starting kademlia server: ", error)
	}

	firstPeer, error := kademlia.NewContactFromAddressString(firstPeerStr)
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
			fmt.Printf("%v\n Node ID: %v\n", kademliaServer.GetNodeID().AsString(), kademliaServer.GetNodeID())
		case "local_find_value":
			if len(command) < 2 {
				log.Printf("Error in command \"local_find_value\": must enter key, command must be of the form \"local_find_value key\"")
				continue
			}
			id, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"local_find_value\": %v", error)
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
				log.Printf("Error in command \"get_contact\": must enter ID, command must be of the form \"get_contact ID\"")
				continue
			}
			id, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"get_contact\": %v", error)
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
			log.Printf("Not yet implemented")

		case "iterativeFindNode":
			if len(command) < 2 {
				log.Printf("Error in command \"find_node\": command must be of the form \"find_node nodeID\"")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"find_node\": nodeID: %v", error)
				continue
			}

			log.Printf("Finding nodes close to %v", nodeID.AsString())

			error, foundContacts := kademliaServer.SendIterativeFindNode(nodeID)
			if error != nil {
				log.Printf("%v", error)
				fmt.Printf("ERR\n")
				continue
			}

			foundIDs := make([]kademlia.ID, len(foundContacts), len(foundContacts))

			log.Printf("NodeIDs found: ")

			for i := 0; i < len(foundContacts); i++ {
				foundIDs[i] = foundContacts[i].NodeID

				log.Printf("%v", foundIDs[i].AsString())
			}

			fmt.Printf("%v\n", foundIDs)

		case "iterativeFindValue":
			log.Printf("Not yet implemented")

		case "ping":
			if len(command) < 2 {
				log.Printf("Error in command \"ping\": must enter address or node if, command must be of the form \"ping nodeID\" or \"ping host:port\"")
				continue
			}
			if firstContact, error := kademlia.NewContactFromAddressString(command[1]); error == nil {
				//ping host:port
				_, error := kademliaServer.SendPing(firstContact)
				if error != nil {
					log.Println(error)
				}
			} else {
				//ping nodeID
				id, error := kademlia.FromString(command[1])
				if error != nil {
					log.Printf("Error in command \"ping\": nodeID: %v", error)
					continue
				}

				hasContact, isSelf, contact := kademliaServer.RoutingTable.LookupContactByNodeID(id)
				if !hasContact {
					log.Printf("Contact not found")
					fmt.Printf("ERR\n")
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
				log.Printf("Error in command \"store\": command must be of the form \"store nodeID key value\"")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"store\": nodeID: %v", error)
				continue
			}
			key, error := kademlia.FromString(command[2])
			if error != nil {
				log.Printf("Error in command \"store\": key: %v", error)
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
				log.Printf("Error in command \"find_node\": command must be of the form \"find_node nodeID key\"")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"find_node\": nodeID: %v", error)
				continue
			}

			nodeToFind, error := kademlia.FromString(command[2])
			if error != nil {
				log.Printf("Error in command \"find_node\": key: %v", error)
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

			foundIDs := make([]kademlia.ID, len(foundContacts), len(foundContacts))

			log.Printf("NodeIDs found: ")

			for i := 0; i < len(foundContacts); i++ {
				foundIDs[i] = foundContacts[i].NodeID

				log.Printf("%v", foundIDs[i].AsString())
			}

			fmt.Printf("%v\n", foundIDs)

		case "find_value":
			if len(command) < 3 {
				log.Printf("Error in command \"find_value\": command must be of the form \"find_node nodeID key\"")
				continue
			}
			nodeID, error := kademlia.FromString(command[1])
			if error != nil {
				log.Printf("Error in command \"find_value\": nodeID: %v", error)
				continue
			}

			key, error := kademlia.FromString(command[2])
			if error != nil {
				log.Printf("Error in command \"find_value\": key: %v", error)
				continue
			}
			log.Printf("Finding value with key %v from node %v", key.AsString(), nodeID.AsString())

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
				fmt.Printf("%v %v\n", nodeID, string(foundValue))
			} else {
				foundIDs := make([]kademlia.ID, len(foundContacts), len(foundContacts))

				log.Printf("No value, but NodeIDs found: ")

				for i := 0; i < len(foundContacts); i++ {
					foundIDs[i] = foundContacts[i].NodeID

					log.Printf("%v", foundIDs[i].AsString())
				}

				fmt.Printf("%v\n", foundIDs)
			}
		default:
			log.Printf("Unrecognized command: %s", command[0])
		}
	}
}
