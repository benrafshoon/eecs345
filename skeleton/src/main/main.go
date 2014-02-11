package main

import (
    "flag"
    "fmt"
    "log"
    "math/rand"
    "net"
    //"net/http"
    //"net/rpc"
    "time"
    "os"
    "bufio"
    "strings"
    "strconv"
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

    var kademliaServer *kademlia.KademliaServer

    if len(args) >= 3 {
        testNodeID, error := kademlia.FromString(args[2])
        if error != nil {
            log.Fatal("Error parsing test node ID: ", error)
        }
        kademliaServer = kademlia.NewTestKademliaServer(testNodeID)

    } else {
        kademliaServer = kademlia.NewKademliaServer()
    }
    
    error := kademliaServer.StartKademliaServer(listenStr)

    if error != nil {
        log.Fatal("Error starting kademlia server: ", error)
    }

    kademliaServer.Ping(firstPeerStr)

    /*
    fmt.Printf("kademlia starting up!\n")
    kadem := kademlia.NewKademlia()

    rpc.Register(kadem)
    rpc.HandleHTTP()
    l, err := net.Listen("tcp", listenStr)
    if err != nil {
        log.Fatal("Listen: ", err)
    }

    // Serve forever.
    go http.Serve(l, nil)

    // Confirm our server is up with a PING request and then exit.
    // Your code should loop forever, reading instructions from stdin and
    // printing their results to stdout. See README.txt for more details.
    client, err := rpc.DialHTTP("tcp", firstPeerStr)
    if err != nil {
        log.Fatal("DialHTTP: ", err)
    }
    ping := new(kademlia.Ping)
    ping.MsgID = kademlia.NewRandomID()

    //contact := kadem.GetContact()
    var pong kademlia.Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        log.Fatal("Call: ", err)
    }

    log.Printf("ping msgID: %s\n", ping.MsgID.AsString())
    log.Printf("pong msgID: %s\n", pong.MsgID.AsString())

    */
    //Testing for the basic queue of a bucket
    /*log.Printf(" *** TESTING BUCKET METHODS *** \n")
    bucket := kademlia.NewBucket()
    contact1 := kademlia.NewContact()
    contact2 := kademlia.NewContact()
    contact3 := kademlia.NewContact()
    contact4 := kademlia.NewContact()
    bucket.PingBucket(*contact1)
    log.Printf("What does the bucket look like? 1", bucket)
    bucket.PingBucket(*contact2)
    log.Printf("What does the bucket look like? 2", bucket)
    bucket.PingBucket(*contact3)
    log.Printf("What does the bucket look like? 3", bucket)
    bucket.PingBucket(*contact2)
    log.Printf("Bumped contact 2 to bottom", bucket) */

    in := bufio.NewReader(os.Stdin)
    quit := false
    for !quit {
        
        input, err := in.ReadString('\n')
        if err != nil {
                // handle error
        }
        input = strings.Replace(input, "\n", "", -1) //use this as our end of input so remove it here
        command := strings.Split(input," ")

        switch command[0] {
        case "whoami":
            fmt.Printf("%v\n Node ID: %v\n", kademliaServer.GetNodeID().AsString(), kademliaServer.GetNodeID())
        case "local_find_value":
            if len(command) < 2 {
                log.Printf("Error in command \"local_find_value\": must enter key, command must be of the form \"local_find_value key\"")
            } else if id, error := kademlia.FromString(command[1]); error != nil {

                log.Printf("Error in command \"local_find_value\": %v", error)
            } else {
                log.Printf("Finding local value for key %v", id)
                value := kademliaServer.Data.RetrieveValue(id)
                if value != nil {
                    fmt.Printf("%v\n", string(value))
                } else {
                    fmt.Printf("ERR\n")
                }
                
            }
        case "get_contact":
            log.Printf("Not yet implemented")
        case "iterativeStore":
            log.Printf("Not yet implemented")
        case "iterativeFindNode":
            log.Printf("Not yet implemented")
        case "iterativeFindValue":
            log.Printf("Not yet implemented")

        case "ping":
            if len(command) < 2 {
                log.Printf("Error in command \"ping\": must enter address or node if, command must be of the form \"ping nodeID\" or \"ping host:port\"")
            } else if _, _, error := net.SplitHostPort(command[1]); error == nil {
                error := kademliaServer.Ping(command[1])
                if error != nil {
                    log.Println(error)
                }
            } else {
                _, error := kademlia.FromString(command[1])
                if error != nil {
                    log.Printf("Error in command \"ping\": nodeID: %v", error)
                } else {
                    log.Printf("Ping by nodeID not yet implemented\n")
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
            if kademliaServer.GetNodeID().Equals(nodeID) {
                log.Printf("Storing in self")
                kademliaServer.Data.InsertValue(nodeID, value)
                
            } else {
                log.Printf("Remote store not yet implemented")
            }

            fmt.Printf("\n")
        case "find_node":
            if len(command) != 3 {
                log.Printf("Error in command \"find_node\": command must be of the form \"find_node nodeID key\"")
            } else {
                nodeID, _ := kademlia.FromString(command[1])
                log.Printf("Node ID: %v\n", nodeID)
                key, _ := strconv.Atoi(command[2])
                kademliaServer.FindNode(nodeID, key)
            }
        case "find_value":
            log.Printf("Not yet implemented")
        case "quit": 
            quit = true

        default:
            log.Printf("Unrecognized command: %s", command[0])
        }
    }
}

