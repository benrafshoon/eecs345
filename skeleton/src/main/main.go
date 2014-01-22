package main

import (
    "flag"
    "fmt"
    "log"
    "math/rand"
    "net"
    "net/http"
    "net/rpc"
    "time"
    "os"
    "bufio"
    "strings"
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
    if len(args) != 2 {
        log.Fatal("Must be invoked with exactly two arguments!\n")
    }
    listenStr := args[0]
    firstPeerStr := args[1]

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
    var pong kademlia.Pong
    err = client.Call("Kademlia.Ping", ping, &pong)
    if err != nil {
        log.Fatal("Call: ", err)
    }

    log.Printf("ping msgID: %s\n", ping.MsgID.AsString())
    log.Printf("pong msgID: %s\n", pong.MsgID.AsString())

    //Testing for the basic queue of a bucket
    log.Printf(" *** TESTING BUCKET METHODS *** \n")
    bucket := kademlia.NewBucket()
    testContact := kademlia.NewContact()
    bucket.AddContact(*testContact)
    log.Printf("There should be one thing in the bucket: %v", bucket.ItemCount)
    bucket.Pop()
    log.Printf("There should be nothing in the bucket: %v", bucket.ItemCount)
    for i:=0; i<180; i++ {
        bucket.AddContact(*testContact)
    }
    log.Printf("Bucket should be no more than 160: %v", bucket.ItemCount)

    for {
        in := bufio.NewReader(os.Stdin)
        input, err := in.ReadString('\n')
        if err != nil {
                // handle error
        }
        input = strings.Replace(input, "\n", "", -1) //use this as our end of input so remove it here
        command := strings.Split(input," ")

        switch command[0] {
        case "whoami":
            log.Printf("Your node ID is: %v", kadem.NodeID)
        default:
            log.Printf("Unrecognized command: %s", command[0])
        }
    }
}

