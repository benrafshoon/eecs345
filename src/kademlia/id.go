package kademlia
// Contains definitions for the 160-bit identifiers used throughout kademlia.

import (
    "encoding/hex"
    "math/rand"
    "errors"
)


// IDs are 160-bit ints. We're going to use byte arrays with a number of
// methods.
const IDBytes = 20
type ID [IDBytes]byte

//Kademlia IDs are big endian.  ID[0] is the MSB while ID[19] is the LSB
//Examples
//0100000000000000000000000000000000000000 is interpreted byte by byte
//01-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00
//LSB-----------------------------------------------------MSB
//High-Order----------------------------------------Low-Order
//[19]----------------------------------------------------[0]
//Value: 1

//F000000000000000000000000000000000000000 is interpreted byte by byte
//F0-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00-00
//LSB-----------------------------------------------------MSB
//High-Order----------------------------------------Low-Order
//[19]----------------------------------------------------[0]
//Value: 240

func (id ID) AsString() string {
    return hex.EncodeToString(id[0:IDBytes])
}

func (id ID) Xor(other ID) (ret ID) {
    for i := 0; i < IDBytes; i++ {
        ret[i] = id[i] ^ other[i]
    }
    return
}

func (id ID) Distance(other ID) (distance int) {
    distance = 0
    for i := 0; i<IDBytes; i++ {
        tempdistance := int(id[i]) - int(other[i])
        if tempdistance < 0 {
            tempdistance = -tempdistance
        }
        distance = distance + tempdistance
    }
    return distance
}

// Return -1, 0, or 1, with the same meaning as strcmp, etc.
func (id ID) Compare(other ID) int {
    for i := 0; i < IDBytes; i++ {
        difference := int(id[i]) - int(other[i])
        switch {
        case difference == 0:
            continue
        case difference < 0:
            return -1
        case difference > 0:
            return 1
        }
    }
    return 0
}

func (id ID) Equals(other ID) bool {
    return id.Compare(other) == 0
}

func (id ID) Less(other ID) bool {
    return id.Compare(other) < 0
}

// Return the number of consecutive zeroes, starting from the low-order (most-significant) bit, in
// a ID.
func (id ID) PrefixLen() int {
    for i:= 0; i < IDBytes; i++ {
        for j := 0; j < 8; j++ {
            if (id[i] >> uint8(j)) & 0x1 != 0 {
                return (8 * i) + j
            }
        }
    }
    return IDBytes * 8
}


func (id ID) PrefixLenFixed() int {
    for i:= 0; i < IDBytes; i++ {
        for j := 7; j >= 0; j-- { //We need to start at the MSB and go down
            if (id[i] >> uint8(j)) & 0x1 != 0 {
                //If the first 1 is at the MSB, then there are 0 prefixing 0s.
                //If the first 1 is at the LSB, then there are 7 prefixing 0s.
                return (8 * i) + (7 - j) 
            }
        }
    }
    return IDBytes * 8
}

// Generate a new ID from nothing.
func NewRandomID() (ret ID) {
    for i := 0; i < IDBytes; i++ {
        ret[i] = uint8(rand.Intn(256))
    }
    return
}

// Generate an ID identical to another.
func CopyID(id ID) (ret ID) {
    for i := 0; i < IDBytes; i++ {
        ret[i] = id[i]
    }
    return
}

// Generate a ID matching a given string.
func FromString(idstr string) (ret ID, err error) {
    bytes, err := hex.DecodeString(idstr)
    if err != nil {
        return
    }

    if len(bytes) != IDBytes {
        err = errors.New("Must be 160bit ID string")
        return
    }

    for i := 0; i < IDBytes; i++ {
        ret[i] = bytes[i]
    }
    return
}

//Distance bucket 
//Returns -1 if the ID == otherID
func (id ID) DistanceBucket(otherID ID) int {
    return (const_B - 1) - id.Xor(otherID).PrefixLenFixed()
}

