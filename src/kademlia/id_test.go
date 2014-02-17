package kademlia

import (
	"math/rand"
	"testing"
	"time"
	//"encoding/hex"
)

func test_DistanceBucket(t *testing.T, id1Str string, id2Str string) int {
	id1, error := FromString(id1Str)
	if error != nil {
		t.Fatal("Error interpreting ", id1Str, " ", error)
	}
	id2, error := FromString(id2Str)
	if error != nil {
		t.Fatal("Error interpreting ", id2Str, " ", error)
	}

	t.Logf("ID 1: %v", id1.AsString())
	t.Logf("ID 2: %v", id2.AsString())
	xor := id1.Xor(id2)
	t.Logf(" XOR: %v", xor.AsString())
	t.Logf("MSB")
	for i := 0; i < len(xor); i++ {
		t.Logf("Byte %v: %v", i, xor[i])
	}
	t.Logf("LSB")
	distanceBucket := id1.DistanceBucket(id2)
	t.Logf("Distance bucket: %v", distanceBucket)
	return distanceBucket
}

func Test_DistanceBucket_1(t *testing.T) {
	id1 := "0000000000000000000000000000000000000000"
	id2 := "8000000000000000000000000000000000000000"
	expectedResult := 159
	actualResult := test_DistanceBucket(t, id1, id2)
	if actualResult != expectedResult {
		t.Fatal("Expected %v, got %v", expectedResult, actualResult)
	}
}

func Test_DistanceBucket_2(t *testing.T) {
	id1 := "0000000000000000000000000000000000000000"
	id2 := "0000000000000000000000000000000000000001"
	expectedResult := 0
	actualResult := test_DistanceBucket(t, id1, id2)
	if actualResult != expectedResult {
		t.Fatal("Expected %v, got %v", expectedResult, actualResult)
	}
}

func Test_DistanceBucket_3(t *testing.T) {
	id1 := "0000000000000000000000000000000000000000"
	id2 := "0000000000000000000000000000000000000005"
	expectedResult := 2
	actualResult := test_DistanceBucket(t, id1, id2)
	if actualResult != expectedResult {
		t.Fatal("Expected %v, got %v", expectedResult, actualResult)
	}
}

func Test_DistanceBucket_4(t *testing.T) {
	id := "123456789ABCDEF0123456789ABCDEF012345678"
	expectedResult := -1
	actualResult := test_DistanceBucket(t, id, id)
	if actualResult != expectedResult {
		t.Fatal("Expected %v, got %v", expectedResult, actualResult)
	}
}

func Test_RandomIDInBucket(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	id1Str := "0000000000000000000000000000000000000000"
	id1, _ := FromString(id1Str)
	t.Log(id1.RandomIDInBucket(0).AsString())
	t.Log(id1.RandomIDInBucket(159).AsString())
	id2Str := "8000000000000000000000000000000000000000"
	id2, _ := FromString(id2Str)
	t.Log(id2.RandomIDInBucket(0).AsString())
	t.Log(id2.RandomIDInBucket(159).AsString())
}
