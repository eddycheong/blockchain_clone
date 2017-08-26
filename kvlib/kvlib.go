package kvlib

import (
	"fmt"
	"strings"
)

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// Return true if entry in key-value map. Otherwise false
func KeyExist(kvMap map[Key]Value, key Key) (exist bool) {
	_, exist = kvMap[key]
	return
}

func TXExist(kvMap map[int]string, key int) (exist bool) {
	_, exist = kvMap[key]
	return
}

func KeyToString(key Key) (str string) {
	str = strings.TrimSpace(fmt.Sprintf("%s", key))
	return
}

func ValueToString(value Value) (str string) {
	str = strings.TrimSpace(fmt.Sprintf("%s", value))
	return
}
