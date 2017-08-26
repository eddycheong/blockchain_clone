/*

This package specifies the application's interface to the key-value
service library to be used in assignment 7 of UBC CS 416 2016 W2.

*/

package kvservice

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
	"sync"
)

var (
	hasAssignedTXID bool
	nodeServers     map[string]*rpc.Client
)

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Used by a client to ask a node for information about the
	// block-chain. Node is an IP:port string of one of the nodes that
	// was used to create the connection.  parentHash is either an
	// empty string to indicate that the client wants to retrieve the
	// SHA 256 hash of the genesis block. Or, parentHash is a string
	// identifying the hexadecimal SHA 256 hash of one of the blocks
	// in the block-chain. In this case the return value should be the
	// string representations of SHA 256 hash values of all of the
	// children blocks that have the block identified by parentHash as
	// their prev-hash value.
	GetChildren(node string, parentHash string) (children []string)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is an empty string, and err is non-nil. If
	// success is false, then all future calls on this transaction
	// must immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true, then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// The validateNum argument indicates the number of blocks that
	// must follow this transaction's block in the block-chain along
	// the longest path before the commit returns with a success.
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit(validateNum int) (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	c := new(myconn)
	nodeServers = make(map[string]*rpc.Client)
	c.Servers = make([]*rpc.Client, len(nodes))

	for i, nodeIP := range nodes {
		fmt.Fprintf(os.Stderr, "Going through node id: %d address: %s\n", (i + 1), nodeIP)
		nodeAddr, err := net.ResolveTCPAddr("tcp", nodeIP)

		if err != nil {
			checkError("Something went wrong while connecting to a node", err, false)
			// TODO
		}

		nodeConn, err := net.DialTCP("tcp", nil, nodeAddr)

		if err != nil {
			//checkError("", err, false)
		} else {
			nodeServer := rpc.NewClient(nodeConn)
			var nodeID int
			nodeServer.Call("Server.Identify", 1, &nodeID)

			if nodeID != 0 {
				nodeServers[nodeIP] = nodeServer
				c.Servers[(nodeID - 1)] = nodeServer
			}
		}
	}

	return c
}

//////////////////////////////////////////////
// Connection interface

// Concrete implementation of a connection interface.
type myconn struct {
	Servers []*rpc.Client
}

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	fmt.Fprintf(os.Stderr, "NewTX\n")

	// Client needs a new transaction ID
	hasAssignedTXID = false

	m := new(mytx)
	m.Servers = conn.Servers

	for _, server := range m.Servers {
		var newTXID int
		server.Call("Server.NewTX", 1, &newTXID)

		if newTXID != 0 && !hasAssignedTXID {
			m.TransactionID = newTXID
			hasAssignedTXID = true
			fmt.Fprintf(os.Stderr, "Client has a new TXID: %d\n", m.TransactionID)
		}
	}

	var err error
	if !hasAssignedTXID {
		// None of the nodes assigned us a TXID... Probably means they're all dead
		err = errors.New("Transaction was not assigned a TXID, nodes are dead.")
	}

	// Initialize Last Transaction as an empty string
	m.LastCmd = "NewTX"

	return m, err
}

func (conn *myconn) GetChildren(node string, parentHash string) (children []string) {
	fmt.Fprintf(os.Stderr, "GetChildren\n")
	children = make([]string, 0)
	return
	// TODO
}

// Close the connection.
func (conn *myconn) Close() {
	fmt.Fprintf(os.Stderr, "Close\n")
	for _, server := range conn.Servers {
		server.Close()
	}
}

// /Connection interface
//////////////////////////////////////////////

//////////////////////////////////////////////
// Transaction interface

// Concrete implementation of a tx interface.
type mytx struct {
	TransactionID int
	LastCmd       string
	Servers       []*rpc.Client
}

type GetReq struct {
	TransactionID int
	Key           Key
}

type GetRes struct {
	Value   Value
	Success bool
	LastCmd string
}

type PutReq struct {
	TransactionID int
	Key           Key
	Value         Value
}

type PutRes struct {
	Success bool
	LastCmd string
}

type CommitReq struct {
	TransactionID int
	ValidateNum   int
}

// Retrieves a value v associated with a key k.
func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	fmt.Fprintf(os.Stderr, "Get\n")

	lastTx := t.LastCmd

	var req GetReq
	req.TransactionID = t.TransactionID
	req.Key = k

	var res GetRes
	v = ""

	for _, server := range t.Servers {
		server.Call("Server.Get", req, &res)

		if res.Success {
			// The Happy Path
			if strings.TrimSpace(res.LastCmd) == lastTx {
				// Assumes that a Value in a K-V pair will never be the empty string
				if v == "" {
					v = res.Value
					success = true
				} else if v != res.Value {
					var i int
					server.Call("Server.UpdateTX", t.TransactionID, &i)
					err = errors.New("Get: Value did not match between nodes")
				}
			} else {
				var i int
				server.Call("Server.UpdateTX", t.TransactionID, &i)
				err = errors.New("Get: Last Cmd did not match")
			}
		} else {
			// Could not find the value in the server or server is dead
			if strings.TrimSpace(res.LastCmd) == lastTx {
				v = ""
				success = false
			} else {
				var i int
				server.Call("Server.UpdateTX", t.TransactionID, &i)
				err = errors.New("Get: Last Cmd did not match")
			}
		}
	}

	if v == "" && !success {
		err = errors.New("Key has not been used yet for transactions or all nodes are dead")
	}

	t.LastCmd = "Get " + KeyToString(k)

	return
}

// Associates a value v with a key k.
func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	fmt.Fprintf(os.Stderr, "Put\n")

	lastTx := t.LastCmd

	var req PutReq
	req.TransactionID = t.TransactionID
	req.Key = k
	req.Value = v

	var res PutRes

	for _, server := range t.Servers {
		server.Call("Server.Put", req, &res)

		if res.Success {
			// The Happy Path
			if strings.TrimSpace(res.LastCmd) == lastTx {
				success = true
			} else {
				var i int
				server.Call("Server.UpdateTX", t.TransactionID, &i)
				err = errors.New("Put: At least one server was told to update itself")
			}
		} else {
			// Server is dead: Put always returns "Success = true" if alive
			err = errors.New("Put: At least one server died")
			// TODO: Do we delete this node from our list of servers?
		}
	}

	t.LastCmd = "Put " + KeyToString(k) + " " + ValueToString(v)

	return
}

// Commits the transaction.
func (t *mytx) Commit(validateNum int) (success bool, txID int, err error) {
	fmt.Fprintf(os.Stderr, "Commit\n")
	// Setup variables
	success = false
	txID = t.TransactionID

	var req CommitReq
	req.TransactionID = txID
	req.ValidateNum = validateNum

	var wg sync.WaitGroup

	for _, server := range t.Servers {
		wg.Add(1)
		go func(node *rpc.Client) {
			// fmt.Fprintf(os.Stderr, "Committing to server #%d\n")
			defer wg.Done()
			var ret bool

			node.Call("Server.Commit", req, &ret)
			if ret {
				success = true
			}
		}(server)
	}

	wg.Wait()

	if !success {
		err = errors.New("Commit: Transaction was aborted")
	}

	return
}

// Aborts the transaction.
func (t *mytx) Abort() {
	fmt.Fprintf(os.Stderr, "Abort\n")
	for _, server := range t.Servers {
		var success bool
		server.Call("Server.Abort", t.TransactionID, &success)

		if !success {
			fmt.Fprintf(os.Stderr, "Abort: At least one server died\n")
		}
	}
}

// /Transaction interface
//////////////////////////////////////////////

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func KeyToString(key Key) (str string) {
	str = strings.TrimSpace(fmt.Sprintf("%s", key))
	return
}

func ValueToString(value Value) (str string) {
	str = strings.TrimSpace(fmt.Sprintf("%s", value))
	return
}
