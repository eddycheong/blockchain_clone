// Usage: go run kvnode.go [ghash] [num-zeroes] [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]

package main

import "./blockchain"
import "./kvlib"

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Peer node resource type
type Peer int

// Server node resource type
type Server int

//////////////////////////////////////////////
// Server Interactions

type UpdateMapRes struct {
	TxMap   map[kvlib.Key]kvlib.Value
	LastCmd string
	Aborted bool
	Success bool
}

type BlockReq struct {
	TxID  int
	Block blockchain.Block
}

// Takes in a txID, returns the corresponding map and Last Cmd
func (t *Peer) UpdateTXMap(req *int, res *UpdateMapRes) error {
	txID := *req
	var ret UpdateMapRes
	ret.TxMap = txKeyValue[txID]
	ret.LastCmd = lastCmds[txID]
	ret.Success = true
	ret.Aborted = abortedTX[txID]
	*res = ret

	return nil
}

func RebuildGlobalKeyValue(targetHash string) {

	prevHash := targetHash
	stack := make([]string, 0)

	// If the prevHash does not match the genesis hash
	for prevHash != ghash {
		stack = append(stack, prevHash)
		prevHash = bChain.Chain[prevHash].BlockData.PrevHash
	}

	newGlobalKV := make(map[kvlib.Key]kvlib.Value)

	// Recompute the newGlobalKV
	for _, hash := range stack {
		for key, value := range bChain.Chain[hash].BlockData.Transactions {
			newGlobalKV[key] = value
		}
	}

	gkvLock.Lock()
	globalKeyValue = newGlobalKV
	gkvLock.Unlock()

}

func HandleGlobalKeyValue(signal string, txID int) {
	if signal == "update" {
		// Update global keyvalue store
		if txID == 0 {
			txLock.Lock()
			UpdateKeyValueStore(txKeyValue[txID])
			txLock.Unlock()
		}
	} else if signal == "rebuild" {
		RebuildGlobalKeyValue(bChain.TailHash[0])

		if txID == 0 {
			txLock.Lock()
			UpdateKeyValueStore(txKeyValue[txID])
			txLock.Unlock()
		}
	} else {
		// signal = nothing, meaning a noop
	}
}

func (t *Peer) BroadcastBlock(req *BlockReq, res *int) error {

	var tempBlock blockchain.Block
	tempBlock.NodeID = req.Block.NodeID
	tempBlock.PrevHash = req.Block.PrevHash
	tempBlock.Nonce = req.Block.Nonce
	tempBlock.Transactions = req.Block.Transactions

	if blockchain.Validate(tempBlock, numZeroes) {
		signal := bChain.IntegrateBlockAndSignal(req.Block)

		HandleGlobalKeyValue(signal, req.TxID)

		if req.TxID != 0 {
			outsideTxnBlocks[req.TxID] = 0

			hashLock.Lock()

			txHashes[req.TxID] = blockchain.HashBlock(tempBlock)
			// logger.Println(tempBlock)
			// logger.Printf("BROADCAST TXID#%d HASH IS: %s\n", req.TxID, txHashes[req.TxID])
			hashLock.Unlock()

			// Tell the commit channel that we've integrated the block
			channelLock.Lock()
			channels[req.TxID] <- -2

			// Tell all the channels what is the current max depth of
			for _, channel := range channels {
				channel <- bChain.MaxDepth
			}
			channelLock.Unlock()

			logger.Printf("Integrating Txn Block (txID:%d) from Node %d\n:", req.TxID, req.Block.NodeID, blockchain.HashBlock(req.Block))
		} else {
			logger.Println("Integrating No-op Block :", blockchain.HashBlock(req.Block))
		}

	}

	return nil
}

// Server Interactions
//////////////////////////////////////////////

//////////////////////////////////////////////
// Client Interactions
type GetReq struct {
	TransactionID int
	Key           kvlib.Key
}

type GetRes struct {
	Value   kvlib.Value
	Success bool
	LastCmd string
}

type PutReq struct {
	TransactionID int
	Key           kvlib.Key
	Value         kvlib.Value
}

type PutRes struct {
	Success bool
	LastCmd string
}

type ChildrenRes struct {
	Children []string
	Success  bool
}

type CommitReq struct {
	TransactionID int
	ValidateNum   int
}

func (t *Server) Identify(req *int, res *int) error {
	*res = nodeID
	return nil
}

func (t *Server) NewTX(req *int, res *int) error {
	// Make new tx key-value store for new transaction
	currTXID := nextTXID
	txLock.Lock()
	txKeyValue[currTXID] = make(map[kvlib.Key]kvlib.Value)
	abortedTX[currTXID] = false
	txLock.Unlock()

	timeLock.Lock()
	clientTimers[currTXID] = time.AfterFunc(time.Second*timeout, func() {
		fmt.Fprintf(os.Stderr, "A client has timed out\n")

		// Erase the transaction
		txLock.Lock()
		delete(txKeyValue, currTXID)
		txLock.Unlock()

		cmdLock.Lock()
		delete(lastCmds, currTXID)
		cmdLock.Unlock()

		timeLock.Lock()
		clientTimers[currTXID].Stop()
		timeLock.Unlock()
	})
	timeLock.Unlock()

	*res = nextTXID

	txLock.Lock()
	lastCmds[nextTXID] = "NewTX"
	txLock.Unlock()

	// Increase TXID
	nextTXID = nextTXID + 1

	return nil
}

// Returns value corresponding to key in request from tx key-value store
func (t *Server) Get(req *GetReq, res *GetRes) error {
	id := req.TransactionID
	key := req.Key
	var ret GetRes

	// Check if we have a record of what the last transaction was
	if kvlib.TXExist(lastCmds, id) {
		// If this transaction was aborted
		if abortedTX[id] {
			ret.Value = ""
			ret.LastCmd = ""
			ret.Success = false
			*res = ret
			return nil
		}

		ret.LastCmd = lastCmds[id]
		clientTimers[id].Reset(time.Second * timeout)
	} else {
		// Make a new map for this transaction
		ret.LastCmd = ""

		abortedLock.Lock()
		abortedTX[id] = false
		abortedLock.Unlock()

		txLock.Lock()
		txKeyValue[id] = make(map[kvlib.Key]kvlib.Value)
		txLock.Unlock()

		timeLock.Lock()
		clientTimers[id] = time.AfterFunc(time.Second*timeout, func() {
			fmt.Fprintf(os.Stderr, "A client has timed out\n")

			// Erase the transaction
			txLock.Lock()
			delete(txKeyValue, id)
			txLock.Unlock()

			cmdLock.Lock()
			delete(lastCmds, id)
			cmdLock.Unlock()

			timeLock.Lock()
			clientTimers[id].Stop()
			timeLock.Unlock()
		})
		timeLock.Unlock()
	}

	kvStore := txKeyValue[id]

	// Check if that key has already been used
	if kvlib.KeyExist(kvStore, key) {
		ret.Value = kvStore[key]
	} else {
		// If our transactio kvStore doesn't have it, get it from global
		ret.Value = globalKeyValue[key]
	}

	ret.Success = true
	// Ex: key = "hello"
	// Result: Get hello
	cmdLock.Lock()
	lastCmds[id] = "Get " + kvlib.KeyToString(key)
	cmdLock.Unlock()

	// Do some logging to make it easier to resolve conflicts later
	if !KeyAccessed(id, key) {
		txAccessKeys[id] = append(txAccessKeys[id], key)
		// logger.Println("No prevoius access:", key)
	} else {
		// logger.Println("Prevoiusly accessed:", key)
	}

	if FirstAccess(id) {
		txAccessDepth[id] = bChain.MaxDepth
		// logger.Println("First access:", aDepth)
	} else {
		// logger.Println("Not first access:", txAccessDepth[txID])
	}

	*res = ret

	return nil
}

// Puts a key value pair into a tx key-value store
func (t *Server) Put(req *PutReq, res *PutRes) error {
	id := req.TransactionID
	key := req.Key
	val := req.Value
	var ret PutRes

	// Check if we have a record of what the last transaction was
	if kvlib.TXExist(lastCmds, id) {
		// If this transaction was aborted
		if abortedTX[id] {
			ret.LastCmd = ""
			ret.Success = false
			*res = ret
			return nil
		}

		cmdLock.Lock()
		ret.LastCmd = lastCmds[id]
		cmdLock.Unlock()

		timeLock.Lock()
		clientTimers[id].Reset(time.Second * timeout)
		timeLock.Unlock()

	} else {
		ret.LastCmd = ""

		abortedLock.Lock()
		abortedTX[id] = false
		abortedLock.Unlock()

		txLock.Lock()
		txKeyValue[id] = make(map[kvlib.Key]kvlib.Value)
		txLock.Unlock()

		timeLock.Lock()
		clientTimers[id] = time.AfterFunc(time.Second*timeout, func() {
			fmt.Fprintf(os.Stderr, "A client has timed out\n")

			// Erase the transaction
			txLock.Lock()
			delete(txKeyValue, id)
			txLock.Unlock()

			cmdLock.Lock()
			delete(lastCmds, id)
			cmdLock.Unlock()

			timeLock.Lock()
			clientTimers[id].Stop()
			timeLock.Unlock()
		})
		timeLock.Unlock()
	}

	txLock.Lock()
	(txKeyValue[id])[key] = val
	txLock.Unlock()

	ret.Success = true

	// Ex: key = "dog", value = "daschund"
	// Result: Put dog daschund
	cmdLock.Lock()
	lastCmds[id] = "Put " + kvlib.KeyToString(key) + " " + kvlib.ValueToString(val)
	cmdLock.Unlock()

	*res = ret

	return nil
}

// Something is incorrect with node. Needs to update itself
func (t *Server) UpdateTX(req *int, res *int) error {
	txID := *req
	var i int

	timeLock.Lock()
	clientTimers[txID].Reset(time.Second * timeout)
	timeLock.Unlock()

Loop:
	for i = (nodeID - 2); i >= 0; i-- {
		peerServer := peers[i]
		var res UpdateMapRes
		peerServer.Call("Peer.UpdateTXMap", txID, &res)

		// We have successfully contacted an earlier node
		// Update all of our information
		if res.Success {
			txLock.Lock()
			txKeyValue[txID] = make(map[kvlib.Key]kvlib.Value)

			newMap := txKeyValue[txID]
			for k, v := range res.TxMap {
				newMap[k] = v
			}
			txLock.Unlock()

			cmdLock.Lock()
			lastCmds[txID] = res.LastCmd
			cmdLock.Unlock()

			abortedLock.Lock()
			abortedTX[txID] = res.Aborted
			abortedLock.Unlock()

			break Loop
		}
	}

	return nil
}

// Client aborts this transaction
func (t *Server) Abort(req *int, success *bool) error {
	txID := *req
	logger.Printf("Client aborted transaction #%d\n", txID)

	// Stop timer for this client
	timeLock.Lock()
	clientTimers[txID].Stop()
	timeLock.Unlock()

	// Erase the transaction
	txLock.Lock()
	delete(txKeyValue, txID)
	txLock.Unlock()

	cmdLock.Lock()
	delete(lastCmds, txID)
	cmdLock.Unlock()

	abortedLock.Lock()
	abortedTX[txID] = true
	abortedLock.Unlock()

	*success = true
	return nil
}

func (t *Server) GetChildren(req *string, res *ChildrenRes) error {
	var ret ChildrenRes

	if *req == "" {
		ret.Children = []string{ghash}
	} else {
		ret.Children = bChain.Children[*req]
	}

	ret.Success = true
	*res = ret

	return nil
}

func (t *Server) Commit(req *CommitReq, res *bool) error {
	id := req.TransactionID
	valid := req.ValidateNum
	pendingCommits = append(pendingCommits, id)

	// Stop the client timer
	clientTimers[id].Stop()

	channelLock.Lock()
	channels[id] = make(chan int)
	channelLock.Unlock()

	nodeDepth := 0

Loop:
	for {
		info := <-channels[id]

		switch info {
		case -1:
			// Aborted
			*res = false
			break Loop
		case -2:
			// My commit has been integrated
			hashLock.Lock()
			metadata := bChain.Chain[txHashes[id]]
			hashLock.Unlock()

			nodeDepth = metadata.Depth
			logger.Printf("Commit: nodeDepth: %d\n", nodeDepth)
		default:
			// Some block has been integrated to block chain. We're notified of maxDepth
			if nodeDepth != 0 && (info-nodeDepth) >= valid {
				logger.Printf("Current MaxDepth:%d\n", info)
				hashLock.Lock()
				myHash := txHashes[id]
				hashLock.Unlock()

				validated := false

			Check:
				for _, tailHash := range bChain.TailHash {
					currHash := tailHash
					currMetadata := bChain.Chain[currHash]

					for i := currMetadata.Depth; i >= nodeDepth; i-- {
						if i == nodeDepth && currHash == myHash {
							validated = true
							break Check
						} else {
							currHash = currMetadata.BlockData.PrevHash
							currMetadata = bChain.Chain[currHash]
						}
					}
				}

				if validated {
					*res = true
					break Loop
				} else {
					// Migration
					pendingCommits = append(pendingCommits, id)
					nodeDepth = 0
				}
			}
		}
	}

	channelLock.Lock()
	close(channels[id])
	delete(channels, id)
	channelLock.Unlock()

	return nil
}

// End of Client Interactions
//////////////////////////////////////////////

var (
	// Arguments
	ghash         string
	numZeroes     int
	nodesFilePath string
	nodeID        int
	nodesIpPort   string
	clientsIpPort string
	nextTXID      int

	// Global Variables
	globalKeyValue map[kvlib.Key]kvlib.Value
	txKeyValue     map[int]map[kvlib.Key]kvlib.Value
	txAccessKeys   map[int][]kvlib.Key
	txAccessDepth  map[int]int
	lastCmds       map[int]string
	peers          []*rpc.Client
	abortedTX      map[int]bool
	channels       map[int]chan int
	txHashes       map[int]string

	// Heartbeat variables
	clientTimers map[int]*time.Timer
	timeout      time.Duration

	// Locks
	gkvLock     sync.Mutex
	txLock      sync.Mutex
	timeLock    sync.Mutex
	cmdLock     sync.Mutex
	abortedLock sync.Mutex
	channelLock sync.Mutex
	hashLock    sync.Mutex

	// Global blockchain variables
	bChain           blockchain.BlockChain
	pendingCommits   []int       // List or Queue of transactionIDs ready to commit
	outsideTxnBlocks map[int]int // TransactionIDs of transaction blocks from other nodes
	miningTxID       int         // current transactionID being mined by the node

	// List of all nodes' ipPorts
	nodes []string

	// Global Logger
	logger *log.Logger

	// Enable/Disable logging
	debug = true
)

////////////////////////////

func main() {
	Initialize()

	Test()

	go ListenClients(clientsIpPort)

	go ListenNodes(nodesIpPort)

	go ConnectToNodes(nodes, nodeID)

	// Prevents the node from terminating
	select {}
}

func ManageBlockchain() {

	// for i := 0; i < 3; i++ {
	for true {

		// If a fork exists, resolve the fork and select a single chain
		bChain.ResolveForks(txKeyValue[miningTxID], txAccessDepth[miningTxID])

		broadcastTxID := 0
		noBlock := false

		var newBlock blockchain.Block

		// Generate No-op block
		if len(pendingCommits) == 0 {
			newBlock = bChain.GenerateAndMineBlock(bChain.TailHash[0], nodeID, nil, numZeroes)
			signal := bChain.IntegrateBlockAndSignal(newBlock)

			HandleGlobalKeyValue(signal, 0)

			// Tell all the channels what is the current max depth of
			channelLock.Lock()
			for _, channel := range channels {
				channel <- bChain.MaxDepth
			}
			channelLock.Unlock()

			logger.Println("Generating No-op")
			logger.Println("Block Hash:", blockchain.HashBlock(newBlock))

			// Generate Transaction block
		} else {

			txID := pendingCommits[0]
			pendingCommits = pendingCommits[1:]

			if !IsConflicting(txID) {

				// Keep track of what transaction the node is working on
				miningTxID = txID
				broadcastTxID = txID

				transaction := txKeyValue[txID]

				newBlock = bChain.GenerateAndMineBlock(bChain.TailHash[0], nodeID, transaction, numZeroes)

				// Check if the following transaction has already been mined
				_, minedOutside := outsideTxnBlocks[miningTxID]

				//
				if !minedOutside {
					signal := bChain.IntegrateBlockAndSignal(newBlock)

					HandleGlobalKeyValue(signal, miningTxID)

					hashLock.Lock()
					txHashes[miningTxID] = blockchain.HashBlock(newBlock)
					logger.Println(newBlock)
					logger.Printf("GENERATE TXID#%d HASH IS: %s\n", miningTxID, txHashes[miningTxID])
					hashLock.Unlock()

					// Tell the commit channel that we've integrated the block
					channelLock.Lock()
					channels[miningTxID] <- -2

					// Tell all the channels what is the current max depth of
					for _, channel := range channels {
						channel <- bChain.MaxDepth
					}
					channelLock.Unlock()

					logger.Printf("Transaction %d successful. Commiting %v\n", txID, transaction)
					logger.Println("Block Hash:", blockchain.HashBlock(newBlock))
				} else {
					noBlock = true
					logger.Printf("Transaction %d is already mined from another node", miningTxID)
				}

				// Node is currently not working on any transactions
				miningTxID = 0

			} else {
				// TODO: Abort conflicting transaction
				channelLock.Lock()
				channels[txID] <- -1
				channelLock.Unlock()
				logger.Printf("Transaction %d will be aborted\n", txID)
				noBlock = true
			}

		}

		// Only broadcast if there is a new block generated by this node
		if !noBlock {
			var res int
			req := BlockReq{broadcastTxID, newBlock}

			for _, node := range peers {
				err := node.Call("Peer.BroadcastBlock", &req, &res)

				if err != nil {
					logger.Println(err)
				}
			}
		}
	}

	pretty(ghash)
	logger.Println(len(peers))
}

func UpdateKeyValueStore(kvStore map[kvlib.Key]kvlib.Value) {

	// Update key-value store from commits or sent by other nodes
	for key, value := range kvStore {
		gkvLock.Lock()
		globalKeyValue[key] = value
		gkvLock.Unlock()
	}
}

func KeyAccessed(txID int, key kvlib.Key) (accessed bool) {
	accessed = false

	for _, accessKey := range txAccessKeys[txID] {

		// A key has been accessed
		if accessKey == key {
			accessed = true
			break
		}
	}
	return

}

func FirstAccess(txID int) (first bool) {
	_, first = txAccessDepth[txID]
	first = !first
	return
}

// Checks if the current transaction is conflicting with the longest chain in the blockchain
func IsConflicting(txID int) (conflict bool) {

	conflict = false

	accessKeys := txAccessKeys[txID]   // all the accessed key from the transaction
	accessDepth := txAccessDepth[txID] // The depth of the first access from the transaction

	// Access metadata-block information
	tailBlock := bChain.Chain[bChain.TailHash[0]]

	for accessDepth < tailBlock.Depth {

		hashString := tailBlock.BlockData.PrevHash
		transactions := tailBlock.BlockData.Transactions

		// Check for any conflicting access
		for _, key := range accessKeys {

			// A key has been modified since last accessed by transaction
			if kvlib.KeyExist(transactions, key) {
				conflict = true
				return
			}
		}

		// Iterate to the next block
		tailBlock = bChain.Chain[hashString]
	}
	return
}

func ConnectToNodes(peerNodes []string, myID int) {
	peers = make([]*rpc.Client, 0)
	time.Sleep(4 * time.Second)

	for i, peer := range peerNodes {
		if (i + 1) != myID {
			peerAddr, err := net.ResolveTCPAddr("tcp", peer)

			if err != nil {
				checkError("Something went wrong while connecting to a node", err, false)
				// TODO
			}

			peerConn, err := net.DialTCP("tcp", nil, peerAddr)

			if err != nil {
				//checkError("", err, false)
			} else {
				peerServer := rpc.NewClient(peerConn)
				peers = append(peers, peerServer)
			}
		}
	}

	go ManageBlockchain()
}

// Listen for new nodes that want to connect
func ListenNodes(ipPort string) {
	logger.Println("Listening for nodes on", ipPort)

	mserver := new(Peer)

	server := rpc.NewServer()
	server.Register(mserver)

	l, err := net.Listen("tcp", ipPort)
	if err != nil {
		logger.Println(err)
	}

	for {
		// Listens for new nodes
		conn, err := l.Accept()
		if err != nil {
			logger.Println(err)
		}

		// Handles RPC calls from a node asynchronously
		go server.ServeConn(conn)
	}
}

// Listen to new connections from clients and requests from clients
func ListenClients(ipPort string) {
	logger.Println("Listening for clients on", ipPort)

	mserver := new(Server)

	server := rpc.NewServer()
	server.Register(mserver)

	l, err := net.Listen("tcp", ipPort)
	if err != nil {
		logger.Println(err)
	}

	for {
		// Listens for new clients
		conn, err := l.Accept()
		if err != nil {
			logger.Println(err)
		}

		// Handles RPC calls from a client asynchronously
		go server.ServeConn(conn)
	}
}

// Parses the node file into a list
func ParseNodeFile(filename string) []string {

	f, err := ioutil.ReadFile(filename)
	if err != nil {
		logger.Println(err)
		os.Exit(1)
	}

	// Return a slice of strings "IP:port"
	return strings.Split(string(f), "\n")
}

// Parses the command line args.
func ParseArguments() (ghash string, numZeroes int, nodesFilePath string, nodeID int, nodesIpPort string, clientsIpPort string) {
	args := os.Args[1:]

	if len(args) == 6 {
		ghash = args[0]
		numZeroes, _ = strconv.Atoi(args[1])
		nodesFilePath = args[2]
		nodeID, _ = strconv.Atoi(args[3])
		nodesIpPort = args[4]
		clientsIpPort = args[5]
	} else {
		fmt.Println("Usage: go run kvnode.go [ghash] [num-zeroes] [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]")
		os.Exit(0)
	}

	// Log the parameters that is being used
	logger.Println("ghash:", ghash)
	logger.Println("numZeroes:", numZeroes)
	logger.Println("nodesFilePath:", nodesFilePath)
	logger.Println("nodeID:", nodeID)
	logger.Println("nodesIpPort:", nodesIpPort)
	logger.Println("clientsIpPort:", clientsIpPort)
	return
}

// Initialize logger and global variables
func Initialize() {
	logger = log.New(os.Stdout, "[debug] ", log.Lshortfile)

	// Enable/Disable logger
	if !debug {
		logger.SetOutput(ioutil.Discard)
	}
	logger.Println("Initializing: Logging Enabled")

	logger.Println("Parsing Command-Line Arguments:")
	ghash, numZeroes, nodesFilePath, nodeID, nodesIpPort, clientsIpPort = ParseArguments()

	// Parses Node File into a list of strings
	nodes = ParseNodeFile(nodesFilePath)

	// Initialize global variables
	globalKeyValue = make(map[kvlib.Key]kvlib.Value)
	txKeyValue = make(map[int]map[kvlib.Key]kvlib.Value)
	nextTXID = 1
	lastCmds = make(map[int]string)
	abortedTX = make(map[int]bool)
	channels = make(map[int]chan int)
	txHashes = make(map[int]string)

	clientTimers = make(map[int]*time.Timer)
	timeout = time.Duration(4 * len(nodes))

	txAccessKeys = make(map[int][]kvlib.Key)
	txAccessDepth = make(map[int]int)

	outsideTxnBlocks = make(map[int]int)

	// Initialize the blockchain and internal variables
	bChain.Init(ghash)
}

/////////////////////
// DEBUGGING TOOLS //
// TODO: DELETE  //
///////////////////

func pretty(root string) {

	fmt.Println(root)
	prettyHelper(bChain.Children[root])
}

func prettyHelper(children []string) {

	for _, child := range children {
		metadata := bChain.Chain[child]

		for i := 0; i < metadata.Depth; i++ {
			fmt.Print("    ")
		}

		fmt.Println(child)
		prettyHelper(bChain.Children[child])
	}

}

// TODO: ONLY FOR TESTING PURPOSES
// TODO: repurpose this function for running block generation in the background
//func ManageBlockchain() {
func Test() {

	// Create mock transaction
	// txKeyValue[1] = make(map[kvlib.Key]kvlib.Value)
	// txKeyValue[2] = make(map[kvlib.Key]kvlib.Value)
	// txKeyValue[3] = make(map[kvlib.Key]kvlib.Value)

	// Test Case 1
	// Tx2 and Tx3 should conflict. Tx3 should abort
	// SimPut(1, "a", "1", 0)
	// SimCommit(1)

	// SimGet(2, "a", 1)
	// SimPut(2, "a", "2", 1)

	// SimGet(3, "a", 1)
	// SimPut(3, "a", "3", 1)

	// SimCommit(2)
	// SimCommit(3)

	// Test Case 2
	// Tx2 and Tx3 should conflict. Tx2 should abort
	// SimPut(1, "a", "1", 0)
	// SimCommit(1)

	// SimGet(2, "a", 1)
	// SimPut(2, "a", "2", 1)

	// SimGet(3, "a", 1)
	// SimPut(3, "a", "3", 1)

	// SimCommit(3)
	// SimCommit(2)

	// Test Case 3
	// Tx2 and Tx3 should have no conflict. No conflicting keys even though same access depth
	// SimPut(1, "a", "1", 0)
	// SimCommit(1)

	// SimGet(2, "a", 1)
	// SimPut(2, "a", "2", 1)

	// SimGet(3, "b", 1)
	// SimPut(3, "b", "3", 1)

	// SimCommit(2)
	// SimCommit(3)

	// Test Case 4
	// Tx2 and Tx3 should have no conflict. Conflicting keys but in different access depth
	// SimPut(1, "a", "1", 0)
	// SimCommit(1)

	// SimGet(2, "a", 1)
	// SimPut(2, "a", "2", 1)

	// SimGet(3, "a", 2)
	// SimPut(3, "a", "3", 2)

	// SimCommit(2)
	// SimCommit(3)

	// logger.Println(bChain.TailHash)
	// logger.Println(len(bChain.Chain))
	// logger.Println(len(bChain.Children))
	// logger.Println(bChain.Chain)
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func SimGet(txID int, key kvlib.Key, aDepth int) {
	if !KeyAccessed(txID, key) {
		txAccessKeys[txID] = append(txAccessKeys[txID], key)
		// logger.Println("No prevoius access:", key)
	} else {
		// logger.Println("Prevoiusly accessed:", key)
	}

	if FirstAccess(txID) {
		txAccessDepth[txID] = aDepth
		// logger.Println("First access:", aDepth)
	} else {
		// logger.Println("Not first access:", txAccessDepth[txID])
	}
}

func SimPut(txID int, key kvlib.Key, value kvlib.Value, aDepth int) {
	if !KeyAccessed(txID, key) {
		txAccessKeys[txID] = append(txAccessKeys[txID], key)
		// logger.Println("No prevoius access:", key)
	} else {
		// logger.Println("Prevoiusly accessed:", key)
	}

	if FirstAccess(txID) {
		txAccessDepth[txID] = aDepth
		// logger.Println("First access:", aDepth)
	} else {
		// logger.Println("Not first access:", txAccessDepth[txID])
	}

	txKeyValue[txID][key] = value
}

func SimCommit(txID int) {
	pendingCommits = append(pendingCommits, txID)
}
