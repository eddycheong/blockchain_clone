package blockchain

import "../kvlib"

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"math/rand"
	"sort"
	"strconv"
	"time"
	// "os"
	// "fmt"
)

// Generic block data structure for: genesis block, transaction block, or no-op block
type Block struct {
	PrevHash     string
	Transactions map[kvlib.Key]kvlib.Value //KeyValue
	NodeID       int
	Nonce        uint32
}

type HashingBlock struct {
	PrevHash     string
	Transactions []string
	NodeID       int
	Nonce        uint32
}

type BlockChain struct {
	// Metadata
	TailHash []string // Hashstring of the end-block on the longest chain
	MaxDepth int

	// Structure data
	Chain    map[string]MetadataBlock
	Children map[string][]string // Key: Parent's hash, Value: List of Children's hash
}

type MetadataBlock struct {
	BlockData Block
	Depth     int
	//TransactionID 	int // TODO: should i keep the transactionID that was associated with the block? The block data does not have this information
}

//
func (bc *BlockChain) Init(gHash string) {

	// Initialize internal attributes
	bc.Chain = make(map[string]MetadataBlock)
	bc.Children = make(map[string][]string)

	// Create the genesis block
	var genesisBlock Block

	// Update the blockchain
	bc.Chain[gHash] = MetadataBlock{genesisBlock, 0}
	bc.TailHash = append(bc.TailHash, gHash)
	bc.MaxDepth = 0
}

func (bc *BlockChain) GenerateAndMineBlock(prevHash string, nodeID int, transactions map[kvlib.Key]kvlib.Value, leadingZeroes int) (block Block) {

	// Create new block and
	block = createBlock(prevHash, transactions, nodeID)
	block.Mining(leadingZeroes)

	return
}

func (bc *BlockChain) IntegrateBlockAndSignal(block Block) (signal string) {
	// Get Parent block information
	parentHash := block.PrevHash
	parentDepth := bc.Chain[parentHash].Depth

	// Get Block information
	blockHash := HashBlock(block)
	blockDepth := parentDepth + 1

	// Update Blockchain information
	bc.Chain[blockHash] = MetadataBlock{block, blockDepth}
	bc.Children[parentHash] = append(bc.Children[parentHash], blockHash)

	// default signal
	signal = "nothing"

	// Update the new hash of the end-block of the longest chain
	if blockDepth > bc.MaxDepth {

		// Check if this block is part of the longest chain
		// The depth that is consistent with the global key-value store
		globalKvDepth := bc.MaxDepth
		targetHash := block.PrevHash

		// Find the hash that matches that globalKvDepth
		for i := blockDepth - 1; i > globalKvDepth; i-- {
			targetHash = bc.Chain[targetHash].BlockData.PrevHash
		}

		// If these two hashes match, then it is safe to update
		if targetHash == HashBlock(bc.Chain[bc.TailHash[0]].BlockData) {
			signal = "update"
		} else {
			signal = "rebuild"
		}

		// Empty list and add the new hash
		bc.TailHash = bc.TailHash[:0]
		bc.TailHash = append(bc.TailHash, blockHash)
		bc.MaxDepth = blockDepth

	} else if blockDepth == bc.MaxDepth {
		// Another node is also the "longest" chain
		bc.TailHash = append(bc.TailHash, blockHash)
	}

	return
}

// Resolve the blockchain in an event that
func (bc *BlockChain) ResolveForks(transactions map[kvlib.Key]kvlib.Value, accessDepth int) {

	// Multiple forks has been detected
	if len(bc.TailHash) > 1 {

		candidates := make([]string, 0)

		// Add non-conflicting blocks as candidates
		for _, hash := range bc.TailHash {
			if !bc.IsConflictingChain(transactions, hash, accessDepth) {
				candidates = append(candidates, hash)
			}
		}

		// TODO: priortize txn block over noop

		// If there are multiple candidates, select one at random
		rand.Seed(time.Now().Unix())
		candidate := rand.Intn(len(candidates))
		bc.TailHash = candidates[candidate : candidate+1]
	}

	return
}

func (bc *BlockChain) IsConflictingChain(transactions map[kvlib.Key]kvlib.Value, hash string, accessDepth int) (conflict bool) {

	conflict = false

	//prevHash := bc.Chain[hash].BlockData.PrevHash
	prevHash := hash

	// Check all blocks to the depth accessed by the currently mined transaction block
	for maxDepth := bc.MaxDepth; maxDepth >= accessDepth; maxDepth-- {

		blockData := bc.Chain[prevHash].BlockData
		prevHash = blockData.PrevHash

		// Check if there are any "conflicting"
		for key, _ := range blockData.Transactions {
			if kvlib.KeyExist(transactions, key) {
				conflict = true
				return
			}
		}

	}

	return
}

func createBlock(prevHash string, transactions map[kvlib.Key]kvlib.Value, nodeID int) (b Block) {

	b.PrevHash = prevHash
	b.Transactions = transactions
	b.NodeID = nodeID

	return
}

// Proof-of-work algorithm in generating a new nonce for the block
func (b *Block) Mining(leadingZeroes int) {

	var nonce uint32
	nonce = 0

	// Initial setup of the block's data
	b.Nonce = nonce

	for true {
		if Validate(*b, leadingZeroes) {
			break
		}

		// Increment the nonce for the block data
		nonce = nonce + 1
		b.Nonce = nonce
	}
}

func OrderTransaction(blockData Block) (hashBlock HashingBlock) {
	var listTx []string
	listTx = make([]string, 0)
	for k, v := range blockData.Transactions {
		kStr := kvlib.KeyToString(k)
		vStr := kvlib.ValueToString(v)
		newStr := kStr + "," + vStr
		listTx = append(listTx, newStr)
	}
	sort.Strings(listTx)

	hashBlock.Transactions = listTx
	hashBlock.PrevHash = blockData.PrevHash
	hashBlock.NodeID = blockData.NodeID
	hashBlock.Nonce = blockData.Nonce
	return
}

// Return true if the hash of the block contains at least the number of leading zeroes. Otherwise false
func Validate(blockData Block, leadingZeroes int) (valid bool) {

	hashBlock := OrderTransaction(blockData)
	// fmt.Fprintf(os.Stderr, "HASHBLOCK: %v", hashBlock)
	// Convert block into an array of bytes
	hash, _ := getBytes(hashBlock)

	hasher := sha256.New()
	hasher.Write(hash)

	// Return the sha256 checksum hash of the block
	sha := hasher.Sum(nil)

	return checkLeadingZeroes(sha, leadingZeroes)
}

func HashBlock(blockData Block) (hashstring string) {

	hashBlock := OrderTransaction(blockData)

	// fmt.Fprintf(os.Stderr, "HASHBLOCK: %v", hashBlock)
	// Convert block into an array of bytes
	hash, _ := getBytes(hashBlock)

	hasher := sha256.New()
	hasher.Write(hash)

	// Return the sha256 checksum hash of the block
	sha := hasher.Sum(nil)

	hashstring = HashToString(sha)
	return
}

// Return true if the number of zeroes at least matches with argument leadingZeroes. Otherwise false
func checkLeadingZeroes(sha []byte, leadingZeroes int) (valid bool) {

	// Edge case: empty string "" is not convertible via strconv
	if leadingZeroes == 0 {
		return true
	}

	// Convert the sha256 checksum into a string
	shaString := HashToString(sha)

	// The prefix bits of the "sha" hash
	prefixSha := shaString[:leadingZeroes]

	// Convert the string into a number
	zero, err := strconv.Atoi(prefixSha)

	if err != nil {
		// a non-numeric value is part of the prefix bits of the "sha" hash.
		return false
	}

	return zero == 0
}

// Return the []byte of any type
func getBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Convert byte array (hash representation)
func HashToString(hash []byte) (hashString string) {
	return hex.EncodeToString(hash)
}
