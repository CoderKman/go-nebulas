package trie

import (
	"bytes"
	"errors"

	"github.com/nebulasio/go-nebulas/crypto/hash"
	"github.com/nebulasio/go-nebulas/storage"
	"github.com/nebulasio/go-nebulas/util/byteutils"
)

// Errors
var (
	ErrCloneInBatch      = errors.New("cannot clone with a batch task unfinished")
	ErrBeginAgainInBatch = errors.New("cannot begin with a batch task unfinished")
)

// Action represents operation types in BatchTrie
type Action int

// Action constants
const (
	Insert Action = iota
	Update
	Delete
	Get
)

// Entry in changelog, [key, old value, new value]
type Entry struct {
	action Action
	key    []byte
	old    []byte
	update []byte
}

// BatchTrie is a trie that supports batch task
type BatchTrie struct {
	trie         *Trie
	changelog    []*Entry
	batching     bool
	initialoplog map[string]*Entry
	finaloplog   map[string]*Entry
}

// NewBatchTrie if rootHash is nil, create a new BatchTrie, otherwise, build an existed BatchTrie
func NewBatchTrie(rootHash []byte, storage storage.Storage) (*BatchTrie, error) {
	t, err := NewTrie(rootHash, storage)
	if err != nil {
		return nil, err
	}

	return &BatchTrie{trie: t, batching: false, initialoplog: make(map[string]*Entry), finaloplog: make(map[string]*Entry)}, nil
}

// RootHash of the BatchTrie
func (bt *BatchTrie) RootHash() []byte {
	return bt.trie.RootHash()
}

// Clone a the BatchTrie
func (bt *BatchTrie) Clone() (*BatchTrie, error) {
	tr, err := bt.trie.Clone()
	if err != nil {
		return nil, err
	}
	return &BatchTrie{trie: tr, changelog: bt.changelog, batching: bt.batching, initialoplog: make(map[string]*Entry), finaloplog: make(map[string]*Entry)}, nil
}

// Get the value to the key in BatchTrie
// return value to the key
func (bt *BatchTrie) Get(key []byte) ([]byte, error) {
	val, err := bt.trie.Get(key)
	entry := &Entry{Get, key, val, val}

	if _, ok := bt.initialoplog[byteutils.Hex(entry.key)]; !ok {
		bt.initialoplog[byteutils.Hex(entry.key)] = entry
	}

	if _, ok := bt.finaloplog[byteutils.Hex(entry.key)]; !ok || (ok && entry.action == Get) {
		bt.finaloplog[byteutils.Hex(entry.key)] = entry
	}

	return val, err
}

// Put the key-value pair in BatchTrie
// return new rootHash
func (bt *BatchTrie) Put(key []byte, val []byte) ([]byte, error) {
	entry := &Entry{Update, key, nil, val}
	old, getErr := bt.trie.Get(key)
	if getErr != nil {
		entry.action = Insert
	} else {
		entry.old = old
	}
	rootHash, putErr := bt.trie.Put(key, val)
	if putErr != nil {
		return nil, putErr
	}

	if _, ok := bt.initialoplog[byteutils.Hex(entry.key)]; !ok {
		bt.initialoplog[byteutils.Hex(entry.key)] = entry
	}
	bt.finaloplog[byteutils.Hex(entry.key)] = entry

	if bt.batching {
		bt.changelog = append(bt.changelog, entry)
	}
	return rootHash, nil
}

// Del the key-value pair in BatchTrie
// return new rootHash
func (bt *BatchTrie) Del(key []byte) ([]byte, error) {
	entry := &Entry{Delete, key, nil, nil}
	old, getErr := bt.trie.Get(key)
	if getErr == nil {
		entry.old = old
	}
	rootHash, err := bt.trie.Del(key)
	if err != nil {
		return nil, err
	}

	if _, ok := bt.initialoplog[byteutils.Hex(entry.key)]; !ok {
		bt.initialoplog[byteutils.Hex(entry.key)] = entry
	}
	bt.finaloplog[byteutils.Hex(entry.key)] = entry

	if bt.batching {
		bt.changelog = append(bt.changelog, entry)
	}
	return rootHash, nil
}

// SyncTrie data from other servers
// Sync whole trie to build snapshot
func (bt *BatchTrie) SyncTrie(rootHash []byte) error {
	return bt.trie.SyncTrie(rootHash)
}

// SyncPath from rootHash to key node from other servers
// Useful for verification quickly
func (bt *BatchTrie) SyncPath(rootHash []byte, key []byte) error {
	return bt.trie.SyncPath(rootHash, key)
}

// Prove the associated node to the key exists in trie
// if exists, MerkleProof is a complete path from root to the node
// otherwise, MerkleProof is nil
func (bt *BatchTrie) Prove(key []byte) (MerkleProof, error) {
	return bt.trie.Prove(key)
}

// Verify whether the merkle proof from root to the associated node is right
func (bt *BatchTrie) Verify(rootHash []byte, key []byte, proof MerkleProof) error {
	return bt.trie.Verify(rootHash, key, proof)
}

// Empty return if the trie is empty
func (bt *BatchTrie) Empty() bool {
	return bt.trie.Empty()
}

// Iterator return an trie Iterator to traverse leaf node's value in this trie
func (bt *BatchTrie) Iterator(prefix []byte) (*Iterator, error) {
	return bt.trie.Iterator(prefix)
}

// Count return count of members with the prefix in this trie
func (bt *BatchTrie) Count(prefix []byte) (int64, error) {
	count := int64(0)
	iter, err := bt.Iterator(prefix)
	if err != nil && err != storage.ErrKeyNotFound {
		return 0, err
	}
	if err != nil {
		return 0, nil
	}
	exist, err := iter.Next()
	for exist {
		count++
		exist, err = iter.Next()
	}
	return count, nil
}

// BeginBatch to process a batch task
func (bt *BatchTrie) BeginBatch() {
	bt.batching = true
}

// Commit a batch task
func (bt *BatchTrie) Commit() {
	// clear changelog
	bt.changelog = bt.changelog[:0]
	bt.batching = false
}

// RollBack a batch task
func (bt *BatchTrie) RollBack() {
	// compress changelog
	changelog := make(map[string]*Entry)
	for _, entry := range bt.changelog {
		if _, ok := changelog[byteutils.Hex(entry.key)]; !ok {
			changelog[byteutils.Hex(entry.key)] = entry
		}
	}
	// clear changelog
	bt.changelog = bt.changelog[:0]

	bt.initialoplog = nil
	bt.finaloplog = nil

	// rollback
	for _, entry := range changelog {
		switch entry.action {
		case Insert:
			bt.trie.Del(entry.key)
		case Update, Delete:
			bt.trie.Put(entry.key, entry.old)
		}
	}
	bt.batching = false
}

// HashDomains for each variable in contract
// each domain will represented as 6 bytes, support 4 level domain at most
// such as,
// 4a56b7 000000 000000 000000,
// 4a56b8 1c9812 000000 000000,
// 4a56b8 3a1289 000000 000000,
// support iterator with same prefix
func HashDomains(domains ...string) []byte {
	if len(domains) > 24/6 {
		panic("only support 4 level domain at most")
	}
	key := [24]byte{0}
	for k, v := range domains {
		domain := hash.Sha3256([]byte(v))[0:6]
		for i := 0; i < len(domain); i++ {
			key[k*6+i] = domain[i]
		}
	}
	return key[:]
}

// HashDomainsPrefix is same as HashDomains, but without tail zeros
func HashDomainsPrefix(domains ...string) []byte {
	if len(domains) > 24/6 {
		panic("only support 4 level domain at most")
	}
	key := []byte{}
	for _, v := range domains {
		domain := hash.Sha3256([]byte(v))[0:6]
		key = append(key, domain...)
	}
	return key[:]
}

// RelatedTo if two have same key and action is not GET return true else return false
func (bt *BatchTrie) RelatedTo(tobt *BatchTrie) bool {

	for _, toentry := range bt.finaloplog {

		if entry, ok := tobt.finaloplog[byteutils.Hex(toentry.key)]; ok {
			if toentry.action != Get || entry.action != Get {
				return true
			}
		}
	}

	return false
}

// MergeWith merge two batchtrie if key value is equal return false
func (bt *BatchTrie) MergeWith(tobt *BatchTrie) (bool, *BatchTrie) {

	//record the last state
	initialoplog := bt.initialoplog

	// the first state compare to the last state
	for _, toentry := range tobt.initialoplog {
		if entry, ok := bt.finaloplog[byteutils.Hex(toentry.key)]; ok {

			if !bytes.Equal(toentry.old, entry.update) {
				return false, nil
			}
		}
		if _, ok := bt.initialoplog[byteutils.Hex(toentry.key)]; !ok {
			initialoplog[byteutils.Hex(toentry.key)] = toentry
		}
	}

	// Clone a the BatchTrie
	newbt, err := bt.Clone()
	if err != nil {
		return false, nil
	}
	newbt.finaloplog = bt.finaloplog
	newbt.initialoplog = initialoplog

	// merge trie
	for _, entry := range tobt.finaloplog {
		switch entry.action {
		case Delete:
			newbt.trie.Del(entry.key)
			newbt.finaloplog[byteutils.Hex(entry.key)] = entry
		case Update, Insert:
			newbt.trie.Put(entry.key, entry.update)
			newbt.finaloplog[byteutils.Hex(entry.key)] = entry
		case Get:
			if _, ok := newbt.initialoplog[byteutils.Hex(entry.key)]; !ok || (ok && entry.action == Get) {
				newbt.initialoplog[byteutils.Hex(entry.key)] = entry
			}
		}
	}

	return true, newbt
}
