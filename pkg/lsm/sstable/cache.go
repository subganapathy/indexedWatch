package sstable

import "sync"

// BlockCache is a thread-safe LRU cache for SSTable data blocks.
// It is shared across all SSTable readers to avoid redundant disk reads.
//
// Cache keys are (fileID, blockOffset) pairs. Values are the raw
// uncompressed block data (without the 5-byte trailer).
type BlockCache struct {
	mu       sync.Mutex
	capacity int
	size     int
	items    map[cacheKey]*lruEntry
	head     *lruEntry // most recently used
	tail     *lruEntry // least recently used
}

type cacheKey struct {
	fileID      uint64
	blockOffset uint64
}

type lruEntry struct {
	key  cacheKey
	data []byte
	prev *lruEntry
	next *lruEntry
}

// NewBlockCache creates a new LRU block cache with the given capacity in bytes.
func NewBlockCache(capacity int) *BlockCache {
	return &BlockCache{
		capacity: capacity,
		items:    make(map[cacheKey]*lruEntry),
	}
}

// Get retrieves a cached block. Returns nil if not found.
func (c *BlockCache) Get(fileID, blockOffset uint64) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{fileID, blockOffset}
	entry, ok := c.items[key]
	if !ok {
		return nil
	}
	c.moveToFront(entry)
	return entry.data
}

// Set adds a block to the cache. Evicts LRU entries if necessary.
func (c *BlockCache) Set(fileID, blockOffset uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{fileID, blockOffset}

	// Already cached — move to front.
	if entry, ok := c.items[key]; ok {
		c.moveToFront(entry)
		return
	}

	// Add new entry.
	entry := &lruEntry{key: key, data: data}
	c.items[key] = entry
	c.pushFront(entry)
	c.size += len(data)

	// Evict LRU entries until within capacity.
	for c.size > c.capacity && c.tail != nil {
		c.evictTail()
	}
}

// Evict removes all cached blocks for the given file.
func (c *BlockCache) Evict(fileID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect keys to delete (can't modify map during iteration safely
	// while also modifying the linked list).
	var toDelete []cacheKey
	for key := range c.items {
		if key.fileID == fileID {
			toDelete = append(toDelete, key)
		}
	}
	for _, key := range toDelete {
		entry := c.items[key]
		c.remove(entry)
		delete(c.items, key)
		c.size -= len(entry.data)
	}
}

func (c *BlockCache) pushFront(entry *lruEntry) {
	entry.prev = nil
	entry.next = c.head
	if c.head != nil {
		c.head.prev = entry
	}
	c.head = entry
	if c.tail == nil {
		c.tail = entry
	}
}

func (c *BlockCache) moveToFront(entry *lruEntry) {
	if entry == c.head {
		return
	}
	c.remove(entry)
	c.pushFront(entry)
}

func (c *BlockCache) remove(entry *lruEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
	entry.prev = nil
	entry.next = nil
}

func (c *BlockCache) evictTail() {
	if c.tail == nil {
		return
	}
	entry := c.tail
	c.remove(entry)
	delete(c.items, entry.key)
	c.size -= len(entry.data)
}
