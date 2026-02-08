package skiplist

import "errors"

// ErrRecordExists indicates that a key already exists in the skiplist.
var ErrRecordExists = errors.New("skiplist: record with this key already exists")
