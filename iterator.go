// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"

	"github.com/petermattis/pebble/db"
)

type iterPos int8

const (
	iterPosCur  iterPos = 0
	iterPosNext iterPos = 1
	iterPosPrev iterPos = -1
)

// Iterator iterates over a DB's key/value pairs in key order.
//
// An iterator must be closed after use, but it is not necessary to read an
// iterator until exhaustion.
//
// An iterator is not necessarily goroutine-safe, but it is safe to use
// multiple iterators concurrently, with each in a dedicated goroutine.
//
// It is also safe to use an iterator concurrently with modifying its
// underlying DB, if that DB permits modification. However, the resultant
// key/value pairs are not guaranteed to be a consistent snapshot of that DB
// at a particular point in time.
type Iterator struct {
	opts      *db.IterOptions
	cmp       db.Compare
	equal     db.Equal
	merge     db.Merge
	iter      internalIterator
	readState *readState
	err       error
	key       []byte
	keyBuf    []byte
	value     []byte
	valueBuf  []byte
	valueBuf2 []byte
	valid     bool
	iterKey   *db.InternalKey
	iterValue []byte
	pos       iterPos
	alloc     *iterAlloc
}

func (i *Iterator) findNextEntry() bool {
	i.valid = false
	i.pos = iterPosCur

	for i.iterKey != nil {
		key := *i.iterKey
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			i.nextUserKey()
			continue

		case db.InternalKeyKindRangeDelete:
			// Range deletions are treated as no-ops. See the comments in levelIter
			// for more details.
			i.iterKey, i.iterValue = i.iter.Next()
			continue

		case db.InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.valid = true
			return true

		case db.InternalKeyKindMerge:
			return i.mergeNext(key)

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}

	return false
}

func (i *Iterator) nextUserKey() {
	if i.iterKey != nil {
		done := i.iterKey.SeqNum() == 0
		if !i.valid {
			i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
			i.key = i.keyBuf
		}
		for {
			i.iterKey, i.iterValue = i.iter.Next()
			if done || i.iterKey == nil || !i.equal(i.key, i.iterKey.UserKey) {
				break
			}
			done = i.iterKey.SeqNum() == 0
		}
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
}

func (i *Iterator) findPrevEntry() bool {
	i.valid = false
	i.pos = iterPosCur

	for i.iterKey != nil {
		key := *i.iterKey
		if i.valid {
			if !i.equal(key.UserKey, i.key) {
				// We've iterated to the previous user key.
				i.pos = iterPosPrev
				return true
			}
		}

		switch key.Kind() {
		case db.InternalKeyKindDelete:
			i.value = nil
			i.valid = false
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		case db.InternalKeyKindRangeDelete:
			// Range deletions are treated as no-ops. See the comments in levelIter
			// for more details.
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		case db.InternalKeyKindSet:
			i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
			i.key = i.keyBuf
			i.value = i.iterValue
			i.valid = true
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		case db.InternalKeyKindMerge:
			if !i.valid {
				i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
				i.key = i.keyBuf
				i.value = i.iterValue
				i.valid = true
			} else {
				// The existing value is either stored in valueBuf2 or the underlying
				// iterators value. We append the new value to valueBuf in order to
				// merge(valueBuf, valueBuf2). Then we swap valueBuf and valueBuf2 in
				// order to maintain the invariant that the existing value points to
				// valueBuf2 (in preparation for handling th next merge value).
				i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
				i.valueBuf = i.merge(i.key, i.valueBuf, i.value, nil)
				i.valueBuf, i.valueBuf2 = i.valueBuf2, i.valueBuf
				i.value = i.valueBuf2
			}
			i.iterKey, i.iterValue = i.iter.Prev()
			continue

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}

	if i.valid {
		i.pos = iterPosPrev
		return true
	}

	return false
}

func (i *Iterator) prevUserKey() {
	if i.iterKey != nil {
		if !i.valid {
			// If we're going to compare against the prev key, we need to save the
			// current key.
			i.keyBuf = append(i.keyBuf[:0], i.iterKey.UserKey...)
			i.key = i.keyBuf
		}
		for {
			i.iterKey, i.iterValue = i.iter.Prev()
			if i.iterKey == nil || !i.equal(i.key, i.iterKey.UserKey) {
				break
			}
		}
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
}

func (i *Iterator) mergeNext(key db.InternalKey) bool {
	// Save the current key and value.
	i.keyBuf = append(i.keyBuf[:0], key.UserKey...)
	i.valueBuf = append(i.valueBuf[:0], i.iterValue...)
	i.key, i.value = i.keyBuf, i.valueBuf
	i.valid = true

	// Loop looking for older values for this key and merging them.
	for {
		i.iterKey, i.iterValue = i.iter.Next()
		if i.iterKey == nil {
			i.pos = iterPosNext
			return true
		}
		key = *i.iterKey
		if !i.equal(i.key, key.UserKey) {
			// We've advanced to the next key.
			i.pos = iterPosNext
			return true
		}
		switch key.Kind() {
		case db.InternalKeyKindDelete:
			// We've hit a deletion tombstone. Return everything up to this
			// point.
			return true

		case db.InternalKeyKindRangeDelete:
			// Range deletions are treated as no-ops. See the comments in levelIter
			// for more details.
			continue

		case db.InternalKeyKindSet:
			// We've hit a Set value. Merge with the existing value and return.
			i.value = i.merge(i.key, i.value, i.iterValue, nil)
			return true

		case db.InternalKeyKindMerge:
			// We've hit another Merge value. Merge with the existing value and
			// continue looping.
			i.value = i.merge(i.key, i.value, i.iterValue, nil)
			i.valueBuf = i.value[:0]
			continue

		default:
			i.err = fmt.Errorf("invalid internal key kind: %d", key.Kind())
			return false
		}
	}
}

// SeekGE moves the iterator to the first key/value pair whose key is greater
// than or equal to the given key. Returns true if the iterator is pointing at
// a valid entry and false otherwise.
func (i *Iterator) SeekGE(key []byte) bool {
	if i.err != nil {
		return false
	}

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil && i.cmp(key, lowerBound) < 0 {
		key = lowerBound
	}

	i.iterKey, i.iterValue = i.iter.SeekGE(key)
	return i.findNextEntry()
}

// SeekLT moves the iterator to the last key/value pair whose key is less than
// the given key. Returns true if the iterator is pointing at a valid entry and
// false otherwise.
func (i *Iterator) SeekLT(key []byte) bool {
	if i.err != nil {
		return false
	}

	if upperBound := i.opts.GetUpperBound(); upperBound != nil && i.cmp(key, upperBound) >= 0 {
		key = upperBound
	}

	i.iterKey, i.iterValue = i.iter.SeekLT(key)
	return i.findPrevEntry()
}

// First moves the iterator the the first key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) First() bool {
	if i.err != nil {
		return false
	}

	if lowerBound := i.opts.GetLowerBound(); lowerBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekGE(lowerBound)
	} else {
		i.iterKey, i.iterValue = i.iter.First()
	}
	return i.findNextEntry()
}

// Last moves the iterator the the last key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Last() bool {
	if i.err != nil {
		return false
	}

	if upperBound := i.opts.GetUpperBound(); upperBound != nil {
		i.iterKey, i.iterValue = i.iter.SeekLT(upperBound)
	} else {
		i.iterKey, i.iterValue = i.iter.Last()
	}
	return i.findPrevEntry()
}

// Next moves the iterator to the next key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case iterPosCur:
		i.nextUserKey()
	case iterPosPrev:
		// The underlying iterator is pointed to the previous key (this can only
		// happen when switching iteration directions). We set i.valid to false
		// here to force the calls to nextUserKey to save the current key i.iter is
		// pointing at in order to determine when the next user-key is reached.
		//
		// TODO(peter): This is subtle. Perhaps we should provide a special version
		// of nextUserKey which advances until a larger user key is found.
		i.valid = false
		i.nextUserKey()
		i.nextUserKey()
	case iterPosNext:
	}
	return i.findNextEntry()
}

// Prev moves the iterator to the previous key/value pair. Returns true if the
// iterator is pointing at a valid entry and false otherwise.
func (i *Iterator) Prev() bool {
	if i.err != nil {
		return false
	}
	switch i.pos {
	case iterPosCur:
		i.prevUserKey()
	case iterPosNext:
		// The underlying iterator is pointed to the next key (this can only happen
		// when switching iteration directions). We set i.valid to false here to
		// force the calls to prevUserKey to save the current key i.iter is
		// pointing at in order to determine when the next user-key is reached.
		//
		// TODO(peter): This is subtle. Perhaps we should provide a special version
		// of prevUserKey which advances until a smaller user key is found.
		i.valid = false
		i.prevUserKey()
		i.prevUserKey()
	case iterPosPrev:
	}
	return i.findPrevEntry()
}

// Key returns the key of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
func (i *Iterator) Key() []byte {
	return i.key
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its
// contents may change on the next call to Next.
func (i *Iterator) Value() []byte {
	return i.value
}

// Valid returns true if the iterator is positioned at a valid key/value pair
// and false otherwise.
func (i *Iterator) Valid() bool {
	return i.valid
}

// Error returns any accumulated error.
func (i *Iterator) Error() error {
	return i.err
}

// Close closes the iterator and returns any accumulated error. Exhausting
// all the key/value pairs in a table is not considered to be an error.
// It is valid to call Close multiple times. Other methods should not be
// called after the iterator has been closed.
func (i *Iterator) Close() error {
	if i.readState != nil {
		i.readState.unref()
		i.readState = nil
	}
	if err := i.iter.Close(); err != nil && i.err != nil {
		i.err = err
	}
	err := i.err
	if alloc := i.alloc; alloc != nil {
		*i = Iterator{}
		iterAllocPool.Put(alloc)
	}
	return err
}
