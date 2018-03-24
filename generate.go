package main

import "fmt"
import "strconv"
import "encoding/binary"
import "math/rand"

const minkeysize = 16

// Generate presorted load, always return unique key, off fixed size
// key and value, return nil after `n` keys.
func Generateloads(
	klen, vlen, n int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte

	keynum := int64(0)
	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		if keynum >= n {
			return nil, nil
		}
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		// create value
		value = makevalue(len(key), vlen, ascii, value, opaque, 'l', false)

		keynum++
		return key, value
	}
}

// Generate unsorted load, always return unique key, variable width
// key and value, return nill after `n` keys.
func Generateloadr(
	klen, vlen, n, seed int64,
	randwidth bool) func([]byte, []byte, uint64) ([]byte, []byte) {

	var text [16]byte
	var ascii []byte

	intn := n * rndscale
	rnd := rand.New(rand.NewSource(seed))
	bitmap := make([]byte, ((intn / 8) + 1))

	count := int64(0)
	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		if count >= n {
			return nil, nil
		}
		ascii, key = makeuniquekey(
			rnd, bitmap, 0 /*offset*/, intn, klen, text[:0], key, randwidth,
		)
		//fmt.Printf("load %q\n", key)
		value = makevalue(len(key), vlen, ascii, value, opaque, 'l', randwidth)

		count++
		return key, value
	}
}

// Generate keys greater than loadn, always return unique keys.
func Generatecreate(
	klen, vlen, loadn, insertn,
	seed int64, randwidth bool) func([]byte, []byte, uint64) ([]byte, []byte) {

	var text [1024]byte
	var ascii []byte

	loadn = int64(loadn * rndscale)
	intn := insertn * rndscale
	rnd := rand.New(rand.NewSource(seed))
	bitmap := make([]byte, ((intn / 8) + 1))

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		ascii, key = makeuniquekey(
			rnd, bitmap, loadn, intn, klen, text[:0], key, randwidth,
		)
		//fmt.Printf("create %q\n", key)
		value = makevalue(len(key), vlen, ascii, value, opaque, 'c', randwidth)
		return key, value
	}
}

func Generateupdate(
	klen, vlen, loadn, insertn,
	seedl, seedc, mod int64,
	randwidth bool) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [1024]byte
	var getkey func() int64

	loadn1 := int64(loadn * rndscale)
	intn := insertn * rndscale
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	lcount := int64(0)

	getkey = func() (keynum int64) {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if (lcount % 3) == 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
		}
		if mod >= 0 && (keynum%2) != mod {
			return getkey()
		}
		return keynum
	}

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		keynum := getkey()
		ascii, key := makekey(keynum, klen, textint[:0], key, randwidth)
		//fmt.Printf("update %q\n", key)
		value = makevalue(len(key), vlen, ascii, value, opaque, 'l', randwidth)
		return key, value
	}
}

func Generateread(
	klen, loadn, insertn, seedl, seedc int64,
	randwidth bool) func([]byte, int64) []byte {

	var textint [16]byte
	var getkey func(int64) int64

	loadn1 := loadn * rndscale
	intn := insertn * rndscale
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	lcount := int64(0)

	getkey = func(mod int64) (keynum int64) {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if mod > 0 && (lcount%mod) != 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
			rndc = rand.New(rand.NewSource(seedc))
		}
		return keynum
	}

	return func(key []byte, ncreates int64) []byte {
		keynum := getkey(ncreates / loadn)
		_, key = makekey(keynum, klen, textint[:0], key, randwidth)
		return key
	}
}

func Generatereadseq(klen, loadn, seedl int64) func([]byte, int64) []byte {
	var textint [16]byte
	var getkey func(int64) int64

	rndl := rand.New(rand.NewSource(seedl))
	lcount := int64(0)

	getkey = func(mod int64) (keynum int64) {
		keynum = int64(rndl.Intn(int(loadn)))
		lcount++
		return keynum
	}

	return func(key []byte, ncreates int64) []byte {
		keynum := getkey(ncreates / loadn)
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		return key
	}
}

func Generatedelete(
	klen, vlen, loadn, insertn, seedl, seedc,
	mod int64, randwidth bool) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var getkey func() int64

	loadn1 := loadn * rndscale
	intn := insertn * rndscale
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	lcount := int64(0)

	getkey = func() (keynum int64) {
		if lcount < loadn { // from load pool, headstart
			keynum = int64(rndl.Intn(int(loadn1)))
		} else if (lcount % 3) == 0 { // from create pool
			keynum = loadn1 + int64(rndc.Intn(int(intn)))
		} else { // from load pool
			keynum = int64(rndl.Intn(int(loadn1)))
		}
		lcount++
		if lcount >= loadn && (lcount%loadn) == 0 {
			rndl = rand.New(rand.NewSource(seedl))
		}
		if mod >= 0 && (keynum%2) != mod {
			return getkey()
		}
		return keynum
	}

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		keynum := getkey()
		ascii, key := makekey(keynum, klen, textint[:0], key, randwidth)
		//fmt.Printf("delete %q\n", key)
		value = makevalue(len(key), vlen, ascii, value, opaque, 'l', randwidth)
		return key, value
	}
}

var rndscale = int64(3)
var bitmask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
var zeros = make([]byte, 4096)

func makeuniquekey(
	rnd *rand.Rand, bitmap []byte, offset, intn, klen int64,
	textint, key []byte, randwidth bool) ([]byte, []byte) {

	for true {
		keynum := int64(rnd.Intn(int(intn)))
		if (bitmap[keynum/8] & bitmask[keynum%8]) == 0 {
			bitmap[keynum/8] |= bitmask[keynum%8]
			keynum += offset
			ascii := strconv.AppendInt(textint[:0], keynum, 10)
			// create key
			if randwidth {
				klen = minkeysize + (keynum % klen)
			} else if klen < minkeysize {
				klen = minkeysize
			}
			key = Fixbuffer(key, int64(klen))
			copy(key, zeros)
			copy(key[klen-int64(len(ascii)):klen], ascii)
			return ascii, key
		}
	}
	panic(fmt.Errorf("unreachable code"))
}

func makekey(
	keynum, klen int64, textint, key []byte, randwidth bool) ([]byte, []byte) {

	ascii := strconv.AppendInt(textint[:0], keynum, 10)
	// create key
	if randwidth {
		klen = minkeysize + (keynum % klen)
	} else if klen < minkeysize {
		klen = minkeysize
	}
	key = Fixbuffer(key, int64(klen))
	copy(key, zeros)
	copy(key[klen-int64(len(ascii)):klen], ascii)
	return ascii, key
}

func makevalue(
	klen int, vlen int64,
	ascii, value []byte, opaque uint64, ch byte, randwidth bool) []byte {

	if vlen == 0 {
		return value
	}

	if randwidth {
		vlen = int64(klen) + (int64(opaque) % vlen)
	} else if vlen < int64(klen) {
		vlen = int64(klen)
	}

	var opqbin [8]byte
	value = Fixbuffer(value, int64(vlen))
	copy(value, zeros)
	copy(value[vlen-int64(len(ascii)):vlen], ascii)
	if opaque > 0 {
		binary.BigEndian.PutUint64(opqbin[:], opaque)
		opqbin[0] = ch
		value = append(value, opqbin[:]...)
	}
	return value
}

func init() {
	for i := range zeros {
		zeros[i] = '0'
	}
}
