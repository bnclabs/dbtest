package main

import "fmt"
import "strconv"
import "encoding/binary"
import "math/rand"

var _ = fmt.Sprintf("dummy")

// Generate presorted load, always return unique key,
// return nil after `n` keys.
func Generateloads(
	klen, vlen, n int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var opqbin [8]byte

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
		if vlen > 0 {
			value = Fixbuffer(value, int64(vlen))
			copy(value, zeros)
			copy(value[vlen-int64(len(ascii)):vlen], ascii)
			if opaque > 0 {
				binary.BigEndian.PutUint64(opqbin[:], opaque)
				value = append(value, opqbin[:]...)
			}
		}

		keynum++
		return key, value
	}
}

// Generate unsorted load, always return unique key,
// return nill after `n` keys.
func Generateloadr(
	klen, vlen, n, seed int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var opqbin [8]byte

	intn := n * rndscale
	rnd := rand.New(rand.NewSource(seed))
	bitmap := make([]byte, ((intn / 8) + 1))

	count := int64(0)
	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		if count >= n {
			return nil, nil
		}
		keynum := makeuniquekey(rnd, bitmap, intn)
		ascii := strconv.AppendInt(textint[:0], keynum, 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		//fmt.Printf("load %q\n", key)
		// create value
		if vlen > 0 {
			value = Fixbuffer(value, int64(vlen))
			copy(value, zeros)
			copy(value[vlen-int64(len(ascii)):vlen], ascii)
			if opaque > 0 {
				binary.BigEndian.PutUint64(opqbin[:], opaque)
				opqbin[0] = 'l'
				value = append(value, opqbin[:]...)
			}
		}

		count++
		return key, value
	}
}

// Generate keys greater than loadn, always return unique keys.
func Generatecreate(
	klen, vlen, loadn,
	seed int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var opqbin [8]byte

	loadn = int64(loadn * rndscale)
	intn := int64(9223372036854775) - loadn
	rnd := rand.New(rand.NewSource(seed))

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		keynum := int64(rnd.Intn(int(intn))) + loadn
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		//fmt.Printf("create %q\n", key)
		// create value
		if vlen > 0 {
			value = Fixbuffer(value, int64(vlen))
			copy(value, zeros)
			copy(value[vlen-int64(len(ascii)):vlen], ascii)
			if opaque > 0 {
				binary.BigEndian.PutUint64(opqbin[:], opaque)
				opqbin[0] = 'c'
				value = append(value, opqbin[:]...)
			}
		}
		return key, value
	}
}

func Generateupdate(
	klen, vlen, loadn,
	seedl, seedc, mod int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var opqbin [8]byte
	var getkey func()

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func() {
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
			getkey()
		}
	}

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		getkey()
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		//fmt.Printf("update %q\n", key)
		// create value
		if vlen > 0 {
			value = Fixbuffer(value, int64(vlen))
			copy(value, zeros)
			copy(value[vlen-int64(len(ascii)):vlen], ascii)
			if opaque > 0 {
				binary.BigEndian.PutUint64(opqbin[:], opaque)
				opqbin[0] = 'u'
				value = append(value, opqbin[:]...)
			}
		}
		return key, value
	}
}

func Generateread(klen, loadn, seedl, seedc int64) func([]byte, int64) []byte {
	var textint [16]byte
	var getkey func(int64)

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func(mod int64) {
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
	}

	return func(key []byte, ncreates int64) []byte {
		getkey(ncreates / loadn)
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		return key
	}
}

func Generatereadseq(klen, loadn, seedl int64) func([]byte, int64) []byte {
	var textint [16]byte
	var getkey func(int64)

	rndl := rand.New(rand.NewSource(seedl))
	keynum, lcount := int64(0), int64(0)

	getkey = func(mod int64) {
		keynum = int64(rndl.Intn(int(loadn)))
		lcount++
	}

	return func(key []byte, ncreates int64) []byte {
		getkey(ncreates / loadn)
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		return key
	}
}

func Generatedelete(
	klen, vlen, loadn, seedl, seedc,
	mod int64) func([]byte, []byte, uint64) ([]byte, []byte) {

	var textint [16]byte
	var opqbin [8]byte
	var getkey func()

	loadn1 := loadn * rndscale
	intn := int64(9223372036854775) - loadn1
	rndl := rand.New(rand.NewSource(seedl))
	rndc := rand.New(rand.NewSource(seedc))
	keynum, lcount := int64(0), int64(0)

	getkey = func() {
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
			getkey()
		}
	}

	return func(key, value []byte, opaque uint64) ([]byte, []byte) {
		getkey()
		ascii := strconv.AppendInt(textint[:0], int64(keynum), 10)
		// create key
		key = Fixbuffer(key, int64(klen))
		copy(key, zeros)
		copy(key[klen-int64(len(ascii)):klen], ascii)
		//fmt.Printf("delete %q\n", key)
		// create value
		if vlen > 0 {
			value = Fixbuffer(value, int64(vlen))
			copy(value, zeros)
			copy(value[vlen-int64(len(ascii)):vlen], ascii)
			if opaque > 0 {
				binary.BigEndian.PutUint64(opqbin[:], opaque)
				opqbin[0] = 'd'
				value = append(value, opqbin[:]...)
			}
		}
		return key, value
	}
}

var rndscale = int64(3)
var bitmask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}
var zeros = make([]byte, 4096)

func makeuniquekey(rnd *rand.Rand, bitmap []byte, intn int64) int64 {
	for true {
		keynum := int64(rnd.Intn(int(intn)))
		if (bitmap[keynum/8] & bitmask[keynum%8]) == 0 {
			bitmap[keynum/8] |= bitmask[keynum%8]
			return keynum
		}
	}
	panic("unreachable code")
}

func init() {
	for i := range zeros {
		zeros[i] = '0'
	}
}
