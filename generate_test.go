package main

import "time"
import "reflect"
import "testing"
import "strconv"
import "math/rand"

func TestRandIntnUnique(t *testing.T) {
	s := rand.NewSource(100)
	rnd, n := rand.New(s), 10000000
	m := map[int]bool{}
	now := time.Now()
	for i := 0; i < n; i++ {
		m[rnd.Intn(n)] = true
	}
	fmsg := "Took %v to populate %v(%v) items in map"
	t.Logf(fmsg, time.Since(now), len(m), n)
}

func TestRandSource(t *testing.T) {
	rnd := rand.New(rand.NewSource(100))
	first := []int{}
	for i := 0; i < 10; i++ {
		first = append(first, rnd.Intn(10000000))
	}
	rnd = rand.New(rand.NewSource(100))
	second := []int{}
	for i := 0; i < 10; i++ {
		second = append(second, rnd.Intn(10000000))
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("%v != %v", first, second)
	}
}

func TestGenerateloads(t *testing.T) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	g := Generateloads(klen, vlen, n)
	m, now, lastkey, opaque := map[int64]bool{}, time.Now(), int64(-1), uint64(1)
	key, value := g(nil, nil, opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if keynum <= lastkey {
			t.Fatalf("generated key %s is <= %v", key, lastkey)
		} else if _, ok := m[keynum]; ok {
			t.Fatalf("duplicated key %s", key)
		}
		m[keynum], lastkey = true, keynum
		opaque++
	}
	if int64(len(m)) != n {
		t.Fatalf("expected %v, got %v", n, len(m))
	}
	tm := time.Since(now).Round(time.Second)
	t.Logf("Took %v to load %v items", tm, len(m))
}

func TestGenerateloadr(t *testing.T) {
	klen, vlen, n, seed := int64(32), int64(32), int64(1*1000*1000), int64(100)
	g := Generateloadr(klen, vlen, n, seed)
	m, now, opaque := map[int64]bool{}, time.Now(), uint64(1)
	key, value := g(nil, nil, opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := m[keynum]; ok {
			t.Fatalf("duplicate key %s", key)
		}
		m[keynum] = true
		opaque++
	}
	if int64(len(m)) != n {
		t.Fatalf("expected %v, got %v", n, len(m))
	}
	tm := time.Since(now).Round(time.Second)
	t.Logf("Took %v to load %v items", tm, len(m))
}

func TestGenerateCRUD(t *testing.T) {
	loadm := map[int64]bool{}
	loadmaxkey := int64(0)

	// Initial load
	klen, vlen, loadn, seedl := int64(32), int64(32), int64(1000000), int64(100)
	g := Generateloadr(klen, vlen, loadn, seedl)
	opaque := uint64(1)
	key, value := g(nil, nil, opaque)
	for ; key != nil; key, value = g(key, value, opaque) {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		loadm[keynum] = true
		if keynum > loadmaxkey {
			loadmaxkey = keynum
		}
		opaque++
	}
	if int64(len(loadm)) != loadn {
		t.Fatalf("expected %v, got %v", loadn, len(loadm))
	}

	createm := map[int64]bool{}
	// Create load
	klen, vlen = int64(32), int64(32)
	createn, seedc, opaque := int64(2000000), int64(200), uint64(1)
	g = Generatecreate(klen, vlen, loadn, seedc)
	key, value = g(nil, nil, opaque)
	for i := int64(0); i < createn; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := createm[keynum]; ok {
			t.Fatalf("duplicate key %s in create path", key)
		} else if _, ok := loadm[keynum]; ok {
			t.Fatalf("duplicate key %s in load path", key)
		} else if keynum <= loadmaxkey {
			t.Fatalf("%v <= %v", keynum, loadmaxkey)
		}
		opaque++
		createm[keynum] = true
		key, value = g(key, value, opaque)
	}
	if int64(len(createm)) != createn {
		t.Fatalf("expected %v, got %v", createn, len(createm))
	}

	readl, readc := map[int64]bool{}, map[int64]bool{}
	// read load
	klen, readn := int64(32), int64(1100000)
	gr := Generateread(klen, loadn, seedl, seedc)
	key = gr(nil, createn)
	for i := int64(0); i < readn; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			readl[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			readc[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		key = gr(key, createn)
	}
	if len(readl) != 850300 {
		t.Fatalf("%v != %v", len(readl), 850300)
	} else if len(readc) != 50000 {
		t.Fatalf("%v != %v", len(readc), 50000)
	}

	updatel, updatec := map[int64]bool{}, map[int64]bool{}
	// update load
	klen, vlen, updaten := int64(32), int64(32), int64(1100000)
	opaque = uint64(1)
	g = Generateupdate(klen, vlen, loadn, seedl, seedc, -1)
	key, value = g(nil, nil, opaque)
	for i := int64(0); i < updaten; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			updatel[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			updatec[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		opaque++
		key, value = g(key, value, opaque)
	}
	if len(updatel) != 850300 {
		t.Fatalf("%v != %v", len(updatel), 850300)
	} else if len(updatec) != 33333 {
		t.Fatalf("%v != %v", len(updatec), 33333)
	}

	deletel, deletec := map[int64]bool{}, map[int64]bool{}
	// delete load
	klen, deleten, opaque := int64(32), int64(1000000), uint64(1)
	gd := Generatedelete(klen, vlen, loadn, seedl, seedc, delmod)
	key, value = gd(nil, nil, opaque)
	for i := int64(0); i < deleten; i++ {
		keynum, err := strconv.ParseInt(Bytes2str(key), 10, 64)
		if err != nil {
			t.Fatal(err)
		} else if _, ok := loadm[keynum]; ok {
			deletel[keynum] = true
		} else if _, ok := createm[keynum]; ok {
			deletec[keynum] = true
		} else {
			t.Fatalf("generated key %s not found", key)
		}
		opaque++
		key, value = gd(key, value, opaque)
	}
	if len(deletel) != 424961 {
		t.Fatalf("%v != %v", len(deletel), 424961)
	} else if len(deletec) != 166570 {
		t.Fatalf("%v != %v", len(deletec), 166570)
	}
}

func BenchmarkGenerateloads(b *testing.B) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	opaque := uint64(1)
	g := Generateloads(klen, vlen, n)
	key, value := g(nil, nil, opaque)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value, opaque+uint64(i))
	}
}

func BenchmarkGenerateloadr(b *testing.B) {
	klen, vlen, n, seed := int64(32), int64(32), int64(1*1000*1000), int64(100)
	opaque := uint64(1)
	g := Generateloadr(klen, vlen, n, seed)
	key, value := g(nil, nil, opaque)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value, opaque+uint64(i))
	}
}

func BenchmarkGeneratecreate(b *testing.B) {
	klen, vlen, n, seedc := int64(32), int64(32), int64(1*1000*1000), int64(100)
	opaque := uint64(1)
	g := Generatecreate(klen, vlen, n, seedc)
	key, value := g(nil, nil, opaque)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value, opaque+uint64(i))
	}
}

func BenchmarkGenerateupdate(b *testing.B) {
	klen, vlen, n := int64(32), int64(32), int64(1*1000*1000)
	seedl, seedc, opaque := int64(100), int64(200), uint64(1)
	g := Generateupdate(klen, vlen, n, seedl, seedc, -1)
	key, value := g(nil, nil, opaque)
	for i := 0; i < b.N; i++ {
		key, value = g(key, value, opaque+uint64(i))
	}
}

func BenchmarkGenerateread(b *testing.B) {
	klen, n := int64(32), int64(1*1000*1000)
	seedl, seedc := int64(100), int64(200)
	g := Generateread(klen, n, seedl, seedc)
	key := g(nil, 0)
	for i := 0; i < b.N; i++ {
		g(key, int64(i+1))
	}
}

func BenchmarkGeneratedelete(b *testing.B) {
	klen, n := int64(32), int64(1*1000*1000)
	seedl, seedc, opaque := int64(100), int64(200), uint64(1)
	g := Generatedelete(klen, 0, n, seedl, seedc, delmod)
	key, value := g(nil, nil, opaque)
	for i := 0; i < b.N; i++ {
		g(key, value, opaque+uint64(i))
	}
}

func BenchmarkSourceInt63(b *testing.B) {
	s := rand.NewSource(100)
	for i := 0; i < b.N; i++ {
		s.Int63()
	}
}

func BenchmarkRandIntn(b *testing.B) {
	s := rand.NewSource(100)
	rnd := rand.New(s)
	for i := 0; i < b.N; i++ {
		rnd.Intn(1 * 1000 * 1000)
	}
}
