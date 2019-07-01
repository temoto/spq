package spq

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	q, tmpdir := testNew(t)
	defer os.RemoveAll(tmpdir)
	box := testPushPeek(t, q)
	require.NoError(t, q.Delete(box))
	box = testPushPeek(t, q)
	require.NoError(t, q.Delete(box))

	for i := uint32(77); i <= 111; i++ {
		err := q.MarshalPush(testItem(i))
		require.NoError(t, err)
	}
	for i := uint32(77); i <= 111; i++ {
		box = testPeekExpect(t, q, testItem(i))
		require.NoError(t, q.Delete(box))
	}
}

func TestPushStopPushPop(t *testing.T) {
	q, tmpdir := testNew(t)
	defer os.RemoveAll(tmpdir)
	require.NoError(t, q.MarshalPush(testItem(101)))
	require.NoError(t, q.MarshalPush(testItem(202)))
	dbList(t, q)
	q.Close()

	q, err := Open(tmpdir)
	require.NoError(t, err)
	require.NoError(t, q.MarshalPush(testItem(303)))
	testPeekExpectDelete(t, q, testItem(101))
	testPeekExpectDelete(t, q, testItem(202))
	testPeekExpectDelete(t, q, testItem(303))
}

func TestConcurrent(t *testing.T) {
	const n = 100

	q, tmpdir := testNew(t)
	defer os.RemoveAll(tmpdir)

	rand := RandUnix()
	begin := rand.Uint32()
	next := begin
	var wg sync.WaitGroup
	reads := make(chan testItem, n)
	checkerr := func(err error) bool {
		if err == nil {
			return true
		} else if err == ErrClosed {
			return false
		} else {
			return assert.NoError(t, err)
		}
	}
	producer := func() {
		defer wg.Done()
		r := RandUnix()
		time.Sleep(time.Duration(r.Uint32()%1000) * time.Microsecond)
		n := atomic.AddUint32(&next, 1)
		checkerr(q.MarshalPush(testItem(n)))
	}
	consumer := func() {
		defer wg.Done()
		r := RandUnix()
		time.Sleep(time.Duration(r.Uint32()%1000) * time.Microsecond)
		box, err := q.Peek()
		if checkerr(err) {
			var x testItem
			if assert.NoError(t, box.Unmarshal(&x)) {
				reads <- x
				checkerr(q.Delete(box))
			}
		}
	}

	wg.Add(n * 2)
	for i := 1; i <= n; i++ {
		go producer()
		go consumer()
	}
	time.AfterFunc(time.Duration(rand.Uint32()%1000)*time.Microsecond, func() {
		require.NoError(t, q.Close())
	})
	wg.Wait()
	close(reads)

	for ti := range reads {
		assert.True(t, uint32(ti)-begin <= n)
	}
}

func BenchmarkFull(b *testing.B) {
	for _, vsize := range []int{32, 128, 512} {
		b.Run(strconv.Itoa(vsize), func(b *testing.B) {
			q, tmpdir := testNew(b)
			defer os.RemoveAll(tmpdir)
			sameValue := make([]byte, vsize)
			b.ReportAllocs()
			b.SetBytes(int64(vsize))
			b.ResetTimer()
			for i := 1; i <= b.N; i++ {
				err := q.Push(sameValue)
				require.NoError(b, err)
			}
			for i := 1; i <= b.N; i++ {
				box, err := q.Peek()
				require.NoError(b, err)
				err = q.Delete(box)
				require.NoError(b, err)
			}
			b.StopTimer()
		})
	}
}
