package spq

import (
	"encoding/binary"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testDirPrefix = "test-pq-delete-this-"

type testItem uint32

func (ti testItem) MarshalBinary() ([]byte, error) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(ti))
	return b[:], nil
}
func (ti *testItem) UnmarshalBinary(b []byte) error {
	*ti = testItem(binary.BigEndian.Uint32(b))
	return nil
}

func testNew(t testing.TB) (*Queue, string) {
	tmpdir, err := ioutil.TempDir("", testDirPrefix)
	require.NoError(t, err)
	q, err := Open(tmpdir)
	require.NoError(t, err)
	return q, tmpdir
}

func RandUnix() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func testPush(t testing.TB, q *Queue) testItem {
	rand := RandUnix()

	expect := testItem(rand.Uint32())
	err := q.MarshalPush(expect)
	require.NoError(t, err)
	return expect
}

func testPushPeek(t testing.TB, q *Queue) Box {
	expect := testPush(t, q)
	return testPeekExpect(t, q, expect)
}

func testPeekExpect(t testing.TB, q *Queue, expect testItem) Box {
	box, err := q.Peek()
	require.NoError(t, err)
	eb, err := expect.MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, eb, box.Bytes())
	return box
}

func testPeekExpectDelete(t testing.TB, q *Queue, expect testItem) {
	box := testPeekExpect(t, q, expect)
	err := q.Delete(box)
	require.NoError(t, err)
}

func dbList(t testing.TB, q *Queue) {
	iter := q.db.NewIterator(nil, &q.dbROpt)
	for iter.Next() {
		// stderr is less likely to disappear with panics
		// fmt.Printf("db - %x = %x\n", iter.Key(), iter.Value())
		t.Logf("db - %x = %x\n", iter.Key(), iter.Value())
	}
	iter.Release()
	require.NoError(t, iter.Error())
}
