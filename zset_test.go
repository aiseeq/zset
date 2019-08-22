package zset

import (
	"math/rand"
	"testing"
)

var s *SortedSet

func init() {
	s = New()
}

func TestNew(t *testing.T) {
	if s == nil {
		t.Failed()
	}
	s.Set(66, 1001, "test1")
	s.Set(77, 1002, "test2")
	s.Set(88, 1003, "test3")
	s.Set(100, 1004, "liyiheng")
	s.Set(99, 1005, "test4")
	s.Set(44, 1006, "test5")
	s.Set(44, 1001, "test1")

	rank, score, extra := s.GetRank(1004, false)
	if rank == 5 {
		t.Log("Key:", 1004, "Rank:", rank, "Score:", score, "Extra:", extra)
	} else {
		t.Error("Key:", 1004, "Rank:", rank, "Score:", score, "Extra:", extra)
	}
	rank, score, extra = s.GetRank(1001, false)
	if rank == 0 {
		t.Log("Key:", 1001, "Rank:", rank, "Score:", score, "Extra:", extra)
	} else {
		t.Error("Key:", 1001, "Rank:", rank, "Score:", score, "Extra:", extra)
	}

	id, score, extra := s.GetDataByRank(0, true)
	t.Log("GetData[REVERSE] Rank:", 0, "ID:", id, "Score:", score, "Extra:", extra)
	id, score, extra = s.GetDataByRank(0, false)
	t.Log("GetData[UNREVERSE] Rank:", 0, "ID:", id, "Score:", score, "Extra:", extra)
	_, _, extra = s.GetDataByRank(9999, true)
	if extra != nil {
		t.Error("GetDataByRank is not nil")
	}
	if s.Length() != 6 {
		t.Error("Rank Data Size is wrong")
	}
	s.Delete(1001)
	if s.Length() != 5 {
		t.Error("Rank Data Size is wrong")
	}
	d, ok := s.GetData(1004)
	t.Log(d, ok)
	curScore, dat := s.IncrBy(666, 1004)
	t.Log(curScore, dat)
}

func TestIncrBy(t *testing.T) {
	z := New()
	for i := uint32(1000); i < 1100; i++ {
		z.Set(i, i, "Hello world")
	}
	rank, score, _ := z.GetRank(1050, false)
	curScore, _ := z.IncrBy(2, 1050)
	if score+2 != curScore {
		t.Error(score, curScore)
	}
	r2, score2, _ := z.GetRank(1050, false)
	if score2 != curScore {
		t.Fail()
	}
	if r2 != rank+1 {
		t.Error(r2, rank)
	}

}

func TestRange(t *testing.T) {
	z := New()
	z.Set(1, 1001, nil)
	z.Set(2, 1002, nil)
	z.Set(3, 1003, nil)
	z.Set(4, 1004, nil)
	z.Set(5, 1005, nil)
	z.Set(6, 1006, nil)

	ids := make([]uint32, 0, 6)
	z.Range(0, 10, func(score uint32, k uint32, _ interface{}) {
		ids = append(ids, k)
		t.Log(score, k)
	})
	if ids[0] != 1001 ||
		ids[1] != 1002 ||
		ids[2] != 1003 ||
		ids[3] != 1004 {
		t.Fail()
	}
	z.RevRange(1, 3, func(score uint32, k uint32, _ interface{}) {
		t.Log(score, k)
	})

}

func BenchmarkSortedSet_Add(b *testing.B) {
	b.StopTimer()
	// data initialization
	scores := make([]uint32, b.N)
	IDs := make([]uint32, b.N)
	for i := range IDs {
		scores[i] = rand.Uint32()
		IDs[i] = uint32(i + 100000)
	}
	// BCE
	_ = scores[:b.N]
	_ = IDs[:b.N]

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Set(scores[i], IDs[i], nil)
	}
}

func BenchmarkSortedSet_GetRank(b *testing.B) {
	l := s.Length()
	for i := 0; i < b.N; i++ {
		s.GetRank(100000+uint32(i)%l, true)
	}
}

func BenchmarkSortedSet_GetDataByRank(b *testing.B) {
	l := s.Length()
	for i := 0; i < b.N; i++ {
		s.GetDataByRank(uint32(i)%l, true)
	}
}
