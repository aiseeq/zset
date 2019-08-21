package zset

import (
	"math/rand"
)

/*=================================== Redis SkipList APIs ======================================
 * zslCreate
 * Create a new jump table. O(1)
 * zslFree
 * Releases the given jump table and all the nodes contained in the table. O(N), N is the Length of the jump table.
 * zslInsert
 * Add a new node containing the given member and Score to the jump table. The average O(log N) and the worst O(N),
 * N is the Length of the jump table.
 * zslDelete
 * Delete nodes in the jump table that contain the given member and Score. The average O(log N) and the worst O(N).
 * zslGetRank
 * Returns the rank of the node containing the given member and Score in the jump table. The average O(log N) and
 * the worst O(N), N is the Length of the jump table.
 * zslGetElementByRank
 * Returns the node of the jump table on the given rank. The average O(log N) and the worst O(N).
 * zslIsInRange
 * Given a range of ranges,
 * such as 0 to 15, 20 to 28, and so on,
 * Returns 1 if the given range of scores is included in the Score range of the jump table, otherwise returns 0.
 * By jumping the Header and footer nodes of the table, this detection can be done with O(1) complexity.
 * zslFirstInRange
 * Given a range of scores, return the first node in the jump table that matches this range.
 * Average O(log N), worst O(N). N is the Length of the jump table.
 * zslLastInRange
 * Given a range of scores, returns the last node in the jump table that matches this range.
 * Average O(log N), worst O(N). N is the Length of the jump table.
 * zslDeleteRangeByScore
 * Given a range of scores, delete all nodes in the jump table that are within this range. O(N),
 * N is the number of nodes deleted.
 * zslDeleteRangeByRank
 * Given a rank range, delete all nodes in the jump table that are within this range. O(N),
 * N is the number of nodes deleted.
 */
const zSkiplistMaxlevel = 32

type (
	SkipListLevel struct {
		Forward *SkipListNode
		Span    uint64
	}

	SkipListNode struct {
		ObjID    int64
		Score    float64
		Backward *SkipListNode
		Level    []*SkipListLevel
	}
	Obj struct {
		Key        int64
		Attachment interface{}
		Score      float64
	}

	SkipList struct {
		Header *SkipListNode
		Tail   *SkipListNode
		Length int64 // Number of nodes
		Level  int16 // The number of layers with the most layers
	}

	// SortedSet is the final exported sorted set we can use
	Dict map[int64]*Obj
	SortedSet struct {
		Dict Dict
		Zsl  *SkipList
	}
	Zrangespec struct {
		Min   float64
		Max   float64
		Minex int32
		Maxex int32
	}
	// Struct to hold an inclusive/exclusive range spec by lexicographic comparison.
	Zlexrangespec struct {
		MinKey int64
		MaxKey int64
		Minex  int // are Min or Max exclusive?
		Maxex  int // are Min or Max exclusive?
	}
)

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'Obj' is referenced by the node after the call. */
func zslCreateNode(level int16, score float64, id int64) *SkipListNode {
	n := &SkipListNode{
		Score: score,
		ObjID: id,
		Level: make([]*SkipListLevel, level),
	}
	for i := range n.Level {
		n.Level[i] = new(SkipListLevel)
	}
	return n
}

/* Create a new skiplist. */
func zslCreate() *SkipList {
	return &SkipList{
		Level:  1,
		Header: zslCreateNode(zSkiplistMaxlevel, 0, 0),
	}
}

const zSkiplistP = 0.25 /* Skiplist P = 1/4 */

/* Returns a random Level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and _ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (zSkiplistP * 0xFFFF) {
		level++
	}
	if level < zSkiplistMaxlevel {
		return level
	}
	return zSkiplistMaxlevel
}

/* zslInsert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'Obj'. */
func (zsl *SkipList) zslInsert(score float64, id int64) *SkipListNode {
	update := make([]*SkipListNode, zSkiplistMaxlevel)
	rank := make([]uint64, zSkiplistMaxlevel)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		/* store rank that is crossed to reach the insert position */
		if i == zsl.Level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if x.Level[i] != nil {
			for x.Level[i].Forward != nil &&
				(x.Level[i].Forward.Score < score ||
					(x.Level[i].Forward.Score == score && x.Level[i].Forward.ObjID < id)) {
				rank[i] += x.Level[i].Span
				x = x.Level[i].Forward
			}
		}
		update[i] = x
	}
	/* we assume the element is not already inside, since we allow duplicated
	 * scores, reinserting the same element should never happen since the
	 * caller of zslInsert() should test in the hash table if the element is
	 * already inside or not. */
	level := randomLevel()
	if level > zsl.Level {
		for i := zsl.Level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.Header
			update[i].Level[i].Span = uint64(zsl.Length)
		}
		zsl.Level = level
	}
	x = zslCreateNode(level, score, id)
	for i := int16(0); i < level; i++ {
		x.Level[i].Forward = update[i].Level[i].Forward
		update[i].Level[i].Forward = x

		/* update Span covered by update[i] as x is inserted here */
		x.Level[i].Span = update[i].Level[i].Span - (rank[0] - rank[i])
		update[i].Level[i].Span = (rank[0] - rank[i]) + 1
	}

	/* increment Span for untouched levels */
	for i := level; i < zsl.Level; i++ {
		update[i].Level[i].Span++
	}

	if update[0] == zsl.Header {
		x.Backward = nil
	} else {
		x.Backward = update[0]

	}
	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x
	} else {
		zsl.Tail = x
	}
	zsl.Length++
	return x
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
func (zsl *SkipList) zslDeleteNode(x *SkipListNode, update []*SkipListNode) {
	for i := int16(0); i < zsl.Level; i++ {
		if update[i].Level[i].Forward == x {
			update[i].Level[i].Span += x.Level[i].Span - 1
			update[i].Level[i].Forward = x.Level[i].Forward
		} else {
			update[i].Level[i].Span--
		}
	}
	if x.Level[0].Forward != nil {
		x.Level[0].Forward.Backward = x.Backward
	} else {
		zsl.Tail = x.Backward
	}
	for zsl.Level > 1 && zsl.Header.Level[zsl.Level-1].Forward == nil {
		zsl.Level--
	}
	zsl.Length--
}

/* Delete an element with matching Score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->Obj). */
func (zsl *SkipList) zslDelete(score float64, id int64) int {
	update := make([]*SkipListNode, zSkiplistMaxlevel)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score &&
					x.Level[i].Forward.ObjID < id)) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}
	/* We may have multiple elements with the same Score, what we need
	 * is to find the element with both the right Score and object. */
	x = x.Level[0].Forward
	if x != nil && score == x.Score && x.ObjID == id {
		zsl.zslDeleteNode(x, update)
		return 1
	}
	return 0 /* not found */
}

func zslValueGteMin(value float64, spec *Zrangespec) bool {
	if spec.Minex != 0 {
		return value > spec.Min
	}
	return value >= spec.Min
}

func zslValueLteMax(value float64, spec *Zrangespec) bool {
	if spec.Maxex != 0 {
		return value < spec.Max
	}
	return value <= spec.Max
}

/* Returns if there is a part of the zset is in range. */
func (zsl *SkipList) zslIsInRange(ran *Zrangespec) bool {
	/* Test for ranges that will always be empty. */
	if ran.Min > ran.Max ||
		(ran.Min == ran.Max && (ran.Minex != 0 || ran.Maxex != 0)) {
		return false
	}
	x := zsl.Tail
	if x == nil || !zslValueGteMin(x.Score, ran) {
		return false
	}
	x = zsl.Header.Level[0].Forward
	if x == nil || !zslValueLteMax(x.Score, ran) {
		return false
	}
	return true
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
func (zsl *SkipList) zslFirstInRange(ran *Zrangespec) *SkipListNode {
	/* If everything is out of range, return early. */
	if !zsl.zslIsInRange(ran) {
		return nil
	}

	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		/* Go Forward while *OUT* of range. */
		for x.Level[i].Forward != nil &&
			!zslValueGteMin(x.Level[i].Forward.Score, ran) {
			x = x.Level[i].Forward
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	x = x.Level[0].Forward
	//serverAssert(x != NULL);

	/* Check if Score <= Max. */
	if !zslValueLteMax(x.Score, ran) {
		return nil
	}
	return x
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
func (zsl *SkipList) zslLastInRange(ran *Zrangespec) *SkipListNode {

	/* If everything is out of range, return early. */
	if !zsl.zslIsInRange(ran) {
		return nil
	}
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		/* Go Forward while *IN* range. */
		for x.Level[i].Forward != nil &&
			zslValueLteMax(x.Level[i].Forward.Score, ran) {
			x = x.Level[i].Forward
		}
	}
	/* This is an inner range, so this node cannot be NULL. */
	//serverAssert(x != NULL);

	/* Check if Score >= Min. */
	if !zslValueGteMin(x.Score, ran) {
		return nil
	}
	return x
}

/* Delete all the elements with Score between Min and Max from the skiplist.
 * Min and Max are inclusive, so a Score >= Min || Score <= Max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
func (zsl *SkipList) zslDeleteRangeByScore(ran *Zrangespec, dict map[int64]*Obj) uint64 {
	removed := uint64(0)
	update := make([]*SkipListNode, zSkiplistMaxlevel)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil {
			var condition bool
			if ran.Minex != 0 {
				condition = x.Level[i].Forward.Score <= ran.Min
			} else {
				condition = x.Level[i].Forward.Score < ran.Min
			}
			if !condition {
				break
			}
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	/* Current node is the last with Score < or <= Min. */
	x = x.Level[0].Forward

	/* Delete nodes while in range. */
	for x != nil {
		var condition bool
		if ran.Maxex != 0 {
			condition = x.Score < ran.Max
		} else {
			condition = x.Score <= ran.Max
		}
		if !condition {
			break
		}
		next := x.Level[0].Forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.ObjID)
		// Here is where x->Obj is actually released.
		// And golang has GC, don't need to free manually anymore
		//zslFreeNode(x)
		removed++
		x = next
	}
	return removed
}

func (zsl *SkipList) zslDeleteRangeByLex(ran *Zlexrangespec, dict map[int64]*Obj) uint64 {
	removed := uint64(0)

	update := make([]*SkipListNode, zSkiplistMaxlevel)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && !zslLexValueGteMin(x.Level[i].Forward.ObjID, ran) {
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	/* Current node is the last with Score < or <= Min. */
	x = x.Level[0].Forward

	/* Delete nodes while in range. */
	for x != nil && zslLexValueLteMax(x.ObjID, ran) {
		next := x.Level[0].Forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.ObjID)
		removed++
		x = next
	}
	return removed
}

func zslLexValueGteMin(id int64, spec *Zlexrangespec) bool {
	if spec.Minex != 0 {
		return compareKey(id, spec.MinKey) > 0
	}
	return compareKey(id, spec.MinKey) >= 0
}

func compareKey(a, b int64) int8 {
	if a == b {
		return 0
	} else if a > b {
		return 1
	}
	return -1
}

func zslLexValueLteMax(id int64, spec *Zlexrangespec) bool {
	if spec.Maxex != 0 {
		return compareKey(id, spec.MaxKey) < 0
	}
	return compareKey(id, spec.MaxKey) <= 0
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
func (zsl *SkipList) zslDeleteRangeByRank(start, end uint64, dict map[int64]*Obj) uint64 {
	update := make([]*SkipListNode, zSkiplistMaxlevel)
	var traversed, removed uint64

	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (traversed+x.Level[i].Span) < start {
			traversed += x.Level[i].Span
			x = x.Level[i].Forward
		}
		update[i] = x
	}

	traversed++
	x = x.Level[0].Forward
	for x != nil && traversed <= end {
		next := x.Level[0].Forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.ObjID)
		removed++
		traversed++
		x = next
	}
	return removed
}

/* Find the rank for an element by both Score and Obj.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the Span of Zsl->Header to the
 * first element. */
func (zsl *SkipList) zslGetRank(score float64, key int64) int64 {
	rank := uint64(0)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil &&
			(x.Level[i].Forward.Score < score ||
				(x.Level[i].Forward.Score == score &&
					x.Level[i].Forward.ObjID <= key)) {
			rank += x.Level[i].Span
			x = x.Level[i].Forward
		}

		/* x might be equal to Zsl->Header, so test if Obj is non-NULL */
		if x.ObjID == key {
			return int64(rank)
		}
	}
	return 0
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
func (zsl *SkipList) zslGetElementByRank(rank uint64) *SkipListNode {
	traversed := uint64(0)
	x := zsl.Header
	for i := zsl.Level - 1; i >= 0; i-- {
		for x.Level[i].Forward != nil && (traversed+x.Level[i].Span) <= rank {
			traversed += x.Level[i].Span
			x = x.Level[i].Forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

// New creates a new SortedSet and return its pointer
func New() *SortedSet {
	s := &SortedSet{
		Dict: make(Dict),
		Zsl:  zslCreate(),
	}
	return s
}

// Length returns counts of elements
func (z *SortedSet) Length() int64 {
	return z.Zsl.Length
}

// Set is used to add or update an element
func (z *SortedSet) Set(score float64, key int64, dat interface{}) {
	v, ok := z.Dict[key]
	z.Dict[key] = &Obj{Attachment: dat, Key: key, Score: score}
	if ok {
		/* Remove and re-insert when Score changes. */
		if score != v.Score {
			z.Zsl.zslDelete(v.Score, key)
			z.Zsl.zslInsert(score, key)
		}
	} else {
		z.Zsl.zslInsert(score, key)
	}
}

// IncrBy ..
func (z *SortedSet) IncrBy(score float64, key int64) (float64, interface{}) {
	v, ok := z.Dict[key]
	if !ok {
		// use negative infinity ?
		return 0, nil
	}
	if score != 0 {
		z.Zsl.zslDelete(v.Score, key)
		v.Score += score
		z.Zsl.zslInsert(v.Score, key)
	}
	return v.Score, v.Attachment
}

// Delete removes an element from the SortedSet
// by its Key.
func (z *SortedSet) Delete(key int64) (ok bool) {
	v, ok := z.Dict[key]
	if ok {
		z.Zsl.zslDelete(v.Score, key)
		delete(z.Dict, key)
		return true
	}
	return false
}

// GetRank returns position,Score and extra data of an element which
// found by the parameter Key.
// The parameter reverse determines the rank is descent or ascend，
// true means descend and false means ascend.
func (z *SortedSet) GetRank(key int64, reverse bool) (rank int64, score float64, data interface{}) {
	v, ok := z.Dict[key]
	if !ok {
		return -1, 0, nil
	}
	r := z.Zsl.zslGetRank(v.Score, key)
	if reverse {
		r = z.Zsl.Length - r
	} else {
		r--
	}
	return int64(r), v.Score, v.Attachment

}

// GetData returns data stored in the map by its Key
func (z *SortedSet) GetData(key int64) (data interface{}, ok bool) {
	o, ok := z.Dict[key]
	if !ok {
		return nil, false
	}
	return o.Attachment, true
}

// GetDataByRank returns the id,Score and extra data of an element which
// found by position in the rank.
// The parameter rank is the position, reverse says if in the descend rank.
func (z *SortedSet) GetDataByRank(rank int64, reverse bool) (key int64, score float64, data interface{}) {
	if rank < 0 || rank > z.Zsl.Length {
		return 0, 0, nil
	}
	if reverse {
		rank = z.Zsl.Length - rank
	} else {
		rank++
	}
	n := z.Zsl.zslGetElementByRank(uint64(rank))
	if n == nil {
		return 0, 0, nil
	}
	dat, _ := z.Dict[n.ObjID]
	if dat == nil {
		return 0, 0, nil
	}
	return dat.Key, dat.Score, dat.Attachment
}

// Range implements ZRANGE
func (z *SortedSet) Range(start, end int64, f func(float64, int64, interface{})) {
	z.commonRange(start, end, false, f)
}

// RevRange implements ZREVRANGE
func (z *SortedSet) RevRange(start, end int64, f func(float64, int64, interface{})) {
	z.commonRange(start, end, true, f)
}

func (z *SortedSet) commonRange(start, end int64, reverse bool, f func(float64, int64, interface{})) {
	l := z.Zsl.Length
	if start < 0 {
		start += l
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += l
	}

	if start > end || start >= l {
		return
	}
	if end >= l {
		end = l - 1
	}
	span := (end - start) + 1

	var node *SkipListNode
	if reverse {
		node = z.Zsl.Tail
		if start > 0 {
			node = z.Zsl.zslGetElementByRank(uint64(l - start))
		}
	} else {
		node = z.Zsl.Header.Level[0].Forward
		if start > 0 {
			node = z.Zsl.zslGetElementByRank(uint64(start + 1))
		}
	}
	for span > 0 {
		span--
		k := node.ObjID
		s := node.Score
		f(s, k, z.Dict[k].Attachment)
		if reverse {
			node = node.Backward
		} else {
			node = node.Level[0].Forward
		}
	}
}
