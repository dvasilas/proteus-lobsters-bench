package distributions

import (
	"math/rand"
	"time"

	"github.com/google/btree"
)

// Distribution ...
type Distribution struct {
	Bin   int64
	Count int64
}

type treeNode struct {
	Start  int64
	NextID int64
	Count  int64
}

func (n treeNode) Less(than btree.Item) bool {
	return n.Start < than.(treeNode).Start
}

// Sampler ...
type Sampler struct {
	bins   *btree.BTree
	nextID int64
	end    int64
}

// NewSampler ...
func NewSampler(inDistribution []Distribution) Sampler {
	rand.Seed(time.Now().UTC().UnixNano())

	s := Sampler{
		bins: btree.New(2),
	}

	start := int64(0)
	nextID := int64(0)

	for _, d := range inDistribution {
		s.bins.ReplaceOrInsert(treeNode{
			Start:  start,
			NextID: nextID,
			Count:  d.Count,
		})

		var avgBinVal int64
		if d.Bin == 0 {
			avgBinVal = 10
		} else {
			avgBinVal = 4 * d.Bin
		}

		start += d.Count * avgBinVal
		nextID += d.Count
	}

	s.nextID = nextID
	s.end = start

	return s
}

// Sample ...
func (s Sampler) Sample() int64 {
	var bin treeNode
	it := func(node btree.Item) bool {
		bin = node.(treeNode)
		return false
	}

	sample := rand.Int63n(s.end)

	s.bins.DescendLessOrEqual(treeNode{Start: sample}, it)

	return bin.NextID + (sample % bin.Count)
}
