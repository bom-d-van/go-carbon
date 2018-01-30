package tags

import (
	"github.com/google/btree"
)

type TagIndex btree.BTree

type TagInode struct {
	name   string
	values btree.BTree // TagValueInode
}

func NewTagIndex() *TagIndex {
	// var ti TagInode
	// ti.values = btree.New(2) // create a 2-3-4 tree (each node contains 1-3 items and 2-4 children)
	ti := (*TagIndex)(btree.New(2))
	return ti
}

func (ti *TagIndex) Insert() {
	//
}

type TagValueInode struct {
	value   string
	metrics btree.BTree // MetricInode
}

type MetricInode struct {
	val string
}

func (mi *MetricInode) Less(than btree.Item) bool {
	than.(*MetricInode)
}
