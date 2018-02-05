package tags

import (
	"strings"

	"github.com/google/btree"
)

type TagIndex struct{ btree.BTree }

type TagInode struct {
	name string
	vals btree.BTree // TagValueInode
}

func (ti *TagInode) Less(than btree.Item) bool {
	return ti.name < than.(*TagInode).name
}

type TagValueInode struct {
	val     string
	metrics btree.BTree // MetricInode
}

func (tv *TagValueInode) Less(than btree.Item) bool {
	tv2 := than.(*TagValueInode)
	return tv.val < tv2.val
}

type MetricInode struct {
	val string
}

func (mi *MetricInode) Less(than btree.Item) bool {
	than.(*MetricInode)
}

// type tagKey string

// func (t tagKey) Less(than btree.Item) bool { return string(t) < than.(string) }

func NewTagIndex() *TagIndex {
	// var ti TagInode
	// ti.vals = btree.New(2) // create a 2-3-4 tree (each node contains 1-3 items and 2-4 children)
	ti := &TagIndex{btree.New(2)}
	return ti
}

func (ti *TagIndex) Insert(tag, val, metric string) {
	tagNodex := ti.Get(TagInode{name: tag})
	if tagNodex == nil {
		tagNodex = &TagInode{name: tag, vals: btree.New(2)}
		ti.ReplaceOrInsert(tagNodex)
	}
	tagNode := tagNodex.(*TagInode)
	valueNodex := tagNode.vals.Get(TagInode{val: val})
	if valueNodex == nil {
		tagNodex = &TagValueInode{val: val, metrics: btree.New(2)}
		tagNode.vals.ReplaceOrInsert(tagNodex)
	}
	valueNode := valueNodex.(*TagValueInode)
	valueNode.metrics.ReplaceOrInsert(MetricInode{val: metric})
}

func (t *TagIndex) ListTags(filter string) []string {
	var result []string
	t.Ascend(&TagInode{name: filter}, func(item btree.Item) bool {
		node := item.(*TagInode)
		if !strings.HasPrefix(node.name, filter) {
			return false
		}
		result = append(result, node.name)
	})
	return result
}

type TagStat struct {
	Tag    string
	Values []TagStatValue
}

type TagStatValue struct {
	Count int
	Value string
}

func (t *TagIndex) StatTag(name string, valFilter string) *TagStat {
	tix := t.Get(&TagInode{name: name})
	if tix == nil {
		return nil
	}
	var ts TagStat
	ti := tix.(*TagInode)
	ti.vals.Ascend(func(item btree.Item) bool {
		tv := item.(*TagValueInode)
		if !strings.HasPrefix(tv.val, valFilter) {
			return false
		}
		ts.Values = append(ts.Values, TagStatValue{Count: tv.metrics.Len(), Value: tv.val})
	})
	return &ts
}

type TagValueExpr struct {
	Tag, Value string
	Op         Op
}

type Op string

const (
	OpEq       Op = "="
	OpNotEq    Op = "!="
	OpMatch    Op = "=~"
	OpNotMatch Op = "!=~"
)

func (t *TagIndex) ListMetrics(tvs []TagValue) []string {
	var metrics []string

	var tvis []*TagValueInode
	// TODO: fill tvis

	if len(tvs) == 1 {
		// TODO
		return metrics
	}

	//
}

func (t *TagIndex) findTagValue(tve TagValueExpr) []*TagValueInode {
	tix := t.Get(&TagInode{name: tvs[0].Tag})
	if tix == nil {
		// TODO: ?
	}
	ti := tix.(*TagInode)
	var tvi []*TagValueInode
	ti.vals.Ascend(func(item btree.Item) bool {
		tv := item.(*TagValueInode)
		switch tve.Op {
		case OpEq:
			if tv.val == tve.Value {
				tvi = append(tvi, tv)
				return false
			}
		case OpNotEq:
			// TODO
		case OpMatch:
			// TODO
		case OpNotMatch:
			// TODO
		}
		return true
	})

	return tvi
}

// func (t *TagValueInode) name() {

// }
