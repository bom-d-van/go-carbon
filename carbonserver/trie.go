package carbonserver

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"
)

const (
	gstateSplit  = 128
	gstateMatch  = 129
	gstateRanges = 130
)

var endGstate = &gstate{}

type gmatcher struct {
	exact   bool
	root    *gstate
	expr    string
	dstates []*gdstate
	dsindex int

	// has leading star following by complex expressions
	lsComplex bool
	// trigrams  roaring.Bitmap
	trigrams []uint32
}

type gstate struct {
	// TODO: make c compact
	c    [131]bool
	next []*gstate
}

type gdstate struct {
	gstates []*gstate
	// next    [131]*gdstate
	// cacheHit int
}

func (g *gmatcher) dstate() *gdstate { return g.dstates[len(g.dstates)-1] }
func (g *gmatcher) push(s *gdstate)  { g.dstates = append(g.dstates, s) }
func (g *gmatcher) pop(i int) {
	if len(g.dstates) <= i {
		g.dstates = g.dstates[:1]
		return
	}
	g.dstates = g.dstates[:len(g.dstates)-i]
}

func (g *gstate) String() string {
	var toStr func(g *gstate) string
	var ref = map[*gstate]string{}
	toStr = func(g *gstate) string {
		if r, ok := ref[g]; ok {
			return r
		}

		if g == endGstate {
			ref[g] = "end"
			return "end"
		}
		var b []byte
		for c, t := range g.c {
			if t {
				if isAlphanumeric(byte(c)) {
					b = append(b, byte(c))
				} else {
					b = append(b, '.')
				}
			}
		}
		var r = string(b)
		ref[g] = r + "(...)"
		for _, n := range g.next {
			r += "(" + toStr(n) + ")"
		}

		ref[g] = r
		return r
	}

	return toStr(g)
}

func (g gdstate) String() string {
	var r []string
	for _, s := range g.gstates {
		r = append(r, s.String())
	}
	return strings.Join(r, ",")
}

func (g *gdstate) step(c byte) *gdstate {
	// if g.next[c] != nil {
	// 	g.cacheHit++
	// 	return g.next[c]
	// }

	var ng gdstate
	for _, s := range g.gstates {
		if s.c[c] || s.c['*'] {
			for _, ns := range s.next {
				ng.gstates = g.add(ng.gstates, ns)
			}
		}
	}
	// g.next[c] = &ng
	return &ng
}

func (g *gdstate) add(list []*gstate, s *gstate) []*gstate {
	if s.c[gstateSplit] {
		for _, ns := range s.next {
			list = g.add(list, ns)
		}

		return list
	}

	list = append(list, s)
	return list
}

func (g *gdstate) matched() bool {
	for _, s := range g.gstates {
		if s == endGstate {
			return true
		}
	}
	return false
}

// TODO:
//     add range value validation
func newGlobState(expr string) (*gmatcher, error) {
	var m = gmatcher{root: &gstate{}, exact: true, expr: expr}
	var cur = m.root
	// var inAlter bool
	var alters [][2]*gstate
	// var alterStart, alterEnd *gstate
	for i := 0; i < len(expr); i++ {
		c := expr[i]
		switch c {
		case '[':
			m.exact = false
			s := &gstate{}
			i++
			negative := expr[i] == '^'
			if negative {
				i++
			}
			for i < len(expr) && expr[i] != ']' {
				if expr[i] == '-' {
					if i+1 >= len(expr) {
						return nil, errors.New("glob: missing closing range")
					}

					for j := expr[i-1] + 1; j <= expr[i+1]; j++ {
						s.c[j] = true
					}

					i += 2
					continue
				}

				s.c[expr[i]] = true
				i++
			}
			if expr[i] != ']' {
				return nil, errors.New("glob: missing ]")
			}

			if negative {
				// TODO: revisit
				for i := 32; i <= 126; i++ {
					s.c[i] = !s.c[i]
				}
			}

			cur.next = append(cur.next, s)
			cur = s
		case '?':
			m.exact = false
			var star gstate
			star.c['*'] = true
			cur.next = append(cur.next, &star)
			cur = &star
		case '*':
			m.exact = false
			if i == 0 && len(expr) > 2 {
				// TODO: check dup stars
				m.lsComplex = true
			}

			// de-dup multi stars: *** -> *
			for ; i+1 < len(expr) && expr[i+1] == '*'; i++ {
			}

			var split, star gstate
			star.c['*'] = true
			split.c[gstateSplit] = true
			split.next = append(split.next, &star)
			cur.next = append(cur.next, &split)
			star.next = append(star.next, &split)

			cur = &split
		case '{':
			// inAlter = true
			alterStart := &gstate{c: [131]bool{gstateSplit: true}}
			alterEnd := &gstate{c: [131]bool{gstateSplit: true}}
			cur.next = append(cur.next, alterStart)
			cur = alterStart
			alters = append(alters, [2]*gstate{alterStart, alterEnd})
		case '}':
			if len(alters) == 0 {
				return nil, errors.New("glob: missing {")
			}
			// inAlter = false
			// cur.next = append(cur.next, alterEnd)
			// cur = alterEnd
			cur.next = append(cur.next, alters[len(alters)-1][1])
			cur = alters[len(alters)-1][1]
			alters = alters[:len(alters)-1]
		case ',':
			// if inAlter {
			if len(alters) > 0 {
				// cur.next = append(cur.next, alterEnd)
				// cur = alterStart
				cur.next = append(cur.next, alters[len(alters)-1][1])
				cur = alters[len(alters)-1][0]
				continue
			}

			// TODO: should return error?
			fallthrough
		default:
			s := &gstate{}
			s.c[c] = true
			cur.next = append(cur.next, s)
			cur = s
		}
	}
	cur.next = append(cur.next, endGstate)

	if len(alters) > 0 {
		return nil, errors.New("glob: missing }")
	}

	var droot gdstate
	for _, s := range m.root.next {
		droot.gstates = droot.add(droot.gstates, s)
	}
	m.dstates = append(m.dstates, &droot)

	if m.lsComplex {
		var trigrams = extractTrigrams(expr)
		m.trigrams = make([]uint32, 0, len(trigrams))
		for _, t := range trigrams {
			// m.trigrams.Add(uint32(t))
			m.trigrams = append(m.trigrams, uint32(t))
		}
	}

	return &m, nil
}

func isAlphanumeric(c byte) bool {
	return 32 <= c && c <= 126
}

type trieIndex struct {
	root      *trieNode
	depth     int
	fileExt   string
	fileCount int

	trigrams map[*trieNode][]uint32
}

type trieNode struct {
	c         []byte // TODO: look for a more compact/compressed formats
	childrens []*trieNode
}

var fileNode = &trieNode{}

func (tn *trieNode) dir() bool { return len(tn.c) == 1 && tn.c[0] == '/' }

func (ti *trieIndex) trigramsContains(tn *trieNode, t uint32) bool {
	for _, i := range ti.trigrams[tn] {
		if i == t {
			return true
		}
	}
	return false
}

func newTrie(fileExt string) *trieIndex {
	return &trieIndex{
		root:     &trieNode{},
		fileExt:  fileExt,
		trigrams: map[*trieNode][]uint32{},
		// lseCache: map[string][]string{},
	}
}

// abc.def.ghi
// abc.def2.ghi
// abc.daf2.ghi
// efg.cjk
func (ti *trieIndex) insert(path string) {
	// log.Printf("path = %+v\n", path)

	path = filepath.Clean(path)
	if path[0] == '/' {
		path = path[1:]
	}
	if path == "" || path == "." {
		return
	}

	cur := ti.root
	if len(path) > ti.depth {
		ti.depth = len(path)
	}

	isFile := strings.HasSuffix(path, ti.fileExt)
	if isFile {
		path = path[:len(path)-len(ti.fileExt)]
	}

	var start, match, nlen int
	var sn, newn *trieNode
outer:
	for i := 0; i < len(path)+1; i++ {
		if i < len(path) && path[i] != '/' {
			continue
		}

		// log.Printf("\n")
		// log.Printf("  %s\n", path[start:i])

		// case 1:
		// 	abc . xxx
		// 	ab  . xxx
		// case 2:
		// 	ab  . xxx
		// 	abc . xxx
		// case 3:
		// 	abc . xxx
		// 	abc . xxx
		// case 4:
		// 	abc . xxx
		// 	xyz . xxx
		// case 5:
		// 	abc . xxx
		// 	acc . xxx
		// case 6:
		//  abc . xxx
		//  abd . xxx
		// case 7:
		//  abc  . xxx
		//  abde . xxx

	inner:
		for ci := 0; ci < len(cur.childrens); ci++ {
			child := cur.childrens[ci]
			match = 0

			// log.Printf("    child.c = %s\n", child.c)
			// log.Printf("    path[start:i] = %+v\n", path[start:i])
			// for _, child := range child.childrens {
			// 	log.Printf("      cur.child.c = %s\n", child.c)
			// }

			if len(child.c) == 0 || child.c[0] != path[start] {
				continue
			}

			nlen = i - start
			start++
			for match = 1; match < len(child.c) && match < nlen; match++ {
				// log.Printf("    path[start] = %s\n", string(path[start]))

				if child.c[match] != path[start] {
					break
				}
				start++
			}
			// log.Printf("match = %+v\n", match)
			// log.Printf("nlen = %+v\n", nlen)

			if match == nlen {
				// log.Printf("child.c = %s\n", child.c)
				// log.Printf("len(child.c) = %+v\n", len(child.c))

				// case 1
				if len(child.c) > match {
					cur = child
					goto split
				}

				// case 3
				if len(child.c) == match {
					cur = child
					goto dir
				}

				panic(fmt.Sprintf("match == nlen = %d: unknwon case", nlen))
			}

			if match == len(child.c) && len(child.c) < nlen { // case 2
				cur = child
				goto inner
			}

		split:
			// case 5, 6, 7
			// log.Printf("split\n")
			prefix, suffix := child.c[:match], child.c[match:]
			sn = &trieNode{c: suffix, childrens: child.childrens}

			child.c = prefix
			child.childrens = []*trieNode{sn}

			cur = child

			if nlen-match > 0 {
				// log.Printf("nlen-match = %+v\n", nlen-match)
				// log.Printf("path[start:i] = %+v\n", path[start:i])
				// log.Printf("len(path[start:i]) = %+v\n", len(path[start:i]))

				newn = &trieNode{c: make([]byte, nlen-match)}
				copy(newn.c, []byte(path[start:i]))

				cur.childrens = append(cur.childrens, newn)
				cur = newn
			}

			goto dir
		}

		// case 4 & 2
		if i-start > 0 {
			// log.Printf("new_node = %s\n", path[start:i])

			newn = &trieNode{c: make([]byte, i-start)}
			copy(newn.c, []byte(path[start:i]))
			cur.childrens = append(cur.childrens, newn)
			cur = newn
		}

	dir:
		start = i + 1
		for _, child := range cur.childrens {
			if child.dir() {
				cur = child
				continue outer
			}
		}
		if i < len(path) {
			// log.Printf("append /\n")

			newn = &trieNode{c: []byte{'/'}}
			cur.childrens = append(cur.childrens, newn)
			cur = newn
		}
	}

	if !isFile {
		if cur.dir() {
			cur.childrens = append(cur.childrens, &trieNode{c: []byte{'/'}})
		}
		return
	}

	cur.childrens = append(cur.childrens, fileNode)
	ti.fileCount++

	// 	log.Printf("\n")
	// 	log.Printf("--- all metrics start\n")
	// 	ti.allMetrics('.')
	// 	log.Printf("--- all metrics end\n")
	// 	log.Printf("\n")
}

// depth first search
func (ti *trieIndex) walk(pattern string, limit int) (files []string, isFiles []bool, err error) {
	var matchers []*gmatcher
	var exact bool
	for _, node := range strings.Split(pattern, "/") {
		if node == "" {
			return nil, nil, errors.New("empty node in query")
		}
		gs, err := newGlobState(node)
		if err != nil {
			return nil, nil, err
		}
		exact = exact && gs.exact
		matchers = append(matchers, gs)
	}

	var cur = ti.root
	var nindex = make([]int, ti.depth+1)
	var trieNodes = make([]*trieNode, ti.depth+1)
	var ncindex int
	var mindex int
	var curm = matchers[0]
	var ndstate *gdstate
	var isFile, isDir, hasMoreNodes bool

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			// log.Println("pop", len(cur.c)) // walk debug
			curm.pop(len(cur.c))
			goto parent
		}

		// if ncindex > 0 { // walk debug
		// log.Printf("parent.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', trieNodes[:ncindex])) // walk debug
		// } // walk debug

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', trieNodes[:ncindex])) // walk debug
		// log.Printf("curm.dstate = %+v\n", curm.dstate())                                        // walk debug
		// for _, child := range cur.childrens {                                                   // walk debug
		// 	log.Printf("child.c = %s\n", child.c)          // walk debug
		// 	log.Printf("child.dir() = %+v\n", child.dir()) // walk debug
		// } // walk debug

		if cur.dir() {
			// log.Printf("curm.dstate = %+v\n", curm.dstate())                       // walk debug
			// log.Printf("curm.dstate().matched() = %+v\n", curm.dstate().matched()) // walk debug
			if mindex+1 >= len(matchers) || !curm.dstate().matched() {
				goto parent
			}

			mindex++
			curm = matchers[mindex]
			curm.dstates = curm.dstates[:1]

			continue
		}

		if curm.lsComplex && len(curm.trigrams) > 0 {
			// var ts []trigram.T
			// for _, t := range cur.trigrams {
			// 	ts = append(ts, trigram.T(t))
			// }
			// log.Printf("ts = %+v\n", ts)

			if _, ok := ti.trigrams[cur]; ok {
				for _, t := range curm.trigrams {
					// log.Printf("trigram.T(t) = %s\n", trigram.T(t))
					if !ti.trigramsContains(cur, t) {
						goto parent
					}
				}
			}
		}

		// log.Printf("cur.c = %s\n", cur.c) // walk debug
		for i := 0; i < len(cur.c); i++ {
			ndstate = curm.dstate().step(cur.c[i])
			if len(ndstate.gstates) == 0 {
				if i > 0 {
					// log.Println("pop", i) // walk debug

					// TODO: add test case
					curm.pop(i)
				}
				goto parent
			}
			// log.Println("push") // walk debug
			curm.push(ndstate)

			// log.Printf("curm.dstate = %+v\n", curm.dstate()) // walk debug
		}

		// log.Printf("mindex+1 = %+v\n", mindex+1)           // walk debug
		// log.Printf("len(matchers) = %+v\n", len(matchers)) // walk debug

		if mindex+1 < len(matchers) {
			continue
		}

		isFile = false
		isDir = false
		hasMoreNodes = false
		for _, child := range cur.childrens {
			if child == fileNode {
				isFile = true
			} else if child.dir() {
				isDir = true
			} else {
				hasMoreNodes = true
			}
		}
		// log.Printf("isFile = %+v\n", isFile)             // walk debug
		// log.Printf("isDir = %+v\n", isDir)               // walk debug
		// log.Printf("hasMoreNodes = %+v\n", hasMoreNodes) // walk debug

		if !(isFile || isDir) {
			continue
		}

		// log.Printf("curm.dstate().matched() = %+v\n", curm.dstate().matched()) // walk debug
		if !curm.dstate().matched() {
			if hasMoreNodes {
				continue
			}

			// log.Println("pop", len(cur.c)) // walk debug
			curm.pop(len(cur.c))
			goto parent
		}

		if isFile {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, isFile)
		}
		if isDir {
			files = append(files, cur.fullPath('.', trieNodes[:ncindex]))
			isFiles = append(isFiles, false)
		}

		if len(files) >= limit || exact {
			break
		}

		if hasMoreNodes {
			continue
		}

		// log.Println("pop", len(cur.c)) // walk debug
		curm.pop(len(cur.c))
		goto parent

	parent:
		// use exact for fast exit
		nindex[ncindex] = 0
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = trieNodes[ncindex]

		if cur.dir() && nindex[ncindex] >= len(cur.childrens) {
			mindex--
			curm = matchers[mindex]

			goto parent
		}

		continue
	}

	return files, isFiles, nil
}

func (tn *trieNode) fullPath(sep byte, parents []*trieNode) string {
	var size = len(tn.c)
	for _, n := range parents {
		size += len(n.c)
	}
	var r = make([]byte, size)
	var i int
	for _, n := range parents[1:] {
		if n.dir() {
			r[i] = sep
			i++
		} else {
			copy(r[i:], n.c)
			i += len(n.c)
		}
	}
	copy(r[i:], tn.c)

	return *(*string)(unsafe.Pointer(&r))
}

func (ti *trieIndex) allMetrics(sep byte) []string {
	var files = make([]string, 0, ti.fileCount)
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, ti.depth+1)
	for {
		if nindex[ncindex] >= len(cur.childrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		// log.Printf("cur.fullPath(sep, trieNodes[:ncindex]) = %+v\n", cur.fullPath(sep, trieNodes[:ncindex]))
		// for _, cur := range cur.childrens {
		// 	log.Printf("cur.c = %s\n", cur.c)
		// }

		if cur == fileNode {
			files = append(files, cur.fullPath(sep, trieNodes[:ncindex]))
			goto parent
		}

		continue

	parent:
		nindex[ncindex] = 0
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = trieNodes[ncindex]
		continue
	}
	return files
}

func (ti *trieIndex) setTrigrams() {
	var nindex = make([]int, ti.depth+1)
	var ncindex int
	var cur = ti.root
	var trieNodes = make([]*trieNode, ti.depth+1)
	var trigrams = make([][]uint32, ti.depth+1)

	// chosen semi-randomly, balanced by space and efficiency comparing to a
	// pure trie index. This value *doubles* the trie index memory usages
	// and delivers similar performance comparing to trigram index in
	// queries contains a leading star fowllowing by keywords in one of its
	// nodes.
	const factor = 11

	for {
		if nindex[ncindex] >= len(cur.childrens) {
			goto parent
		}

		trieNodes[ncindex] = cur
		cur = cur.childrens[nindex[ncindex]]
		ncindex++

		// log.Printf("cur.fullPath(., ti.depth) = %+v\n", cur.fullPath('.', trieNodes[:ncindex])) // walk debug
		// for _, child := range cur.childrens {                                                   // walk debug
		// 	log.Printf("child.c = %s\n", child.c)          // walk debug
		// 	log.Printf("child.dir() = %+v\n", child.dir()) // walk debug
		// } // walk debug

		trigrams[ncindex] = []uint32{}
		if ncindex > 1 && len(cur.c) > 0 && !cur.dir() {
			cur1 := trieNodes[ncindex-1]
			if !cur1.dir() {
				if len(cur1.c) > 1 {
					t := uint32(uint32(cur1.c[len(cur1.c)-2])<<16 | uint32(cur1.c[len(cur1.c)-1])<<8 | uint32(cur.c[0]))
					if (ncindex-1)%factor == 0 {
						ti.trigrams[cur1] = append(ti.trigrams[cur1], t)
					}
					trigrams[ncindex] = append(trigrams[ncindex], t)
				} else if ncindex-2 >= 0 {
					cur2 := trieNodes[ncindex-2]
					if !cur2.dir() && len(cur2.c) > 0 && len(cur1.c) > 0 {
						t := uint32(uint32(cur2.c[len(cur2.c)-1])<<16 | uint32(cur1.c[len(cur1.c)-1])<<8 | uint32(cur.c[0]))
						if (ncindex-2)%factor == 0 {
							ti.trigrams[cur2] = append(ti.trigrams[cur2], t)
						}
						trigrams[ncindex] = append(trigrams[ncindex], t)
					}
				}

				if len(cur.c) > 1 {
					t := uint32(cur1.c[len(cur1.c)-1])<<16 | uint32(cur.c[0])<<8 | uint32(cur.c[1])
					if (ncindex-1)%factor == 0 {
						ti.trigrams[cur1] = append(ti.trigrams[cur1], t)
					}
					trigrams[ncindex] = append(trigrams[ncindex], t)
				}
			}
		}
		if !cur.dir() && len(cur.c) > 2 {
			for i := 0; i < len(cur.c)-2; i++ {
				t := uint32(uint32(cur.c[i])<<16 | uint32(cur.c[i+1])<<8 | uint32(cur.c[i+2]))
				if ncindex%factor == 0 {
					ti.trigrams[cur] = append(ti.trigrams[cur], t)
				}
				trigrams[ncindex] = append(trigrams[ncindex], t)
			}
		}
		for i := 0; i < ncindex+1; i++ {
			if ncindex%factor == 0 {
				ti.trigrams[cur] = append(ti.trigrams[cur], trigrams[i]...)
			}
		}

		for i := ncindex - 1; i >= 0; i-- {
			if trieNodes[i].dir() {
				break
			}
			for _, t := range trigrams[ncindex] {
				if i%factor == 0 {
					ti.trigrams[trieNodes[i]] = append(ti.trigrams[trieNodes[i]], t)
				}
			}
		}

		if cur == fileNode {
			goto parent
		}

		continue

	parent:
		nindex[ncindex] = 0
		ncindex--
		if ncindex < 0 {
			break
		}
		nindex[ncindex]++
		cur = trieNodes[ncindex]
		continue
	}
}

func (listener *CarbonserverListener) expandGlobsTrie(query string) ([]string, []bool, error) {
	query = strings.Replace(query, ".", "/", -1)
	globs := []string{query}

	var slashInBraces, inAlter bool
	for _, c := range query {
		if c == '{' {
			inAlter = true
		} else if c == '}' {
			inAlter = false
		} else if inAlter && c == '/' {
			slashInBraces = true
			break
		}
	}
	// for complex queries like {a.b.c,x}.o.p.q, fall back to simple expansion
	if slashInBraces {
		var err error
		globs, err = listener.expandGlobBraces(globs)
		if err != nil {
			return nil, nil, err
		}
	}

	var fidx = listener.CurrentFileIndex()
	var files []string
	var leafs []bool

	for _, g := range globs {
		f, l, err := fidx.trieIdx.walk(g, listener.maxMetricsGlobbed-len(files))
		if err != nil {
			return nil, nil, err
		}
		files = append(files, f...)
		leafs = append(leafs, l...)
	}

	return files, leafs, nil
}
