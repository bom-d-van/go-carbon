package carbonserver

import (
	randc "crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"math"
	"math/rand"
	randm "math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OneOfOne/go-utils/memory"
	"github.com/dgryski/go-trigram"
	"go.uber.org/zap"
)

func init() { log.SetFlags(log.Lshortfile) }

func newTrieServer(files []string, withTrigram bool) *CarbonserverListener {
	var listener CarbonserverListener
	listener.logger, _ = zap.NewDevelopment()
	listener.accessLogger, _ = zap.NewDevelopment()
	listener.trieIndex = true
	listener.whisperData = "./testdata"
	listener.maxGlobs = math.MaxInt64
	listener.maxMetricsGlobbed = math.MaxInt64
	listener.maxMetricsRendered = math.MaxInt64
	listener.failOnMaxGlobs = true

	start := time.Now()
	trieIndex := newTrie(".wsp")
	for _, file := range files {
		trieIndex.insert(file)
	}
	fmt.Printf("trie index took %s\n", time.Now().Sub(start))
	count, max := trieIndex.stat()
	fmt.Printf("trie total nodes: %d max_depth: %d\n", count, max)

	if withTrigram {
		start = time.Now()
		trieIndex.setTrigrams()
		fmt.Printf("trie setTrigrams took %s size %d\n", time.Now().Sub(start), len(trieIndex.trigrams))
	}

	listener.UpdateFileIndex(&fileIndex{
		trieIdx: trieIndex,
	})

	return &listener
}

func newTrigramServer(files []string) *CarbonserverListener {
	var listener CarbonserverListener
	listener.logger, _ = zap.NewDevelopment()
	listener.accessLogger, _ = zap.NewDevelopment()
	listener.trigramIndex = true
	listener.whisperData = "./testdata"
	listener.maxGlobs = math.MaxInt64
	listener.maxMetricsGlobbed = math.MaxInt64
	listener.maxMetricsRendered = math.MaxInt64
	listener.failOnMaxGlobs = true

	start := time.Now()
	idx := trigram.NewIndex(files)
	fmt.Printf("trigram index took %s\n", time.Now().Sub(start))

	listener.UpdateFileIndex(&fileIndex{
		idx:   idx,
		files: files,
	})

	return &listener
}

func TestTrieIndex(t *testing.T) {
	commonFiles := []string{
		"/something/else/server.wsp",
		"/service-00/server-000/metric-namespace-000/43081e003a315b88.wsp",
		"/service-00/server-000/metric-namespace-000/cpu.wsp",
		"/service-00/server-000/metric-namespace-001/d218bc1539f2cf8.wsp",
		"/service-00/server-000/metric-namespace-001/cpu.wsp",
		"/service-00/server-000/metric-namespace-005/cpu.wsp",
		"/service-00/server-000/metric-namespace-002/29370bc791c0fccb.wsp",
		"/service-00/server-000/metric-namespace-002/cpu.wsp",

		"/service-00/server-001/metric-namespace-002/cpu.wsp",
		"/service-00/server-001/metric-namespace-005/cpu.wsp",

		"/service-01/server-000/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-01/server-000/metric-namespace-004/cpu.wsp",
		"/service-01/server-000/metric-namespace-005/cpu.wsp",

		"/service-00/server-002/metric-namespace-003/64cd3228c99afc54.wsp",
		"/service-00/server-002/metric-namespace-003/cpu.wsp",
		"/service-00/server-002/metric-namespace-005/cpu.wsp",
		"/service-00/server-002/metric-namespace-004/6f31f9305c67895c.wsp",
		"/service-00/server-002/metric-namespace-004/cpu.wsp",

		"/service-01/server-110/metric-namespace-007/cpu.wsp",
		"/service-01/server-120/metric-namespace-007/cpu.wsp",
		"/service-01/server-170/metric-namespace-007/cpu.wsp",

		"/service-01/server-114/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespace-007/cpu.wsp",
		"/service-01/server-12a/metric-namespace-007/cpu.wsp",
		"/service-01/server-149/metric-namespace-007/cpu.wsp",

		"/service-01/server-125/metric-namespzce-007/cpu.wsp",

		"/service-01/server-170/metric-namespace-004-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-005-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-008-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-006-xdp/cpu.wsp",
	}
	var cases = []struct {
		input  []string
		query  string
		expect []string
	}{
		{
			input:  commonFiles,
			query:  "service-00",
			expect: []string{"service-00"},
		},
		{
			input:  commonFiles,
			query:  "service-00.server-000.metric-namespace-000.cpu",
			expect: []string{"service-00.server-000.metric-namespace-000.cpu"},
		},

		{
			input: commonFiles,
			query: "service-00.server-000.metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-00.server-00[0-2].metric-namespace-00[0-2].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-000.cpu",
				"service-00.server-000.metric-namespace-001.cpu",
				"service-00.server-000.metric-namespace-002.cpu",
				"service-00.server-001.metric-namespace-002.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2]0.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-110.metric-namespace-007.cpu",
				"service-01.server-120.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-5][4-5a-z].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-12a.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[1]4.metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
			},
		},
		{
			input: []string{
				"/service-01/server-114/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-007/cpu.wsp",
				"/service-01/server-125/metric-namespace-006/cpu.wsp",
				"/service-01/server-111/metric-namespace-007/cpu.wsp",
				"/service-01/server-11a/metric-namespace-007/cpu.wsp",
				"/service-01/something-125/metric-namespace-007/cpu.wsp",
			},
			query: "service-01.server-1[0-2][^4-5].metric-namespace-007.cpu",
			expect: []string{
				"service-01.server-111.metric-namespace-007.cpu",
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-11a.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-01.server-1[0-2][4-5].metric-n[a-z]mesp[a-z1-9]ce-007.cpu",
			expect: []string{
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespzce-007.cpu",
			},
		},

		{
			input: commonFiles,
			query: "service-00.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
			},
		},

		{
			input: commonFiles,
			query: "*",
			expect: []string{
				"service-00",
				"service-01",
				"something",
			},
		},
		{
			input: append(commonFiles,
				"/ooo/abc.wsp",
				"/ooo/abc/xxx.wsp",
				"/ooo/abd/xxx.wsp",
				"/ooo/ab/xxx.wsp",
				"/ooo/abcd/xxx.wsp",
				"/ooo/xyz/xxx.wsp",
				"/ooo/xy/xxx.wsp",
			),
			query: "*.*",
			expect: []string{
				"ooo.ab",
				"ooo.abc",
				"ooo.abc",
				"ooo.abcd",
				"ooo.abd",
				"ooo.xy",
				"ooo.xyz",
				"service-00.server-000",
				"service-00.server-001",
				"service-00.server-002",
				"service-01.server-000",
				"service-01.server-110",
				"service-01.server-114",
				"service-01.server-120",
				"service-01.server-125",
				"service-01.server-12a",
				"service-01.server-149",
				"service-01.server-170",
				"something.else",
			},
		},
		{
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
			input: append(commonFiles,
				"/ooo/abc/xxx.wsp",
				"/ooo/ab/xxx.wsp",
			),
			query: "*.*.*",
			expect: []string{
				"ooo.ab.xxx",
				"ooo.abc.xxx",
				"service-00.server-000.metric-namespace-000",
				"service-00.server-000.metric-namespace-001",
				"service-00.server-000.metric-namespace-002",
				"service-00.server-000.metric-namespace-005",
				"service-00.server-001.metric-namespace-002",
				"service-00.server-001.metric-namespace-005",
				"service-00.server-002.metric-namespace-003",
				"service-00.server-002.metric-namespace-004",
				"service-00.server-002.metric-namespace-005",
				"service-01.server-000.metric-namespace-004",
				"service-01.server-000.metric-namespace-005",
				"service-01.server-110.metric-namespace-007",
				"service-01.server-114.metric-namespace-007",
				"service-01.server-120.metric-namespace-007",
				"service-01.server-125.metric-namespace-007",
				"service-01.server-125.metric-namespzce-007",
				"service-01.server-12a.metric-namespace-007",
				"service-01.server-149.metric-namespace-007",
				"service-01.server-170.metric-namespace-004-007-xdp",
				"service-01.server-170.metric-namespace-006-xdp",
				"service-01.server-170.metric-namespace-007",
				"service-01.server-170.metric-namespace-007-005-xdp",
				"service-01.server-170.metric-namespace-007-007-xdp",
				"service-01.server-170.metric-namespace-007-008-xdp",
				"something.else.server",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-namespace-005.cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-*-00[5-7].cpu",
			expect: []string{
				"service-00.server-000.metric-namespace-005.cpu",
				"service-00.server-001.metric-namespace-005.cpu",
				"service-00.server-002.metric-namespace-005.cpu",
				"service-01.server-000.metric-namespace-005.cpu",
				"service-01.server-110.metric-namespace-007.cpu",
				"service-01.server-114.metric-namespace-007.cpu",
				"service-01.server-120.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespace-007.cpu",
				"service-01.server-125.metric-namespzce-007.cpu",
				"service-01.server-12a.metric-namespace-007.cpu",
				"service-01.server-149.metric-namespace-007.cpu",
				"service-01.server-170.metric-namespace-007.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-*-00[5-7]-xdp.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-006-xdp.cpu",
				"service-01.server-170.metric-namespace-007-005-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.{metric-namespace-004-007-xdp,metric-namespace-007-007-xdp}.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: commonFiles,
			query: "service-0*.*.metric-namespace-{{004,007}}{-007}-xdp.cpu",
			expect: []string{
				"service-01.server-170.metric-namespace-004-007-xdp.cpu",
				"service-01.server-170.metric-namespace-007-007-xdp.cpu",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/backend/random-404_xoxo/http_xxx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/os/random-404_xoxo/http_xxx.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.{{frontend,backend},os}.random-404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.backend.random-404_xoxo.http_xxx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.os.random-404_xoxo.http_xxx",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.random-404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.{random-404_xoxo,random.404.xoxo}.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.404.xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/401/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/403/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/4044/xoxo/http.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.{random-40?_xoxo,random.40?.xoxo}.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-403_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_5xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.401.xoxo.http_other",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random.404.xoxo.http_other",
			},
		},
		{
			input: []string{
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_5xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-403_xoxo/http_4xx.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/401/xoxo/http_other.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/403/xoxo/udp.wsp",
				"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/4044/xoxo/http.wsp",
			},
			query: "services.groups.*.*.nginx.type.*.frontend.*404_xoxo.http*",
			expect: []string{
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_3xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_4xx",
				"services.groups.xyz.xxx_404.nginx.type.prod.frontend.random-404_xoxo.http_other",
			},
		},
		{
			input: []string{
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1018_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1019_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-1020_main7_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/cjk-2022_expr1_internet_com/traffic.wsp",
				"/fe/series/abc_101/xyz/haproxy/host/mno-2022_expr1_internet_com/traffic.wsp",
			},
			query: "fe.series.*.*.haproxy.host.*cjk-*_internet_com.traffic",
			expect: []string{
				"fe.series.abc_101.xyz.haproxy.host.cjk-1018_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1019_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-1020_main7_internet_com.traffic",
				"fe.series.abc_101.xyz.haproxy.host.cjk-2022_expr1_internet_com.traffic",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.query, func(t *testing.T) {
			t.Logf("case: TestTrieIndex/'^%s$'", regexp.QuoteMeta(c.query))

			trieServer := newTrieServer(c.input, true)
			trieFiles, _, err := trieServer.expandGlobsTrie(c.query)
			if err != nil {
				t.Errorf("failed to trie.expandGlobs: %s", err)
			}

			sort.Strings(trieFiles)

			if len(trieFiles) == 0 {
				t.Errorf("trie.expandGlobs fetched 0 files")
			}
			if !reflect.DeepEqual(trieFiles, c.expect) {
				t.Errorf("incorrect files retrieved\nreturns: %s\nexpect:  %s\n", trieFiles, c.expect)
			}
		})
	}
}

func BenchmarkGlobIndexGeneral(b *testing.B) {
	var btrieServer *CarbonserverListener
	var btrigramServer *CarbonserverListener

	// 10/100/50000
	limit0 := 1      // 50
	limit1 := 150000 // 50
	limit2 := 1      // 100
	limit3 := 1      // 1
	files := make([]string, 0, limit0*limit1*limit2*limit3)
	rand.Seed(time.Now().Unix())
	for i := 0; i < limit0; i++ {
		for j := 0; j < limit1; j++ {
			for k := 0; k < limit2; k++ {
				// for l := 0; l < limit3; l++ {
				// 	files = append(files, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/%x.wsp", i, j, k, rand.Int()))
				// }
				files = append(files, fmt.Sprintf("/service-%02d/server-%03d/metric-namespace-%03d/cpu.wsp", i, j, k))
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		btrieServer = newTrieServer(files, true)
		wg.Done()
	}()
	go func() {
		btrigramServer = newTrigramServer(files)
		wg.Done()
	}()

	wg.Wait()

	// b.Logf("memory.Sizeof(btrieServer)    = %+v\n", memory.Sizeof(btrieServer))
	// b.Logf("memory.Sizeof(btrigramServer) = %+v\n", memory.Sizeof(btrigramServer))

	var cases = []struct {
		input string
	}{
		{"service-*.server-*.metric-namespace-[4]0*.*"},
		{"service-*.server-*.*.cpu"},
		{"service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu"},

		// for exact match, trigram index falls back to file system stat, so
		// this case isn't very representaive, however, trie+dfa performs
		// extremely good in this case
		{"service-23.server-029.metric-namespace-030.cpu"},

		{"service-*.server-*.metric-namespace-40*.*"},
		// trigram index performs is better for this case because trie+dfa needs
		// to scan every third nodes because of leading star in the query
		// "*-40*"
		{"service-*.server-*.*-40*.*"},
		{"service-*.*server-300*.*-40*.*"},
		{"service-*.*server-300*.*-4*.*"},
	}

	for _, c := range cases {
		var ctrie, ctrigram int
		b.Run("trie/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f, _, _ := btrieServer.expandGlobsTrie(c.input)
				ctrie = len(f)
			}
		})
		b.Run("trigram/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f, _, _ := btrigramServer.expandGlobs(c.input)
				ctrigram = len(f)
			}
		})
		b.Logf("trie.len = %d trigram.len = %d", ctrie, ctrigram)
	}
}

func BenchmarkGlobIndexKeyword(b *testing.B) {
	var btrieServer, btrieTrigramServer *CarbonserverListener
	var btrigramServer *CarbonserverListener
	_ = btrieTrigramServer
	_ = btrigramServer

	// 10/100/50000
	limit0 := 100 // 50
	limit1 := 500 // 50
	limit2 := 3   // 100
	limit3 := 3   // 1
	files := make([]string, 0, limit0*limit1*limit2*limit3)
	keyword := "keyword"
	// prefixLen := 0

	randm.Seed(time.Now().Unix())
	getc := func(c int) string {
		b := make([]byte, c)
		_, err := randc.Read(b)
		if err != nil {
			panic(err)
		}
		r := base64.StdEncoding.EncodeToString(b)
		for _, c := range []string{"=", "/", "+"} {
			r = strings.ReplaceAll(r, c, "")
		}
		return r
	}

	// var parts0 = make([]string, limit0)
	// for i := 0; i < limit0; i++ {
	// 	parts0[i] = fmt.Sprintf("%s", getc(7))
	// }
	// var parts1 = make([]string, limit1)
	// for i := 0; i < limit1; i++ {
	// 	parts1[i] = fmt.Sprintf("%s", getc(7))
	// }
	// var parts2 = make([]string, limit2)
	// for i := 0; i < limit2; i++ {
	// 	parts2[i] = fmt.Sprintf("%s", getc(7))
	// }
	// var parts3 = make([]string, limit3)
	// for i := 0; i < limit3; i++ {
	// 	parts3[i] = fmt.Sprintf("%s", getc(7))
	// }

	for i0 := 0; i0 < limit0; i0++ {
		for i1 := 0; i1 < limit1; i1++ {
			for i2 := 0; i2 < limit2; i2++ {
				for i3 := 0; i3 < limit3; i3++ {
					files = append(files, "/%s.wsp")

					if randm.Intn(1000) == 1 {
						files = append(files, "/%s-%s.wsp", keyword, getc(32))
						// if randm.Intn(1000) == 1 {
						// 	log.Printf("files[len(files)-1] = %+v\n", files[len(files)-1])
						// }
					}
				}
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		btrieServer = newTrieServer(files, false)
		wg.Done()
	}()
	go func() {
		btrieTrigramServer = newTrieServer(files, true)
		wg.Done()
	}()
	go func() {
		btrigramServer = newTrigramServer(files)
		wg.Done()
	}()

	wg.Wait()

	fmt.Printf("files: %d\n", len(files))
	for i, f := range files[:10] {
		fmt.Printf("  %d: %s\n", i, f)
	}

	b.Logf("memory.Sizeof(btrieServer)        = %+v\n", memory.Sizeof(btrieServer))
	b.Logf("memory.Sizeof(btrieTrigramServer) = %+v\n", memory.Sizeof(btrieTrigramServer))
	b.Logf("memory.Sizeof(btrigramServer)     = %+v\n", memory.Sizeof(btrigramServer))

	var cases = []struct {
		input string
	}{
		// {"service-*.server-*.metric-namespace-[4]0*.*"},
		// {"service-*.server-*.*.cpu"},
		// {"service-2[3-4].server-02[1-9].metric-namespace-0[2-3]0.cpu"},

		// // for exact match, trigram index falls back to file system stat, so
		// // this case isn't very representaive, however, trie+dfa performs
		// // extremely good in this case
		// {"service-23.server-029.metric-namespace-030.cpu"},

		// {"service-*.server-*.metric-namespace-40*.*"},
		// // trigram index performs is better for this case because trie+dfa needs
		// // to scan every third nodes because of leading star in the query
		// // "*-40*"
		// {"service-*.server-*.*-40*.*"},
		// {"service-*.*server-300*.*-40*.*"},
		// {"service-*.*server-300*.*-4*.*"},

		{"*" + keyword + "*"},
	}

	// fmt.Printf("dump\n")
	// for i, f := range btrieServer.CurrentFileIndex().trieIdx.allMetrics('.') {
	// 	fmt.Printf("  %d: %s\n", i, f)
	// }

	for _, c := range cases {
		var ctrie, ctrieTrigram, ctrigram int
		b.Run("trie/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f, _, _ := btrieServer.expandGlobsTrie(c.input)
				ctrie = len(f)
			}
		})
		b.Run("trie2/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f, _, _ := btrieTrigramServer.expandGlobsTrie(c.input)
				ctrieTrigram = len(f)
			}
		})
		b.Run("trigram/"+c.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				f, _, _ := btrigramServer.expandGlobs(c.input)
				ctrigram = len(f)
			}
		})
		b.Logf("trie.len = %d ctrieTrigram.len = %d trigram.len = %d", ctrie, ctrieTrigram, ctrigram)
	}
}

func TestDumpAllMetrics(t *testing.T) {
	files := []string{
		"/bobo.wsp",
		"/bobododo/xxx.wsp",

		"/something.wsp",
		"/somet/xxx.wsp",
		"/something/else/server.wsp",
		"/service-00/server-000/metric-namespace-000/43081e003a315b88.wsp",
		"/service-00/server-000/metric-namespace-000/cpu.wsp",
		"/service-00/server-000/metric-namespace-001/d218bc1539f2cf8.wsp",
		"/service-00/server-000/metric-namespace-001/cpu.wsp",
		"/service-00/server-000/metric-namespace-005/cpu.wsp",
		"/service-00/server-000/metric-namespace-002/29370bc791c0fccb.wsp",
		"/service-00/server-000/metric-namespace-002/cpu.wsp",
		"/service-01/server-000/metric-namespace-002/cpu.wsp",
		"/service-01/switch-000/metric-namespace-002/cpu.wsp",

		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_3xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_5xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_other.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/http_4xx.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/tcp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_xoxo/udp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/http_other.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random/404/xoxo/udp.wsp",
		"/services/groups/xyz/xxx_404/nginx/type/prod/frontend/random-404_koko_abc/xoxo/udp.wsp",

		"/service-01/server-114/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespace-007/cpu.wsp",
		"/service-01/server-12a/metric-namespace-007/cpu.wsp",
		"/service-01/server-149/metric-namespace-007/cpu.wsp",
		"/service-01/server-125/metric-namespzce-007/cpu.wsp",
		"/service-01/server-170/metric-namespace-004-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-007-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-005-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-007-008-xdp/cpu.wsp",
		"/service-01/server-170/metric-namespace-006-xdp/cpu.wsp",
	}
	trie := newTrieServer(files, true)
	metrics := trie.CurrentFileIndex().trieIdx.allMetrics('.')
	for i := range files {
		files[i] = files[i][:len(files[i])-4]
		files[i] = files[i][1:]
		files[i] = strings.Replace(files[i], "/", ".", -1)
	}
	sort.Strings(files)
	sort.Strings(metrics)
	if !reflect.DeepEqual(files, metrics) {
		t.Errorf("trie.allMetrics:\nwant: %s\ngot:  %s\n", files, metrics)
	}
}
