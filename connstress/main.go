// Copyright (C) 2016 Gimpl do foo <gimpldo@gmail.com>
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build trace

package main

import (
	"database/sql/driver"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sqlite3 "github.com/gimpldo/go-sqlite3"
)

var nTraceCalls uint64

func traceCallback(info sqlite3.TraceInfo) int {
	//fmt.Printf("Trace: %#v\n", info)
	atomic.AddUint64(&nTraceCalls, 1)
	return 0
}

const eventMask = sqlite3.TraceStmt | sqlite3.TraceProfile | sqlite3.TraceRow | sqlite3.TraceClose

var traceConf = &sqlite3.TraceConfig{
	Callback:        traceCallback,
	EventMask:       uint32(eventMask),
	WantExpandedSQL: false, // we're not testing trace details
}

type sqliteConnStressState struct {
	name string

	// driver *sqlite3.SQLiteDriver // TODO: needed?
	// Could be, if we want the option to "clone" =
	// open later more connections like the current one,
	// to dynamically adjust the stress.

	conn *sqlite3.SQLiteConn

	wg   *sync.WaitGroup
	done <-chan struct{}
}

func (cs *sqliteConnStressState) open(d *sqlite3.SQLiteDriver, name string) {
	if cs.conn != nil {
		log.Fatalf("[%s] Already opened (this call requests name '%s')", cs.name, name)
	}

	cs.name = name

	var ci driver.Conn // Connection Interface (from standard Go SQL Driver API)
	var err error
	ci, err = d.Open(":memory:")
	if err != nil {
		log.Fatalf("[%s] Failed to open database connection from the SQLite driver: %#v\n", cs.name, err)
	}

	// cs.driver = d // TODO: needed? see above comment
	cs.conn = ci.(*sqlite3.SQLiteConn)
}

// exec is intended for setup; example: create table.
func (cs *sqliteConnStressState) exec(query string) {
	var err error
	_, err = cs.conn.Exec(query, nil)
	if err != nil {
		log.Panicf("[%s] Failed to execute %q: %#v\n", cs.name, query, err)
	}
}

func (cs *sqliteConnStressState) cleanup() {
	if cs.conn != nil {
		cs.conn.Close()
		cs.conn = nil
		log.Printf("[%s] Closed database connection.\n", cs.name)
	}
}

type stressorDescr struct {
	flagName  string
	flagUsage string

	// Number of Goroutines to be started (changeable via command line arg);
	// its initial value will be used as default by the 'flag' package:
	nGoroutines int

	// 'goroIx' stands for "Goroutine Index"
	loopFunc func(cs *sqliteConnStressState, goroIx int)
}

func listStressors() string {
	sf := []string{} // 'sf' stands for "String Fragments"
	for _, descr := range stressors {
		if descr.nGoroutines != 0 {
			sf = append(sf, fmt.Sprintf("%s=%d", descr.flagName, descr.nGoroutines))
		}
	}
	return "-" + strings.Join(sf, " -")
}

var stressors = [...]stressorDescr{
	stressorDescr{
		flagName:    "traceflip",
		flagUsage:   "Number of goroutines with SetTrace calls to start for each connection",
		nGoroutines: 0,
		loopFunc: func(cs *sqliteConnStressState, goroIx int) {
			defer cs.wg.Done()
			var n int
			for {
				select {
				case <-cs.done:
					log.Printf("[%s_%d] SetTrace() stress loop done after %d iterations", cs.name, goroIx, n)
					return
				default:
					cs.conn.SetTrace(nil)
					cs.conn.SetTrace(traceConf)
					n++
				}
			}
		},
	},
	stressorDescr{
		flagName:    "tracedisable",
		flagUsage:   "Number of goroutines with SetTrace(nil) calls to start for each connection",
		nGoroutines: 0,
		loopFunc: func(cs *sqliteConnStressState, goroIx int) {
			defer cs.wg.Done()
			var n int
			for {
				select {
				case <-cs.done:
					log.Printf("[%s_%d] SetTrace(nil) stress loop done after %d iterations", cs.name, goroIx, n)
					return
				default:
					cs.conn.SetTrace(nil)
					n++
				}
			}
		},
	},
	stressorDescr{
		flagName:    "traceinstall",
		flagUsage:   "Number of goroutines with SetTrace(conf) calls to start for each connection",
		nGoroutines: 0,
		loopFunc: func(cs *sqliteConnStressState, goroIx int) {
			defer cs.wg.Done()
			var n int
			for {
				select {
				case <-cs.done:
					log.Printf("[%s_%d] SetTrace(conf) stress loop done after %d iterations", cs.name, goroIx, n)
					return
				default:
					cs.conn.SetTrace(traceConf)
					n++
				}
			}
		},
	},
	stressorDescr{
		flagName:    "sqlop",
		flagUsage:   "Number of goroutines with SQL operations to start for each connection",
		nGoroutines: 0,
		loopFunc: func(cs *sqliteConnStressState, goroIx int) {
			defer cs.wg.Done()

			cs.exec("CREATE TABLE IF NOT EXISTS t1 (dummy VARCHAR)")
			query := "DELETE FROM t1"
			// We are using an in-memory database anyway, no one will be hurt.

			stmt, err := cs.conn.Prepare(query)
			if err != nil {
				log.Panicf("[%s] Failed to prepare statement (%q): %#v\n", cs.name, query, err)
			}
			defer stmt.Close()

			var n int
			for {
				select {
				case <-cs.done:
					log.Printf("[%s_%d] SQL ops stress loop done after %d iterations; trace calls counter=%d.",
						cs.name, goroIx, n, atomic.LoadUint64(&nTraceCalls))
					return
				default:
					stmt.Exec(nil)
					// Ignoring results or errors, for convenience and execution speed.
					// This is intended for stress test, not a functional test.
					n++
				}
			}
		},
	},
	stressorDescr{
		flagName:    "sqltxop",
		flagUsage:   "Number of goroutines with SQL transaction operations to start for each connection",
		nGoroutines: 0,
		loopFunc: func(cs *sqliteConnStressState, goroIx int) {
			defer cs.wg.Done()

			cs.exec("CREATE TABLE IF NOT EXISTS t1 (dummy VARCHAR)")
			query := "DELETE FROM t1"
			// We are using an in-memory database anyway, no one will be hurt.

			tx, err := cs.conn.Begin()
			if err != nil {
				log.Panicf("[%s] Failed to begin Tx: %#v\n", cs.name, err)
			}
			defer tx.Rollback()

			stmt, err := cs.conn.Prepare(query)
			if err != nil {
				log.Panicf("[%s] Failed to prepare statement in Tx (%q): %#v\n", cs.name, query, err)
			}
			defer stmt.Close()

			var n int
			for {
				select {
				case <-cs.done:
					log.Printf("[%s_%d] SQL Tx ops stress loop done after %d iterations; trace calls counter=%d.",
						cs.name, goroIx, n, atomic.LoadUint64(&nTraceCalls))
					return
				default:
					stmt.Exec(nil)
					// Ignoring results or errors, for convenience and execution speed.
					// This is intended for stress test, not a functional test.
					n++
				}
			}
		},
	},
}

func (cs *sqliteConnStressState) startStress(wg *sync.WaitGroup, done <-chan struct{}) {
	if cs.conn == nil {
		log.Fatalf("[%s] Not opened: %#v", cs.name, cs)
	}
	if cs.wg != nil {
		log.Fatalf("[%s] Already started (got the WaitGroup): %#v", cs.name, cs)
	}
	if cs.done != nil {
		log.Fatalf("[%s] Already started (got the 'done' chan): %#v", cs.name, cs)
	}

	cs.wg = wg
	cs.done = done

	var nStarted int

	for stressorIx := range stressors {
		for i := 0; i < stressors[stressorIx].nGoroutines; i++ {
			cs.wg.Add(1)
			go stressors[stressorIx].loopFunc(cs, i)
			nStarted++
		}
	}

	log.Printf("[%s] Started %d stress loops.\n", cs.name, nStarted)
}

func main() {
	var cpuProfileFilename string
	var memProfileFilename string
	flag.StringVar(&cpuProfileFilename, "cpuprofile", "", "Write CPU profile to file")
	flag.StringVar(&memProfileFilename, "memprofile", "", "Write memory profile to file")

	var nConns int
	flag.IntVar(&nConns, "nconns", 1, "Number of SQLite database connections to open")

	for stressorIx := range stressors {
		flag.IntVar(&stressors[stressorIx].nGoroutines,
			stressors[stressorIx].flagName,
			stressors[stressorIx].nGoroutines,
			stressors[stressorIx].flagUsage)
	}

	flag.Parse()

	if cpuProfileFilename != "" {
		f, err := os.Create(cpuProfileFilename)
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
	}

	drivers := [...]*sqlite3.SQLiteDriver{
		&sqlite3.SQLiteDriver{},
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.SetTrace(traceConf)
				return err
			},
		},
	}
	// sql.Register("sqlite3_tracing", sqliteDriver[1]) // not needed,
	// because we don't use the standard Go database/sql API

	conns := make([]*sqliteConnStressState, nConns)

	for i := range conns {
		driver := drivers[1]
		//driver := drivers[i%len(drivers)]

		// This is first loop: we need to create the instances
		// that will do the work in the following loops.
		c := &sqliteConnStressState{}
		c.open(driver, fmt.Sprintf("conn%d", i))
		conns[i] = c
	}

	var wg sync.WaitGroup
	done := make(chan struct{})

	log.Printf("Starting stress loops:\n")

	for _, c := range conns {
		c.startStress(&wg, done)
	}

	sig_chan := make(chan os.Signal, 1)
	signal.Notify(sig_chan, os.Interrupt)
	select {
	case s := <-sig_chan:
		log.Printf("Signal received: %v\n", s)
	case <-time.After(20 * time.Second):
		log.Printf("Time expired.\n")
	}
	signal.Stop(sig_chan)

	if cpuProfileFilename != "" {
		pprof.StopCPUProfile()
	}

	if memProfileFilename != "" {
		f, err := os.Create(memProfileFilename)
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(f)
		log.Printf("(trace calls counter=%d) Wrote memory profile to file '%s'.\n", atomic.LoadUint64(&nTraceCalls), memProfileFilename)
	}

	log.Printf("(trace calls counter=%d) Broadcasting cancellation by closing 'done' chan...\n", atomic.LoadUint64(&nTraceCalls))
	close(done)

	log.Printf("(trace calls counter=%d) Waiting...\n", atomic.LoadUint64(&nTraceCalls))
	wg.Wait()

	for _, c := range conns {
		c.cleanup()
	}

	log.Printf("Finished (-nconns=%d %s); trace calls counter=%d.\n", nConns, listStressors(), atomic.LoadUint64(&nTraceCalls))
}
