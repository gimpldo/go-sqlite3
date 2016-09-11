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

	// used in some tests, not all:
	preparedStmt driver.Stmt

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

func (cs *sqliteConnStressState) prepare(query string) {
	var err error
	cs.preparedStmt, err = cs.conn.Prepare(query)
	if err != nil {
		log.Panicf("[%s] Failed to prepare statement (%q): %#v\n", cs.name, query, err)
	}
}

// execPreparedStmt is intended to be run in a loop, for stress testing.
func (cs *sqliteConnStressState) execPreparedStmt() {
	cs.preparedStmt.Exec(nil)
	// Ignoring results or errors, for convenience and execution speed.
	// This is intended for stress test, not a functional test.
}

func (cs *sqliteConnStressState) cleanup() {
	if cs.preparedStmt != nil {
		cs.preparedStmt.Close()
		log.Printf("[%s] Closed prepared statement.\n", cs.name)
	}
	if cs.conn != nil {
		cs.conn.Close()
		log.Printf("[%s] Closed database connection.\n", cs.name)
	}
}

func (cs *sqliteConnStressState) stressUsingSetTrace(id int) {
	defer cs.wg.Done()
	var n int
	for {
		select {
		case <-cs.done:
			log.Printf("[%s_%d] SetTrace() stress loop done after %d iterations", cs.name, id, n)
			return
		default:
			cs.conn.SetTrace(nil)
			cs.conn.SetTrace(traceConf)
			n++
		}
	}
}

func (cs *sqliteConnStressState) stressUsingPreparedStmt(id int) {
	defer cs.wg.Done()

	cs.exec("CREATE TABLE IF NOT EXISTS t1 (dummy VARCHAR)")

	// We are using an in-memory database anyway, no one will be hurt:
	cs.prepare("DELETE FROM t1")

	var n int
	for {
		select {
		case <-cs.done:
			log.Printf("[%s_%d] SQL ops stress loop done after %d iterations", cs.name, id, n)
			return
		default:
			cs.execPreparedStmt()
			n++
		}
	}
}

var nSetTraceGoroPerConn int
var nSQLOpsGoroPerConn int

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

	for i := 0; i < nSetTraceGoroPerConn; i++ {
		cs.wg.Add(1)
		go cs.stressUsingSetTrace(i)
		nStarted++
	}

	for i := 0; i < nSQLOpsGoroPerConn; i++ {
		cs.wg.Add(1)
		go cs.stressUsingPreparedStmt(i)
		nStarted++
	}

	log.Printf("[%s] Started %d stress loops.\n", cs.name, nStarted)
}

func main() {
	var nConns int
	flag.IntVar(&nConns, "nconns", 1, "Number of SQLite database connections to open")

	flag.IntVar(&nSetTraceGoroPerConn, "settrace", 1, "Number of goroutines with SetTrace calls to start for each connection")
	flag.IntVar(&nSQLOpsGoroPerConn, "sqlop", 1, "Number of goroutines with SQL operations to start for each connection")

	flag.Parse()

	const nDrivers = 2
	var drivers [nDrivers]*sqlite3.SQLiteDriver
	drivers[0] = &sqlite3.SQLiteDriver{}
	drivers[1] = &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			err := conn.SetTrace(traceConf)
			return err
		},
	}
	// sql.Register("sqlite3_tracing", sqliteDriver) // not needed,
	// because we don't use the standard Go database/sql API

	conns := make([]*sqliteConnStressState, nConns)

	for i := 0; i < len(conns); i++ {
		driver := drivers[1]
		//driver := drivers[1]
		//driver := drivers[i%nDrivers]

		// This is first loop: we need to create instances;
		// for the following loops, 'range' will work as expected
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

	log.Printf("(number of trace calls: %d) Broadcasting cancellation by closing 'done' chan...\n", atomic.LoadUint64(&nTraceCalls))
	close(done)

	log.Printf("(number of trace calls: %d) Waiting...\n", atomic.LoadUint64(&nTraceCalls))
	wg.Wait()

	for _, c := range conns {
		c.cleanup()
	}

	log.Printf("Finished; number of trace calls: %d\n", atomic.LoadUint64(&nTraceCalls))

	os.Exit(0)
}
