// Copyright (C) 2016 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
// TODO: add "Gimpl do foo" team?
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package sqlite3

/*
#ifndef USE_LIBSQLITE3
#include <sqlite3-binding.h>
#else
#include <sqlite3.h>
#endif

void traceCallbackTrampoline(unsigned traceEventCode, void *ctx, void *p, void *x);
*/
import "C"

import "unsafe"

// SetTrace installs or removes the trace callback for the given database connection.
// It's not named 'RegisterTrace' because only one callback can be kept and called.
// Calling SetTrace a second time on same database connection
// overrides (cancels) any prior callback and all its settings:
// event mask, etc.
func (c *SQLiteConn) SetTrace(requested *TraceConfig) error {
	connHandle := uintptr(unsafe.Pointer(c.db))

	_, _ = popTraceMapping(connHandle)

	if requested == nil || requested.EventMask == 0 || requested.Callback == nil {
		// The traceMap entry was deleted already by popTraceMapping():
		// can disable all events now, no need to watch for TraceClose.
		err := c.setSQLiteTrace(0)
		return err
	}

	reqCopy := *requested

	// Disable potentially expensive operations
	// if their result will not be used. We are doing this
	// just in case the caller provided nonsensical input.
	if reqCopy.EventMask&TraceStmt == 0 {
		reqCopy.WantExpandedSQL = false
	}

	addTraceMapping(connHandle, reqCopy)

	// The callback trampoline function does cleanup on Close event,
	// regardless of the presence or absence of the user callback.
	// Therefore it needs the Close event to be selected:
	actualEventMask := reqCopy.EventMask | TraceClose
	err := c.setSQLiteTrace(actualEventMask)
	return err
}

func (c *SQLiteConn) setSQLiteTrace(sqliteEventMask uint32) error {
	rv := C.sqlite3_trace_v2(c.db,
		C.uint(sqliteEventMask),
		(*[0]byte)(unsafe.Pointer(C.traceCallbackTrampoline)),
		unsafe.Pointer(c.db)) // Fourth arg is same as first: we are
	// passing the database connection handle as callback context.

	if rv != C.SQLITE_OK {
		return c.lastError()
	}
	return nil
}
