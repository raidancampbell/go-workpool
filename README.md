# go-workpool
[![Go Report Card](https://goreportcard.com/badge/github.com/raidancampbell/go-workpool)](https://goreportcard.com/report/github.com/raidancampbell/go-workpool)
[![GoDoc](https://godoc.org/github.com/raidancampbell/go-workpool?status.svg)](https://godoc.org/github.com/raidancampbell/go-workpool)
```go
import github.com/raidancampbell/go-workpool
```
`go-workpool` provides a pool-of-work pattern,  to synchronize events before passing them to an asynchronous/parallel processing pipeline.

For example, if you have 3 events:
1. an account creation for account `a`
2. an account update for account `a`
3. an account update for account `b`

Events 1 and 3 can be processed simultaneously, while event 2 must wait until event 1 is complete.

### Usage

A workpool is instantiated via `workpool.New()`.  The workpool expects submitted work to implement the `Work` interface.  This interface has a `Key()` function to return a string (`"a"` or `"b"` in the above example), and has a `Do()` function to perform whatever work is required.  The `workpool_test.go` file contains some simple examples.