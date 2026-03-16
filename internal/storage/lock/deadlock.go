// Package lock provides deadlock detection for XxSql storage engine.
package lock

import (
	"sync"
)

// WaitForGraph represents a wait-for graph for deadlock detection.
type WaitForGraph struct {
	// edges[t1] = {t2, t3} means t1 is waiting for t2 and t3
	edges   map[uint64]map[uint64]bool
	mu      sync.RWMutex
}

// NewWaitForGraph creates a new wait-for graph.
func NewWaitForGraph() *WaitForGraph {
	return &WaitForGraph{
		edges: make(map[uint64]map[uint64]bool),
	}
}

// AddEdge adds a wait-for edge from txn1 to txn2.
func (g *WaitForGraph) AddEdge(txn1, txn2 uint64) {
	if txn2 == 0 {
		return // No edge if no one holds the lock
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.edges[txn1] == nil {
		g.edges[txn1] = make(map[uint64]bool)
	}
	g.edges[txn1][txn2] = true
}

// RemoveEdge removes a wait-for edge from txn1 to txn2.
func (g *WaitForGraph) RemoveEdge(txn1, txn2 uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.edges[txn1] != nil {
		delete(g.edges[txn1], txn2)
		if len(g.edges[txn1]) == 0 {
			delete(g.edges, txn1)
		}
	}
}

// RemoveAllEdges removes all edges for a transaction.
func (g *WaitForGraph) RemoveAllEdges(txn uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.edges, txn)

	// Also remove any edges pointing to this transaction
	for t := range g.edges {
		delete(g.edges[t], txn)
	}
}

// DetectCycle detects if there is a cycle in the graph.
// Returns the transaction IDs involved in the cycle, or nil if no cycle.
func (g *WaitForGraph) DetectCycle() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// Use DFS to detect cycle
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	for txn := range g.edges {
		if cycle := g.dfs(txn, visited, recStack, []uint64{}); cycle != nil {
			return cycle
		}
	}

	return nil
}

// dfs performs depth-first search to detect cycles.
func (g *WaitForGraph) dfs(txn uint64, visited, recStack map[uint64]bool, path []uint64) []uint64 {
	visited[txn] = true
	recStack[txn] = true
	path = append(path, txn)

	for waitingFor := range g.edges[txn] {
		if !visited[waitingFor] {
			if cycle := g.dfs(waitingFor, visited, recStack, path); cycle != nil {
				return cycle
			}
		} else if recStack[waitingFor] {
			// Found cycle - extract it
			cycleStart := -1
			for i, t := range path {
				if t == waitingFor {
					cycleStart = i
					break
				}
			}
			if cycleStart >= 0 {
				return append(path[cycleStart:], waitingFor)
			}
			return append(path, waitingFor)
		}
	}

	recStack[txn] = false
	return nil
}

// HasEdge checks if there's an edge from txn1 to txn2.
func (g *WaitForGraph) HasEdge(txn1, txn2 uint64) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.edges[txn1] == nil {
		return false
	}
	return g.edges[txn1][txn2]
}

// GetWaiters returns all transactions that txn is waiting for.
func (g *WaitForGraph) GetWaiters(txn uint64) []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.edges[txn] == nil {
		return nil
	}

	waiters := make([]uint64, 0, len(g.edges[txn]))
	for t := range g.edges[txn] {
		waiters = append(waiters, t)
	}
	return waiters
}

// GetWaitersOf returns all transactions waiting for txn.
func (g *WaitForGraph) GetWaitersOf(txn uint64) []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var waiters []uint64
	for t, edges := range g.edges {
		if edges[txn] {
			waiters = append(waiters, t)
		}
	}
	return waiters
}

// EdgeCount returns the number of edges in the graph.
func (g *WaitForGraph) EdgeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, edges := range g.edges {
		count += len(edges)
	}
	return count
}

// NodeCount returns the number of nodes in the graph.
func (g *WaitForGraph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make(map[uint64]bool)
	for t1, edges := range g.edges {
		nodes[t1] = true
		for t2 := range edges {
			nodes[t2] = true
		}
	}
	return len(nodes)
}
