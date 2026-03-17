package lock

import (
	"testing"
)

func TestWaitForGraph_NewWaitForGraph(t *testing.T) {
	g := NewWaitForGraph()
	if g == nil {
		t.Error("expected non-nil WaitForGraph")
	}
}

func TestWaitForGraph_AddEdge(t *testing.T) {
	g := NewWaitForGraph()

	// Add edge from txn 1 to txn 2
	g.AddEdge(1, 2)

	if !g.HasEdge(1, 2) {
		t.Error("expected edge from 1 to 2")
	}
	if g.HasEdge(2, 1) {
		t.Error("should not have edge from 2 to 1")
	}
}

func TestWaitForGraph_AddEdgeZero(t *testing.T) {
	g := NewWaitForGraph()

	// Adding edge with txn2 = 0 should be a no-op
	g.AddEdge(1, 0)

	if g.HasEdge(1, 0) {
		t.Error("should not have edge to 0")
	}
}

func TestWaitForGraph_RemoveEdge(t *testing.T) {
	g := NewWaitForGraph()

	g.AddEdge(1, 2)
	g.RemoveEdge(1, 2)

	if g.HasEdge(1, 2) {
		t.Error("edge should be removed")
	}
}

func TestWaitForGraph_RemoveAllEdges(t *testing.T) {
	g := NewWaitForGraph()

	// Add multiple edges
	g.AddEdge(1, 2)
	g.AddEdge(1, 3)
	g.AddEdge(2, 1)

	// Remove all edges for txn 1
	g.RemoveAllEdges(1)

	if g.HasEdge(1, 2) {
		t.Error("edge from 1 to 2 should be removed")
	}
	if g.HasEdge(1, 3) {
		t.Error("edge from 1 to 3 should be removed")
	}
	if g.HasEdge(2, 1) {
		t.Error("edge from 2 to 1 should be removed")
	}
}

func TestWaitForGraph_DetectCycle_NoCycle(t *testing.T) {
	g := NewWaitForGraph()

	// 1 -> 2 -> 3 (no cycle)
	g.AddEdge(1, 2)
	g.AddEdge(2, 3)

	cycle := g.DetectCycle()
	if cycle != nil {
		t.Errorf("expected no cycle, got %v", cycle)
	}
}

func TestWaitForGraph_DetectCycle_SimpleCycle(t *testing.T) {
	g := NewWaitForGraph()

	// 1 -> 2 -> 1 (cycle)
	g.AddEdge(1, 2)
	g.AddEdge(2, 1)

	cycle := g.DetectCycle()
	if cycle == nil {
		t.Error("expected to detect cycle")
	}
}

func TestWaitForGraph_DetectCycle_ThreeWayCycle(t *testing.T) {
	g := NewWaitForGraph()

	// 1 -> 2 -> 3 -> 1 (cycle)
	g.AddEdge(1, 2)
	g.AddEdge(2, 3)
	g.AddEdge(3, 1)

	cycle := g.DetectCycle()
	if cycle == nil {
		t.Error("expected to detect cycle")
	}
}

func TestWaitForGraph_HasEdge(t *testing.T) {
	g := NewWaitForGraph()

	g.AddEdge(1, 2)

	tests := []struct {
		txn1, txn2 uint64
		want       bool
	}{
		{1, 2, true},
		{2, 1, false},
		{1, 3, false},
		{3, 1, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if got := g.HasEdge(tt.txn1, tt.txn2); got != tt.want {
				t.Errorf("HasEdge(%d, %d) = %v, want %v", tt.txn1, tt.txn2, got, tt.want)
			}
		})
	}
}

func TestWaitForGraph_GetWaiters(t *testing.T) {
	g := NewWaitForGraph()

	g.AddEdge(1, 2)
	g.AddEdge(1, 3)

	waiters := g.GetWaiters(1)
	if len(waiters) != 2 {
		t.Errorf("expected 2 waiters, got %d", len(waiters))
	}

	// Should be empty for non-existent node
	waiters = g.GetWaiters(99)
	if len(waiters) != 0 {
		t.Errorf("expected 0 waiters for non-existent node, got %d", len(waiters))
	}
}

func TestWaitForGraph_GetWaitersOf(t *testing.T) {
	g := NewWaitForGraph()

	g.AddEdge(1, 3)
	g.AddEdge(2, 3)

	waiters := g.GetWaitersOf(3)
	if len(waiters) != 2 {
		t.Errorf("expected 2 waiters, got %d", len(waiters))
	}
}

func TestWaitForGraph_EdgeCount(t *testing.T) {
	g := NewWaitForGraph()

	if g.EdgeCount() != 0 {
		t.Error("expected 0 edges initially")
	}

	g.AddEdge(1, 2)
	g.AddEdge(2, 3)

	if g.EdgeCount() != 2 {
		t.Errorf("expected 2 edges, got %d", g.EdgeCount())
	}
}

func TestWaitForGraph_NodeCount(t *testing.T) {
	g := NewWaitForGraph()

	if g.NodeCount() != 0 {
		t.Error("expected 0 nodes initially")
	}

	g.AddEdge(1, 2)
	g.AddEdge(2, 3)

	// Nodes: 1, 2, 3
	if g.NodeCount() != 3 {
		t.Errorf("expected 3 nodes, got %d", g.NodeCount())
	}
}