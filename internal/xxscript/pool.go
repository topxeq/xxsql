package xxscript

import (
	"sync"
)

// ValuePool manages pools of Values to reduce allocations.
var ValuePool = &valuePool{
	slicePool: sync.Pool{
		New: func() interface{} {
			s := make([]Value, 0, 8)
			return &s
		},
	},
	mapPool: sync.Pool{
		New: func() interface{} {
			m := make(map[string]Value, 8)
			return &m
		},
	},
	argsPool: sync.Pool{
		New: func() interface{} {
			a := make([]Value, 0, 4)
			return &a
		},
	},
}

type valuePool struct {
	slicePool sync.Pool
	mapPool   sync.Pool
	argsPool  sync.Pool
}

// GetSlice gets a slice from the pool.
func (p *valuePool) GetSlice() *[]Value {
	return p.slicePool.Get().(*[]Value)
}

// PutSlice returns a slice to the pool.
func (p *valuePool) PutSlice(s *[]Value) {
	*s = (*s)[:0]
	p.slicePool.Put(s)
}

// GetMap gets a map from the pool.
func (p *valuePool) GetMap() *map[string]Value {
	return p.mapPool.Get().(*map[string]Value)
}

// PutMap returns a map to the pool.
func (p *valuePool) PutMap(m *map[string]Value) {
	for k := range *m {
		delete(*m, k)
	}
	p.mapPool.Put(m)
}

// GetArgs gets an args slice from the pool.
func (p *valuePool) GetArgs() *[]Value {
	return p.argsPool.Get().(*[]Value)
}

// PutArgs returns an args slice to the pool.
func (p *valuePool) PutArgs(a *[]Value) {
	*a = (*a)[:0]
	p.argsPool.Put(a)
}

// tokenPool is a pool for token slices.
var tokenPool = sync.Pool{
	New: func() interface{} {
		tokens := make([]Token, 0, 64)
		return &tokens
	},
}

// GetTokenSlice gets a token slice from the pool.
func GetTokenSlice() *[]Token {
	return tokenPool.Get().(*[]Token)
}

// PutTokenSlice returns a token slice to the pool.
func PutTokenSlice(t *[]Token) {
	*t = (*t)[:0]
	tokenPool.Put(t)
}
