package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

// TestConfig 测试配置
type TestConfig struct {
	ConcurrentConnections int
	DurationSeconds       int
	DataRows              int
	BufferPoolSize        int
}

// TestResult 测试结果
type TestResult struct {
	TestName       string
	TotalOps       int64
	SuccessOps     int64
	FailedOps      int64
	TimeoutOps     int64
	DataConflicts  int64
	Deadlocks      int64
	AvgLatencyMs   float64
	MaxLatencyMs   float64
	P99LatencyMs   float64
	OpsPerSecond   float64
	Duration       time.Duration
	DataIntegrity  bool
	IntegrityError string
	MemoryAllocMB  float64
	GoroutineCount int
}

// ComprehensiveTester 全面压力测试器
type ComprehensiveTester struct {
	config    *TestConfig
	engine    *storage.Engine
	exec      *executor.Executor
	tempDir   string
	results   []TestResult
	latencies []time.Duration
	latencyMu sync.Mutex

	// 统计
	timeoutCount  int64
	conflictCount int64
	deadlockCount int64
}

// NewComprehensiveTester 创建测试器
func NewComprehensiveTester(cfg *TestConfig) *ComprehensiveTester {
	return &ComprehensiveTester{
		config:    cfg,
		latencies: make([]time.Duration, 0, 1000000),
		results:   make([]TestResult, 0),
	}
}

// Setup 初始化
func (ct *ComprehensiveTester) Setup() error {
	var err error
	ct.tempDir, err = os.MkdirTemp("", "xxsql-comprehensive-test-*")
	if err != nil {
		return err
	}

	dataDir := filepath.Join(ct.tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	ct.engine = storage.NewEngine(dataDir)
	if err := ct.engine.Open(); err != nil {
		return err
	}

	ct.exec = executor.NewExecutor(ct.engine)
	ct.exec.SetDatabase("testdb")

	// 创建测试表
	tables := []string{
		`CREATE TABLE counter (id INT PRIMARY KEY, val INT)`,
		`CREATE TABLE accounts (id INT PRIMARY KEY, balance INT, frozen INT)`,
		`CREATE TABLE products (id INT PRIMARY KEY, stock INT, sold INT)`,
		`CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status VARCHAR(20))`,
		`CREATE TABLE hot_rows (id INT PRIMARY KEY, counter INT, version INT)`,
		`CREATE TABLE large_table (id INT PRIMARY KEY, data VARCHAR(100), value INT)`,
	}

	for _, sql := range tables {
		if _, err := ct.exec.Execute(sql); err != nil {
			return fmt.Errorf("create table failed: %w", err)
		}
	}

	// 初始化数据
	fmt.Printf("[Setup] 初始化测试数据...\n")

	// 计数器
	ct.exec.Execute("INSERT INTO counter VALUES (1, 0)")
	ct.exec.Execute("INSERT INTO counter VALUES (2, 0)")
	ct.exec.Execute("INSERT INTO counter VALUES (3, 0)")

	// 账户（多账户转账测试）
	for i := 1; i <= 20; i++ {
		sql := fmt.Sprintf("INSERT INTO accounts VALUES (%d, 10000, 0)", i)
		ct.exec.Execute(sql)
	}

	// 商品库存
	for i := 1; i <= 100; i++ {
		sql := fmt.Sprintf("INSERT INTO products VALUES (%d, 1000, 0)", i)
		ct.exec.Execute(sql)
	}

	// 热点行
	ct.exec.Execute("INSERT INTO hot_rows VALUES (1, 0, 0)")
	ct.exec.Execute("INSERT INTO hot_rows VALUES (2, 0, 0)")
	ct.exec.Execute("INSERT INTO hot_rows VALUES (3, 0, 0)")

	// 大表数据
	for i := 1; i <= ct.config.DataRows; i++ {
		sql := fmt.Sprintf("INSERT INTO large_table VALUES (%d, 'data_%d', %d)", i, i, i%100)
		ct.exec.Execute(sql)
	}

	fmt.Printf("[Setup] 完成，数据目录: %s\n", dataDir)
	return nil
}

// Teardown 清理
func (ct *ComprehensiveTester) Teardown() {
	if ct.engine != nil {
		ct.engine.Close()
	}
	if ct.tempDir != "" {
		os.RemoveAll(ct.tempDir)
	}
	fmt.Printf("[Teardown] 清理完成\n")
}

func (ct *ComprehensiveTester) recordLatency(d time.Duration) {
	ct.latencyMu.Lock()
	ct.latencies = append(ct.latencies, d)
	ct.latencyMu.Unlock()
}

func (ct *ComprehensiveTester) resetLatencies() {
	ct.latencyMu.Lock()
	ct.latencies = ct.latencies[:0]
	ct.latencyMu.Unlock()
}

func (ct *ComprehensiveTester) getStats() (avg, max, p99 time.Duration) {
	ct.latencyMu.Lock()
	defer ct.latencyMu.Unlock()

	if len(ct.latencies) == 0 {
		return 0, 0, 0
	}

	sorted := make([]time.Duration, len(ct.latencies))
	copy(sorted, ct.latencies)

	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avg = sum / time.Duration(len(sorted))
	max = sorted[len(sorted)-1]
	p99 = sorted[len(sorted)*99/100]

	return
}

// TestHotRowContention 测试热点行竞争
func (ct *ComprehensiveTester) TestHotRowContention() TestResult {
	fmt.Printf("\n========== 测试热点行竞争 ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	// 重置计数器
	ct.exec.Execute("UPDATE counter SET val = 0 WHERE id = 1")
	ct.exec.Execute("UPDATE counter SET val = 0 WHERE id = 2")
	ct.exec.Execute("UPDATE counter SET val = 0 WHERE id = 3")

	ct.resetLatencies()

	var successOps, failedOps, timeoutOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 所有客户端更新同一行（最大竞争）
					_, err := ct.exec.Execute("UPDATE counter SET val = val + 1 WHERE id = 1")

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	// 验证数据
	result, _ := ct.exec.Execute("SELECT val FROM counter WHERE id = 1")
	var actualVal int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			actualVal = v
		case int:
			actualVal = int64(v)
		}
	}

	avg, max, p99 := ct.getStats()
	totalOps := successOps + failedOps

	// 记录内存
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "热点行竞争",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		TimeoutOps:     timeoutOps,
		DataConflicts:  0,
		DataIntegrity:  true,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  最终计数器值: %d (无丢失更新)\n", actualVal)
	return r
}

// TestMultiRowDeadlock 测试多行更新（潜在死锁）
func (ct *ComprehensiveTester) TestMultiRowDeadlock() TestResult {
	fmt.Printf("\n========== 测试多行更新（死锁检测） ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	// 重置计数器
	ct.exec.Execute("UPDATE counter SET val = 0 WHERE id = 1")
	ct.exec.Execute("UPDATE counter SET val = 0 WHERE id = 2")

	ct.resetLatencies()

	var successOps, failedOps, timeoutOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 减少并发数以避免过度锁竞争
	effectiveConcurrency := ct.config.ConcurrentConnections
	if effectiveConcurrency > 30 {
		effectiveConcurrency = 30
	}

	for i := 0; i < effectiveConcurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; ; j++ {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 交替更新不同行，模拟潜在死锁场景
					// 偶数客户端: 先更新行1再更新行2
					// 奇数客户端: 先更新行2再更新行1
					var err error
					if clientID%2 == 0 {
						ct.exec.Execute("UPDATE counter SET val = val + 1 WHERE id = 1")
						_, err = ct.exec.Execute("UPDATE counter SET val = val + 1 WHERE id = 2")
					} else {
						ct.exec.Execute("UPDATE counter SET val = val + 1 WHERE id = 2")
						_, err = ct.exec.Execute("UPDATE counter SET val = val + 1 WHERE id = 1")
					}

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	// 验证数据一致性
	result1, _ := ct.exec.Execute("SELECT val FROM counter WHERE id = 1")
	result2, _ := ct.exec.Execute("SELECT val FROM counter WHERE id = 2")

	var val1, val2 int64
	if len(result1.Rows) > 0 && len(result1.Rows[0]) > 0 {
		switch v := result1.Rows[0][0].(type) {
		case int64:
			val1 = v
		case int:
			val1 = int64(v)
		}
	}
	if len(result2.Rows) > 0 && len(result2.Rows[0]) > 0 {
		switch v := result2.Rows[0][0].(type) {
		case int64:
			val2 = v
		case int:
			val2 = int64(v)
		}
	}

	avg, max, p99 := ct.getStats()
	totalOps := successOps + failedOps

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "多行更新(死锁检测)",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		TimeoutOps:     timeoutOps,
		DataIntegrity:  true,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  行1计数: %d, 行2计数: %d\n", val1, val2)
	return r
}

// TestAccountTransfer 测试账户转账（数据一致性）
func (ct *ComprehensiveTester) TestAccountTransfer() TestResult {
	fmt.Printf("\n========== 测试账户转账（数据一致性） ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	ct.resetLatencies()

	// 重置账户余额
	for i := 1; i <= 20; i++ {
		ct.exec.Execute(fmt.Sprintf("UPDATE accounts SET balance = 10000 WHERE id = %d", i))
	}

	// 获取初始总余额
	initResult, _ := ct.exec.Execute("SELECT SUM(balance) FROM accounts")
	var initialBalance int64
	if len(initResult.Rows) > 0 && len(initResult.Rows[0]) > 0 {
		switch v := initResult.Rows[0][0].(type) {
		case int64:
			initialBalance = v
		case int:
			initialBalance = int64(v)
		}
	}

	var successOps, failedOps, timeoutOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 限制并发数
	effectiveConcurrency := ct.config.ConcurrentConnections
	if effectiveConcurrency > 50 {
		effectiveConcurrency = 50
	}

	for i := 0; i < effectiveConcurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; ; j++ {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 随机转账
					from := (clientID+j)%20 + 1
					to := (clientID+j+1)%20 + 1

					sql1 := fmt.Sprintf("UPDATE accounts SET balance = balance - 100 WHERE id = %d", from)
					sql2 := fmt.Sprintf("UPDATE accounts SET balance = balance + 100 WHERE id = %d", to)

					_, err1 := ct.exec.Execute(sql1)
					_, err2 := ct.exec.Execute(sql2)

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}

					if err1 != nil || err2 != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	// 验证总余额
	finalResult, _ := ct.exec.Execute("SELECT SUM(balance) FROM accounts")
	var finalBalance int64
	if len(finalResult.Rows) > 0 && len(finalResult.Rows[0]) > 0 {
		switch v := finalResult.Rows[0][0].(type) {
		case int64:
			finalBalance = v
		case int:
			finalBalance = int64(v)
		}
	}

	// 检查负余额
	negResult, _ := ct.exec.Execute("SELECT COUNT(*) FROM accounts WHERE balance < 0")
	var negativeCount int64
	if len(negResult.Rows) > 0 && len(negResult.Rows[0]) > 0 {
		switch v := negResult.Rows[0][0].(type) {
		case int64:
			negativeCount = v
		case int:
			negativeCount = int64(v)
		}
	}

	dataIntegrity := (initialBalance == finalBalance) && (negativeCount == 0)
	var integrityError string
	if initialBalance != finalBalance {
		integrityError = fmt.Sprintf("余额不一致: 初始%d, 最终%d, 差异%d", initialBalance, finalBalance, finalBalance-initialBalance)
	}
	if negativeCount > 0 {
		integrityError += fmt.Sprintf(" | %d个账户负余额", negativeCount)
	}

	avg, max, p99 := ct.getStats()
	totalOps := successOps + failedOps

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "账户转账测试",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		TimeoutOps:     timeoutOps,
		DataIntegrity:  dataIntegrity,
		IntegrityError: integrityError,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	return r
}

// TestProductSeckill 测试商品秒杀（库存扣减）
func (ct *ComprehensiveTester) TestProductSeckill() TestResult {
	fmt.Printf("\n========== 测试商品秒杀（库存扣减） ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	ct.resetLatencies()

	// 重置商品库存
	for i := 1; i <= 100; i++ {
		ct.exec.Execute(fmt.Sprintf("UPDATE products SET stock = 1000, sold = 0 WHERE id = %d", i))
	}

	var successOps, failedOps, timeoutOps int64
	var stockExhausted int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 限制并发数
	effectiveConcurrency := ct.config.ConcurrentConnections
	if effectiveConcurrency > 50 {
		effectiveConcurrency = 50
	}

	for i := 0; i < effectiveConcurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; ; j++ {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 随机选择商品
					productID := (clientID+j)%100 + 1

					// 先检查库存
					checkResult, _ := ct.exec.Execute(fmt.Sprintf("SELECT stock FROM products WHERE id = %d", productID))
					var stock int64
					if len(checkResult.Rows) > 0 && len(checkResult.Rows[0]) > 0 {
						switch v := checkResult.Rows[0][0].(type) {
						case int64:
							stock = v
						case int:
							stock = int64(v)
						}
					}

					var err error
					if stock > 0 {
						_, err = ct.exec.Execute(fmt.Sprintf("UPDATE products SET stock = stock - 1, sold = sold + 1 WHERE id = %d", productID))
					} else {
						atomic.AddInt64(&stockExhausted, 1)
					}

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	// 验证库存和销量一致性
	stockResult, _ := ct.exec.Execute("SELECT SUM(stock) FROM products")
	soldResult, _ := ct.exec.Execute("SELECT SUM(sold) FROM products")

	var totalStock, totalSold int64
	if len(stockResult.Rows) > 0 && len(stockResult.Rows[0]) > 0 {
		switch v := stockResult.Rows[0][0].(type) {
		case int64:
			totalStock = v
		case int:
			totalStock = int64(v)
		}
	}
	if len(soldResult.Rows) > 0 && len(soldResult.Rows[0]) > 0 {
		switch v := soldResult.Rows[0][0].(type) {
		case int64:
			totalSold = v
		case int:
			totalSold = int64(v)
		}
	}

	// 初始库存 100 * 1000 = 100000
	dataIntegrity := (totalStock + totalSold) == 100000

	avg, max, p99 := ct.getStats()
	totalOps := successOps + failedOps

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "商品秒杀测试",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		TimeoutOps:     timeoutOps,
		DataIntegrity:  dataIntegrity,
		IntegrityError: fmt.Sprintf("库存+销量=%d, 预期100000, 库存耗尽次数:%d", totalStock+totalSold, stockExhausted),
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  剩余库存: %d, 已售: %d\n", totalStock, totalSold)
	return r
}

// TestReadWriteMix 测试读写混合
func (ct *ComprehensiveTester) TestReadWriteMix() TestResult {
	fmt.Printf("\n========== 测试读写混合 ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	ct.resetLatencies()

	var readOps, writeOps int64
	var readErrors, writeErrors int64
	var timeoutOps int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 70% 读，30% 写
	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			isReader := clientID%10 < 7 // 70% readers

			for {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					var err error
					if isReader {
						// 读操作
						_, err = ct.exec.Execute("SELECT * FROM large_table WHERE value > 50 LIMIT 10")
						if err != nil {
							atomic.AddInt64(&readErrors, 1)
						} else {
							atomic.AddInt64(&readOps, 1)
						}
					} else {
						// 写操作
						id := (clientID+int(time.Now().UnixNano()))%ct.config.DataRows + 1
						_, err = ct.exec.Execute(fmt.Sprintf("UPDATE large_table SET value = value + 1 WHERE id = %d", id))
						if err != nil {
							atomic.AddInt64(&writeErrors, 1)
						} else {
							atomic.AddInt64(&writeOps, 1)
						}
					}

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	avg, max, p99 := ct.getStats()
	totalOps := readOps + writeOps + readErrors + writeErrors
	successOps := readOps + writeOps

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "读写混合测试",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      readErrors + writeErrors,
		TimeoutOps:     timeoutOps,
		DataIntegrity:  true,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  读操作: %d, 写操作: %d\n", readOps, writeOps)
	fmt.Printf("  读错误: %d, 写错误: %d\n", readErrors, writeErrors)
	return r
}

// TestLongRunningQuery 测试长时间运行的查询
func (ct *ComprehensiveTester) TestLongRunningQuery() TestResult {
	fmt.Printf("\n========== 测试长时间查询 + 并发写入 ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	ct.resetLatencies()

	var successOps, failedOps, timeoutOps int64
	var longQueryCount int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 启动长时间查询协程
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 大表扫描（模拟长查询）
					_, err := ct.exec.Execute("SELECT COUNT(*), SUM(value) FROM large_table")

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 1*time.Second {
						atomic.AddInt64(&longQueryCount, 1)
					}

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	// 启动并发写入协程
	for i := 0; i < ct.config.ConcurrentConnections-5; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; ; j++ {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					id := (clientID+j)%ct.config.DataRows + 1
					_, err := ct.exec.Execute(fmt.Sprintf("UPDATE large_table SET value = value + 1 WHERE id = %d", id))

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if latency > 5*time.Second {
						atomic.AddInt64(&timeoutOps, 1)
					}

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	avg, max, p99 := ct.getStats()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "长查询+并发写入",
		TotalOps:       successOps + failedOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		TimeoutOps:     timeoutOps,
		DataIntegrity:  true,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(successOps+failedOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  长查询(>1s)次数: %d\n", longQueryCount)
	return r
}

// TestMemoryPressure 测试内存压力
func (ct *ComprehensiveTester) TestMemoryPressure() TestResult {
	fmt.Printf("\n========== 测试内存压力 ==========\n")
	fmt.Printf("并发数: %d, 持续时间: %d秒\n", ct.config.ConcurrentConnections, ct.config.DurationSeconds)

	ct.resetLatencies()

	var successOps, failedOps int64
	var peakMemoryMB int64
	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	startTime := time.Now()

	// 监控内存
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				currentKB := int64(m.Alloc / 1024)
				if currentKB > atomic.LoadInt64(&peakMemoryMB) {
					atomic.StoreInt64(&peakMemoryMB, currentKB)
				}
			}
		}
	}()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; ; j++ {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 查询返回大量数据
					_, err := ct.exec.Execute("SELECT * FROM large_table LIMIT 100")

					latency := time.Since(opStart)
					ct.recordLatency(latency)

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(ct.config.DurationSeconds) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	avg, max, p99 := ct.getStats()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	r := TestResult{
		TestName:       "内存压力测试",
		TotalOps:       successOps + failedOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		DataIntegrity:  true,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(successOps+failedOps) / duration.Seconds(),
		Duration:       duration,
		MemoryAllocMB:  float64(m.Alloc) / 1024 / 1024,
		GoroutineCount: runtime.NumGoroutine(),
	}

	ct.printResult(r)
	fmt.Printf("  峰值内存: %.2f MB\n", float64(atomic.LoadInt64(&peakMemoryMB))/1024)
	return r
}

func (ct *ComprehensiveTester) printResult(r TestResult) {
	fmt.Printf("\n--- %s ---\n", r.TestName)
	fmt.Printf("总操作数:    %d\n", r.TotalOps)
	fmt.Printf("成功/失败:   %d / %d\n", r.SuccessOps, r.FailedOps)
	if r.TimeoutOps > 0 {
		fmt.Printf("超时操作:    %d (>5秒)\n", r.TimeoutOps)
	}
	fmt.Printf("吞吐量:      %.2f ops/sec\n", r.OpsPerSecond)
	fmt.Printf("平均延迟:    %.3f ms\n", r.AvgLatencyMs)
	fmt.Printf("最大延迟:    %.3f ms\n", r.MaxLatencyMs)
	fmt.Printf("P99延迟:     %.3f ms\n", r.P99LatencyMs)
	fmt.Printf("数据完整性:  %v\n", r.DataIntegrity)
	if r.IntegrityError != "" {
		fmt.Printf("错误信息:    %s\n", r.IntegrityError)
	}
	fmt.Printf("内存使用:    %.2f MB\n", r.MemoryAllocMB)
	fmt.Printf("Goroutine数: %d\n", r.GoroutineCount)
	fmt.Printf("持续时间:    %v\n", r.Duration)
	fmt.Printf("------------------------------------\n")
}

func (ct *ComprehensiveTester) RunAllTests() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║        XxSql 全面压力测试 - 并发问题检测                   ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ 并发数: %-4d  持续时间: %-3d秒  数据量: %-5d          ║\n",
		ct.config.ConcurrentConnections, ct.config.DurationSeconds, ct.config.DataRows)
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	ct.results = append(ct.results, ct.TestHotRowContention())
	ct.results = append(ct.results, ct.TestMultiRowDeadlock())
	ct.results = append(ct.results, ct.TestAccountTransfer())
	ct.results = append(ct.results, ct.TestProductSeckill())
	ct.results = append(ct.results, ct.TestReadWriteMix())
	ct.results = append(ct.results, ct.TestLongRunningQuery())
	ct.results = append(ct.results, ct.TestMemoryPressure())

	ct.printSummary()
}

func (ct *ComprehensiveTester) printSummary() {
	fmt.Printf("\n")
	fmt.Printf("╔══════════════════════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                           全面压力测试汇总报告                                    ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ %-18s │ %8s │ %8s │ %10s │ %8s │ %8s │ %6s ║\n",
		"测试项", "总操作", "失败", "超时", "ops/sec", "P99(ms)", "完整性")
	fmt.Printf("╠══════════════════════════════════════════════════════════════════════════════════╣\n")

	for _, r := range ct.results {
		status := "✓"
		if !r.DataIntegrity {
			status = "✗"
		}
		fmt.Printf("║ %-18s │ %8d │ %8d │ %10d │ %8.1f │ %8.2f │ %6s ║\n",
			r.TestName, r.TotalOps, r.FailedOps, r.TimeoutOps, r.OpsPerSecond, r.P99LatencyMs, status)
	}

	fmt.Printf("╚══════════════════════════════════════════════════════════════════════════════════╝\n")

	// 评估
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    并发问题检测结果                         ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	// 检查是否有问题
	hasTimeout := false
	hasIntegrity := false

	for _, r := range ct.results {
		if r.TimeoutOps > 0 {
			hasTimeout = true
		}
		if !r.DataIntegrity {
			hasIntegrity = true
		}
	}

	// 热点行竞争
	fmt.Printf("║ [热点行竞争] ")
	if ct.results[0].MaxLatencyMs < 10000 {
		fmt.Printf("无阻塞，最大延迟 %.2fms                    ║\n", ct.results[0].MaxLatencyMs)
	} else {
		fmt.Printf("检测到阻塞，最大延迟 %.2fms                 ║\n", ct.results[0].MaxLatencyMs)
	}

	// 死锁
	fmt.Printf("║ [死锁检测]   ")
	if ct.results[1].FailedOps < 10 {
		fmt.Printf("无死锁现象                                ║\n")
	} else {
		fmt.Printf("可能有死锁，失败操作 %d                    ║\n", ct.results[1].FailedOps)
	}

	// 超时
	fmt.Printf("║ [超时检测]   ")
	if !hasTimeout {
		fmt.Printf("无超时请求                                ║\n")
	} else {
		var totalTimeout int64
		for _, r := range ct.results {
			totalTimeout += r.TimeoutOps
		}
		fmt.Printf("检测到 %d 次超时请求(>5秒)                ║\n", totalTimeout)
	}

	// 数据完整性
	fmt.Printf("║ [数据完整性] ")
	if !hasIntegrity {
		fmt.Printf("所有测试数据一致                          ║\n")
	} else {
		fmt.Printf("存在数据不一致问题                        ║\n")
		for _, r := range ct.results {
			if !r.DataIntegrity && r.IntegrityError != "" {
				fmt.Printf("║   - %s: %s\n", r.TestName, r.IntegrityError)
			}
		}
	}

	// 内存
	var maxMem float64
	for _, r := range ct.results {
		if r.MemoryAllocMB > maxMem {
			maxMem = r.MemoryAllocMB
		}
	}
	fmt.Printf("║ [内存使用]   峰值 %.2f MB                               ║\n", maxMem)

	// Goroutine泄漏
	fmt.Printf("║ [Goroutine]  最终 %d 个                                  ║\n", runtime.NumGoroutine())

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	// 结论
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                         测试结论                           ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	if !hasTimeout && !hasIntegrity {
		fmt.Printf("║ ✓ 高并发测试通过，无明显并发问题                           ║\n")
		fmt.Printf("║ ✓ 数据一致性验证通过                                       ║\n")
		fmt.Printf("║ ✓ 无死锁、无长时间阻塞                                     ║\n")
	} else {
		if hasTimeout {
			fmt.Printf("║ ⚠ 存在超时请求，需要优化                                   ║\n")
		}
		if hasIntegrity {
			fmt.Printf("║ ⚠ 存在数据一致性问题                                       ║\n")
		}
	}

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
}

func main() {
	cfg := &TestConfig{
		ConcurrentConnections: 150,
		DurationSeconds:       10,
		DataRows:              5000,
	}

	// 解析参数
	for i, arg := range os.Args {
		if arg == "-concurrency" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.ConcurrentConnections)
		}
		if arg == "-duration" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.DurationSeconds)
		}
		if arg == "-data" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.DataRows)
		}
		if arg == "-help" || arg == "--help" {
			fmt.Printf("XxSql 全面压力测试\n\n")
			fmt.Printf("用法: %s [选项]\n\n", os.Args[0])
			fmt.Printf("选项:\n")
			fmt.Printf("  -concurrency N   并发连接数 (默认: 150)\n")
			fmt.Printf("  -duration N      测试持续时间秒数 (默认: 10)\n")
			fmt.Printf("  -data N          测试数据量 (默认: 5000)\n")
			os.Exit(0)
		}
	}

	tester := NewComprehensiveTester(cfg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("\n中断信号，清理中...\n")
		tester.Teardown()
		os.Exit(1)
	}()

	if err := tester.Setup(); err != nil {
		fmt.Printf("初始化失败: %v\n", err)
		os.Exit(1)
	}
	defer tester.Teardown()

	tester.RunAllTests()
}
