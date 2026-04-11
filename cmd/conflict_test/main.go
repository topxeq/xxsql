package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

// TestConfig 压力测试配置
type TestConfig struct {
	ConcurrentConnections int
	OperationsPerClient   int
}

// TestResult 测试结果
type TestResult struct {
	TestName       string
	TotalOps       int64
	SuccessOps     int64
	FailedOps      int64
	DataConflicts  int64
	ConflictRate   float64
	AvgLatencyMs   float64
	P99LatencyMs   float64
	OpsPerSecond   float64
	Duration       time.Duration
	DataIntegrity  bool
	IntegrityError string
}

// ConflictTester 数据冲突检测测试器
type ConflictTester struct {
	config    *TestConfig
	engine    *storage.Engine
	exec      *executor.Executor
	tempDir   string
	results   []TestResult
	latencies []time.Duration
	latencyMu sync.Mutex
}

// NewConflictTester 创建冲突检测测试器
func NewConflictTester(cfg *TestConfig) *ConflictTester {
	return &ConflictTester{
		config:    cfg,
		latencies: make([]time.Duration, 0, 1000000),
		results:   make([]TestResult, 0),
	}
}

// Setup 初始化测试环境
func (ct *ConflictTester) Setup() error {
	var err error
	ct.tempDir, err = os.MkdirTemp("", "xxsql-conflict-test-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	dataDir := filepath.Join(ct.tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	ct.engine = storage.NewEngine(dataDir)
	if err := ct.engine.Open(); err != nil {
		return fmt.Errorf("failed to open engine: %w", err)
	}

	ct.exec = executor.NewExecutor(ct.engine)
	ct.exec.SetDatabase("testdb")

	// 创建测试表
	tables := []string{
		`CREATE TABLE conflict_test (id INT PRIMARY KEY, val INT, ver INT)`,
		`CREATE TABLE nums (id INT PRIMARY KEY, total INT)`,
		`CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)`,
	}

	for _, sql := range tables {
		if _, err := ct.exec.Execute(sql); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	fmt.Printf("[Setup] 测试环境初始化完成\n")
	fmt.Printf("[Setup] 数据目录: %s\n", dataDir)
	return nil
}

// Teardown 清理测试环境
func (ct *ConflictTester) Teardown() {
	if ct.engine != nil {
		ct.engine.Close()
	}
	if ct.tempDir != "" {
		os.RemoveAll(ct.tempDir)
	}
	fmt.Printf("[Teardown] 测试环境清理完成\n")
}

func (ct *ConflictTester) recordLatency(d time.Duration) {
	ct.latencyMu.Lock()
	ct.latencies = append(ct.latencies, d)
	ct.latencyMu.Unlock()
}

func (ct *ConflictTester) resetLatencies() {
	ct.latencyMu.Lock()
	ct.latencies = ct.latencies[:0]
	ct.latencyMu.Unlock()
}

func (ct *ConflictTester) getLatencyStats() (avg, p99 time.Duration) {
	ct.latencyMu.Lock()
	defer ct.latencyMu.Unlock()

	if len(ct.latencies) == 0 {
		return 0, 0
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
	p99 = sorted[len(sorted)*99/100]

	return
}

// TestConcurrentUpdateSameRow 测试并发更新同一行
func (ct *ConflictTester) TestConcurrentUpdateSameRow() TestResult {
	fmt.Printf("\n========== 测试并发更新同一行（数据冲突检测） ==========\n")
	fmt.Printf("并发数: %d, 操作数/客户端: %d\n", ct.config.ConcurrentConnections, ct.config.OperationsPerClient)

	ct.resetLatencies()

	// 初始化：创建一个计数器，初始值为0
	_, err := ct.exec.Execute("INSERT INTO nums VALUES (1, 0)")
	if err != nil {
		fmt.Printf("初始化失败: %v\n", err)
	}

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < ct.config.OperationsPerClient; j++ {
				opStart := time.Now()

				_, err := ct.exec.Execute("UPDATE nums SET total = total + 1 WHERE id = 1")
				ct.recordLatency(time.Since(opStart))

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 验证数据一致性
	expectedValue := int64(ct.config.ConcurrentConnections * ct.config.OperationsPerClient)
	result, _ := ct.exec.Execute("SELECT total FROM nums WHERE id = 1")

	var actualValue int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			actualValue = v
		case int:
			actualValue = int64(v)
		}
	}

	dataIntegrity := (actualValue == expectedValue)
	var integrityError string
	lostUpdates := expectedValue - actualValue

	if !dataIntegrity {
		integrityError = fmt.Sprintf("期望值: %d, 实际值: %d, 丢失更新: %d", expectedValue, actualValue, lostUpdates)
	}

	avg, p99 := ct.getLatencyStats()
	totalOps := successOps + failedOps

	r := TestResult{
		TestName:       "并发更新同一行",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		DataConflicts:  lostUpdates,
		ConflictRate:   float64(lostUpdates) / float64(expectedValue) * 100,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		DataIntegrity:  dataIntegrity,
		IntegrityError: integrityError,
	}

	ct.printResult(r)
	return r
}

// TestAccountTransfer 测试并发转账
func (ct *ConflictTester) TestAccountTransfer() TestResult {
	fmt.Printf("\n========== 测试并发转账（余额一致性检测） ==========\n")
	fmt.Printf("并发数: %d, 操作数/客户端: %d\n", ct.config.ConcurrentConnections, ct.config.OperationsPerClient)

	ct.resetLatencies()

	// 初始化账户
	numAccounts := 10
	initialBalance := int64(10000)
	totalInitialBalance := int64(numAccounts) * initialBalance

	for i := 1; i <= numAccounts; i++ {
		sql := fmt.Sprintf("INSERT INTO accounts VALUES (%d, %d)", i, initialBalance)
		ct.exec.Execute(sql)
	}

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < ct.config.OperationsPerClient; j++ {
				opStart := time.Now()

				fromAccount := (clientID+j)%numAccounts + 1
				toAccount := (clientID+j+1)%numAccounts + 1

				sql1 := fmt.Sprintf("UPDATE accounts SET balance = balance - 100 WHERE id = %d", fromAccount)
				sql2 := fmt.Sprintf("UPDATE accounts SET balance = balance + 100 WHERE id = %d", toAccount)

				_, err1 := ct.exec.Execute(sql1)
				_, err2 := ct.exec.Execute(sql2)

				ct.recordLatency(time.Since(opStart))

				if err1 != nil || err2 != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 验证总余额
	result, _ := ct.exec.Execute("SELECT SUM(balance) FROM accounts")

	var actualTotalBalance int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			actualTotalBalance = v
		case int:
			actualTotalBalance = int64(v)
		case float64:
			actualTotalBalance = int64(v)
		}
	}

	balanceDiff := actualTotalBalance - totalInitialBalance
	dataIntegrity := (balanceDiff == 0)
	var integrityError string

	if !dataIntegrity {
		integrityError = fmt.Sprintf("初始总余额: %d, 最终总余额: %d, 差异: %d", totalInitialBalance, actualTotalBalance, balanceDiff)
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

	if negativeCount > 0 {
		integrityError += fmt.Sprintf(" | 负余额账户: %d", negativeCount)
		dataIntegrity = false
	}

	avg, p99 := ct.getLatencyStats()
	totalOps := successOps + failedOps

	r := TestResult{
		TestName:       "并发转账测试",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		DataConflicts:  balanceDiff,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		DataIntegrity:  dataIntegrity,
		IntegrityError: integrityError,
	}

	ct.printResult(r)
	return r
}

// TestLostUpdateDetection 测试丢失更新
func (ct *ConflictTester) TestLostUpdateDetection() TestResult {
	fmt.Printf("\n========== 测试丢失更新检测 ==========\n")
	fmt.Printf("并发数: %d\n", ct.config.ConcurrentConnections)

	ct.resetLatencies()

	numRows := 50
	for i := 1; i <= numRows; i++ {
		sql := fmt.Sprintf("INSERT INTO conflict_test VALUES (%d, 0, 0)", i)
		ct.exec.Execute(sql)
	}

	var successOps, failedOps int64
	var expectedIncrement int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < ct.config.OperationsPerClient; j++ {
				opStart := time.Now()

				rowID := (clientID+j)%numRows + 1

				// 读取当前值
				readSQL := fmt.Sprintf("SELECT val FROM conflict_test WHERE id = %d", rowID)
				result, err := ct.exec.Execute(readSQL)
				if err != nil {
					atomic.AddInt64(&failedOps, 1)
					ct.recordLatency(time.Since(opStart))
					continue
				}

				var currentValue int64
				if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
					switch v := result.Rows[0][0].(type) {
					case int64:
						currentValue = v
					case int:
						currentValue = int64(v)
					}
				}

				time.Sleep(time.Microsecond * 50) // 模拟处理延迟

				// 写入新值
				newValue := currentValue + 1
				updateSQL := fmt.Sprintf("UPDATE conflict_test SET val = %d WHERE id = %d", newValue, rowID)
				_, err = ct.exec.Execute(updateSQL)

				ct.recordLatency(time.Since(opStart))

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
					atomic.AddInt64(&expectedIncrement, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 验证
	result, _ := ct.exec.Execute("SELECT SUM(val) FROM conflict_test")
	var actualSum int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			actualSum = v
		case int:
			actualSum = int64(v)
		}
	}

	lostUpdates := expectedIncrement - actualSum
	dataIntegrity := (lostUpdates == 0)
	var integrityError string

	if !dataIntegrity {
		integrityError = fmt.Sprintf("预期增量: %d, 实际增量: %d, 丢失更新: %d", expectedIncrement, actualSum, lostUpdates)
	}

	avg, p99 := ct.getLatencyStats()
	totalOps := successOps + failedOps

	r := TestResult{
		TestName:       "丢失更新检测",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		DataConflicts:  lostUpdates,
		ConflictRate:   float64(lostUpdates) / float64(expectedIncrement) * 100,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		DataIntegrity:  dataIntegrity,
		IntegrityError: integrityError,
	}

	ct.printResult(r)
	return r
}

// TestInsertConflict 测试主键冲突
func (ct *ConflictTester) TestInsertConflict() TestResult {
	fmt.Printf("\n========== 测试主键冲突检测 ==========\n")
	fmt.Printf("并发数: %d, 操作数/客户端: %d\n", ct.config.ConcurrentConnections, ct.config.OperationsPerClient)

	ct.resetLatencies()

	var successOps, failedOps int64
	var primaryConflicts int64
	var wg sync.WaitGroup

	idRange := ct.config.OperationsPerClient

	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < ct.config.OperationsPerClient; j++ {
				opStart := time.Now()

				id := j%idRange + 1
				sql := fmt.Sprintf("INSERT INTO conflict_test VALUES (%d, %d, 1)", id+10000, clientID)

				_, err := ct.exec.Execute(sql)
				ct.recordLatency(time.Since(opStart))

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
					atomic.AddInt64(&primaryConflicts, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	// 验证数据完整性
	result, _ := ct.exec.Execute("SELECT COUNT(*) FROM conflict_test WHERE id > 10000")
	var actualCount int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			actualCount = v
		case int:
			actualCount = int64(v)
		}
	}

	dataIntegrity := actualCount <= int64(idRange)
	var integrityError string
	if !dataIntegrity {
		integrityError = fmt.Sprintf("插入行数: %d, 超过预期: %d", actualCount, idRange)
	}

	avg, p99 := ct.getLatencyStats()
	totalOps := successOps + failedOps

	r := TestResult{
		TestName:       "主键冲突检测",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		DataConflicts:  primaryConflicts,
		ConflictRate:   float64(primaryConflicts) / float64(totalOps) * 100,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		DataIntegrity:  dataIntegrity,
		IntegrityError: integrityError,
	}

	ct.printResult(r)
	fmt.Printf("  主键冲突次数: %d (这是预期的，说明主键约束生效)\n", primaryConflicts)
	return r
}

// TestReadWriteConflict 测试读写冲突
func (ct *ConflictTester) TestReadWriteConflict() TestResult {
	fmt.Printf("\n========== 测试读写冲突 ==========\n")
	fmt.Printf("并发数: %d\n", ct.config.ConcurrentConnections)

	ct.resetLatencies()

	// 初始化数据
	for i := 1; i <= 10; i++ {
		sql := fmt.Sprintf("INSERT INTO conflict_test VALUES (%d, 100, 0)", i+20000)
		ct.exec.Execute(sql)
	}

	var readOps, writeOps int64
	var readErrors, writeErrors int64
	var wg sync.WaitGroup

	stopCh := make(chan struct{})
	startTime := time.Now()

	for i := 0; i < ct.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			isReader := clientID%2 == 0

			for {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					if isReader {
						_, err := ct.exec.Execute("SELECT * FROM conflict_test WHERE id > 20000 AND id <= 20010")
						ct.recordLatency(time.Since(opStart))

						if err != nil {
							atomic.AddInt64(&readErrors, 1)
						} else {
							atomic.AddInt64(&readOps, 1)
						}
					} else {
						id := (clientID % 10) + 20001
						sql := fmt.Sprintf("UPDATE conflict_test SET val = val + 1 WHERE id = %d", id)
						_, err := ct.exec.Execute(sql)
						ct.recordLatency(time.Since(opStart))

						if err != nil {
							atomic.AddInt64(&writeErrors, 1)
						} else {
							atomic.AddInt64(&writeOps, 1)
						}
					}
				}
			}
		}(i)
	}

	time.Sleep(5 * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	// 验证数据
	result, _ := ct.exec.Execute("SELECT SUM(val) FROM conflict_test WHERE id > 20000 AND id <= 20010")
	var sumValue int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			sumValue = v
		case int:
			sumValue = int64(v)
		}
	}

	dataIntegrity := sumValue >= 1000

	avg, p99 := ct.getLatencyStats()
	totalOps := readOps + writeOps + readErrors + writeErrors

	r := TestResult{
		TestName:      "读写冲突测试",
		TotalOps:      totalOps,
		SuccessOps:    readOps + writeOps,
		FailedOps:     readErrors + writeErrors,
		DataConflicts: 0,
		AvgLatencyMs:  float64(avg.Microseconds()) / 1000,
		P99LatencyMs:  float64(p99.Microseconds()) / 1000,
		OpsPerSecond:  float64(totalOps) / duration.Seconds(),
		Duration:      duration,
		DataIntegrity: dataIntegrity,
	}

	ct.printResult(r)
	fmt.Printf("  读操作: %d, 写操作: %d\n", readOps, writeOps)
	return r
}

func (ct *ConflictTester) printResult(r TestResult) {
	fmt.Printf("\n--- 测试结果: %s ---\n", r.TestName)
	fmt.Printf("总操作数:      %d\n", r.TotalOps)
	fmt.Printf("成功操作:      %d\n", r.SuccessOps)
	fmt.Printf("失败操作:      %d\n", r.FailedOps)
	fmt.Printf("数据冲突:      %d\n", r.DataConflicts)
	if r.ConflictRate > 0 {
		fmt.Printf("冲突率:        %.2f%%\n", r.ConflictRate)
	}
	fmt.Printf("吞吐量:        %.2f ops/sec\n", r.OpsPerSecond)
	fmt.Printf("平均延迟:      %.3f ms\n", r.AvgLatencyMs)
	fmt.Printf("P99延迟:       %.3f ms\n", r.P99LatencyMs)
	fmt.Printf("数据完整性:    %v\n", r.DataIntegrity)
	if r.IntegrityError != "" {
		fmt.Printf("错误信息:      %s\n", r.IntegrityError)
	}
	fmt.Printf("------------------------------------\n")
}

func (ct *ConflictTester) RunAllTests() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║          XxSql 数据冲突检测测试                            ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ 并发数: %d                                               ║\n", ct.config.ConcurrentConnections)
	fmt.Printf("║ 每客户端操作数: %d                                        ║\n", ct.config.OperationsPerClient)
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	ct.results = append(ct.results, ct.TestConcurrentUpdateSameRow())
	ct.results = append(ct.results, ct.TestLostUpdateDetection())
	ct.results = append(ct.results, ct.TestInsertConflict())
	ct.results = append(ct.results, ct.TestReadWriteConflict())
	ct.results = append(ct.results, ct.TestAccountTransfer())

	ct.printSummary()
}

func (ct *ConflictTester) printSummary() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    数据冲突检测汇总报告                    ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ %-20s │ %8s │ %8s │ %8s │ %6s ║\n", "测试项", "总操作", "冲突数", "冲突率", "完整性")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	for _, r := range ct.results {
		status := "✓"
		if !r.DataIntegrity {
			status = "✗"
		}
		fmt.Printf("║ %-20s │ %8d │ %8d │ %7.1f%% │ %6s ║\n",
			r.TestName, r.TotalOps, r.DataConflicts, r.ConflictRate, status)
	}

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	// 评估
	allIntegrity := true
	hasLostUpdate := false

	for _, r := range ct.results {
		if !r.DataIntegrity {
			allIntegrity = false
		}
		if r.DataConflicts > 0 && r.TestName == "并发更新同一行" {
			hasLostUpdate = true
		}
	}

	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    数据一致性评估结论                      ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	if allIntegrity {
		fmt.Printf("║ ✓ 所有测试数据完整性验证通过                               ║\n")
		fmt.Printf("║ ✓ 主键约束正常工作                                         ║\n")
		fmt.Printf("║ ✓ 并发操作数据一致                                         ║\n")
	} else {
		fmt.Printf("║ ✗ 存在数据一致性问题                                       ║\n")

		for _, r := range ct.results {
			if !r.DataIntegrity && r.IntegrityError != "" {
				fmt.Printf("║ ⚠ %s: %s\n", r.TestName, r.IntegrityError)
			}
		}

		if hasLostUpdate {
			fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
			fmt.Printf("║ 建议：使用原子操作（如 UPDATE SET x = x + 1）             ║\n")
			fmt.Printf("║       避免读取-修改-写入模式，防止丢失更新                 ║\n")
		}
	}

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
}

func main() {
	cfg := &TestConfig{
		ConcurrentConnections: 100,
		OperationsPerClient:   20,
	}

	for i, arg := range os.Args {
		if arg == "-concurrency" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.ConcurrentConnections)
		}
		if arg == "-ops" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.OperationsPerClient)
		}
		if arg == "-help" || arg == "--help" {
			fmt.Printf("用法: %s [选项]\n", os.Args[0])
			fmt.Printf("选项:\n")
			fmt.Printf("  -concurrency N   并发连接数 (默认: 100)\n")
			fmt.Printf("  -ops N           每客户端操作数 (默认: 20)\n")
			os.Exit(0)
		}
	}

	tester := NewConflictTester(cfg)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("\n接收到终止信号，正在清理...\n")
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
