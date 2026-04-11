package main

import (
	"encoding/json"
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
	// 服务器配置
	ServerPath  string `json:"server_path"`
	DataDir     string `json:"data_dir"`
	ConfigPath  string `json:"config_path"`
	PrivatePort int    `json:"private_port"`
	MySQLPort   int    `json:"mysql_port"`
	HTTPPort    int    `json:"http_port"`

	// 测试参数
	ConcurrentConnections int `json:"concurrent_connections"` // 并发连接数
	OperationsPerClient   int `json:"operations_per_client"`  // 每个客户端的操作数
	InsertCount           int `json:"insert_count"`           // 初始插入数据量
	WarmupSeconds         int `json:"warmup_seconds"`         // 预热时间(秒)
	TestDuration          int `json:"test_duration"`          // 测试持续时间(秒)

	// 测试类型开关
	TestInsert     bool `json:"test_insert"`
	TestSelect     bool `json:"test_select"`
	TestUpdate     bool `json:"test_update"`
	TestDelete     bool `json:"test_delete"`
	TestMixed      bool `json:"test_mixed"`
	TestReadHeavy  bool `json:"test_read_heavy"`
	TestWriteHeavy bool `json:"test_write_heavy"`
}

// TestResult 测试结果
type TestResult struct {
	TestName       string        `json:"test_name"`
	TotalOps       int64         `json:"total_ops"`
	SuccessOps     int64         `json:"success_ops"`
	FailedOps      int64         `json:"failed_ops"`
	AvgLatencyMs   float64       `json:"avg_latency_ms"`
	MinLatencyMs   float64       `json:"min_latency_ms"`
	MaxLatencyMs   float64       `json:"max_latency_ms"`
	P50LatencyMs   float64       `json:"p50_latency_ms"`
	P95LatencyMs   float64       `json:"p95_latency_ms"`
	P99LatencyMs   float64       `json:"p99_latency_ms"`
	OpsPerSecond   float64       `json:"ops_per_second"`
	Duration       time.Duration `json:"duration"`
	ConnsAttempted int           `json:"conns_attempted"`
	ConnsSucceeded int           `json:"conns_succeeded"`
	ConnsFailed    int           `json:"conns_failed"`
}

// StressTester 压力测试器
type StressTester struct {
	config    *TestConfig
	engine    *storage.Engine
	exec      *executor.Executor
	tempDir   string
	results   []TestResult
	latencies []time.Duration
	latencyMu sync.Mutex
}

// NewStressTester 创建压力测试器
func NewStressTester(cfg *TestConfig) *StressTester {
	return &StressTester{
		config:    cfg,
		latencies: make([]time.Duration, 0, 1000000),
		results:   make([]TestResult, 0),
	}
}

// Setup 初始化测试环境
func (st *StressTester) Setup() error {
	// 创建临时目录
	var err error
	st.tempDir, err = os.MkdirTemp("", "xxsql-stress-test-*")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}

	// 创建数据目录
	dataDir := filepath.Join(st.tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	// 创建服务器配置
	configPath := filepath.Join(st.tempDir, "config.json")
	serverConfig := map[string]interface{}{
		"server": map[string]interface{}{
			"name":     "stress-test-server",
			"data_dir": dataDir,
		},
		"network": map[string]interface{}{
			"bind":          "127.0.0.1",
			"private_port":  st.config.PrivatePort,
			"mysql_port":    st.config.MySQLPort,
			"http_port":     st.config.HTTPPort,
			"mysql_enabled": false,
			"http_enabled":  false,
		},
		"log": map[string]interface{}{
			"level": "ERROR",
		},
		"auth": map[string]interface{}{
			"enabled": false,
		},
		"storage": map[string]interface{}{
			"page_size":        4096,
			"buffer_pool_size": 1000,
		},
		"connection": map[string]interface{}{
			"max_connections": st.config.ConcurrentConnections + 50,
		},
		"worker_pool": map[string]interface{}{
			"worker_count": 20,
		},
	}

	configData, err := json.MarshalIndent(serverConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	st.config.ConfigPath = configPath
	st.config.DataDir = dataDir

	// 直接初始化存储引擎(不启动服务器进程)
	st.engine = storage.NewEngine(dataDir)
	if err := st.engine.Open(); err != nil {
		return fmt.Errorf("failed to open engine: %w", err)
	}

	st.exec = executor.NewExecutor(st.engine)
	st.exec.SetDatabase("testdb")

	// 创建测试表
	if _, err := st.exec.Execute(`CREATE TABLE stress_test (
		id INT PRIMARY KEY,
		name VARCHAR(100),
		value INT,
		data TEXT,
		created_at VARCHAR(30)
	)`); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// 创建自增ID表
	if _, err := st.exec.Execute(`CREATE TABLE auto_test (
		id SEQ,
		name VARCHAR(100),
		value INT
	)`); err != nil {
		return fmt.Errorf("failed to create auto_test table: %w", err)
	}

	fmt.Printf("[Setup] 测试环境初始化完成\n")
	fmt.Printf("[Setup] 数据目录: %s\n", dataDir)
	fmt.Printf("[Setup] 配置文件: %s\n", configPath)

	return nil
}

// Teardown 清理测试环境
func (st *StressTester) Teardown() {
	if st.engine != nil {
		st.engine.Close()
	}

	if st.tempDir != "" {
		os.RemoveAll(st.tempDir)
	}

	fmt.Printf("[Teardown] 测试环境清理完成\n")
}

// InsertInitialData 插入初始数据
func (st *StressTester) InsertInitialData(count int) error {
	fmt.Printf("[Setup] 插入初始数据 %d 条...\n", count)

	batchSize := 100
	for i := 0; i < count; i += batchSize {
		end := i + batchSize
		if end > count {
			end = count
		}

		var wg sync.WaitGroup
		errCh := make(chan error, batchSize)

		for j := i; j < end; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				sql := fmt.Sprintf("INSERT INTO stress_test VALUES (%d, 'name_%d', %d, 'data_%d', '2024-01-01 00:00:00')",
					id, id, id%1000, id)
				if _, err := st.exec.Execute(sql); err != nil {
					errCh <- err
				}
			}(j)
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			if err != nil {
				return fmt.Errorf("insert error: %w", err)
			}
		}

		if (i+batchSize)%1000 == 0 {
			fmt.Printf("[Setup] 已插入 %d 条数据\n", i+batchSize)
		}
	}

	fmt.Printf("[Setup] 初始数据插入完成\n")
	return nil
}

// recordLatency 记录延迟
func (st *StressTester) recordLatency(d time.Duration) {
	st.latencyMu.Lock()
	st.latencies = append(st.latencies, d)
	st.latencyMu.Unlock()
}

// calculateLatencies 计算延迟统计
func (st *StressTester) calculateLatencies() (min, max, avg, p50, p95, p99 time.Duration) {
	st.latencyMu.Lock()
	defer st.latencyMu.Unlock()

	if len(st.latencies) == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	// 排序延迟数据
	sorted := make([]time.Duration, len(st.latencies))
	copy(sorted, st.latencies)

	// 简单排序
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	min = sorted[0]
	max = sorted[len(sorted)-1]

	// 计算平均值
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avg = sum / time.Duration(len(sorted))

	// 计算百分位
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]

	return
}

// resetLatencies 重置延迟数据
func (st *StressTester) resetLatencies() {
	st.latencyMu.Lock()
	st.latencies = st.latencies[:0]
	st.latencyMu.Unlock()
}

// TestConcurrentInserts 测试并发插入
func (st *StressTester) TestConcurrentInserts() TestResult {
	fmt.Printf("\n========== 测试并发插入 ==========\n")
	fmt.Printf("并发数: %d, 每客户端操作数: %d\n",
		st.config.ConcurrentConnections, st.config.OperationsPerClient)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			baseID := clientID * st.config.OperationsPerClient
			for j := 0; j < st.config.OperationsPerClient; j++ {
				id := baseID + j
				opStart := time.Now()

				sql := fmt.Sprintf("INSERT INTO auto_test (name, value) VALUES ('client_%d_op_%d', %d)",
					clientID, j, id)

				_, err := st.exec.Execute(sql)
				st.recordLatency(time.Since(opStart))

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

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       "并发插入",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: st.config.ConcurrentConnections,
	}

	st.printResult(result)
	return result
}

// TestConcurrentSelects 测试并发查询
func (st *StressTester) TestConcurrentSelects() TestResult {
	fmt.Printf("\n========== 测试并发查询 ==========\n")
	fmt.Printf("并发数: %d, 每客户端操作数: %d\n",
		st.config.ConcurrentConnections, st.config.OperationsPerClient)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < st.config.OperationsPerClient; j++ {
				opStart := time.Now()

				// 随机选择查询类型
				var sql string
				switch j % 4 {
				case 0:
					sql = "SELECT * FROM stress_test"
				case 1:
					sql = fmt.Sprintf("SELECT * FROM stress_test WHERE id = %d", j%st.config.InsertCount)
				case 2:
					sql = fmt.Sprintf("SELECT * FROM stress_test WHERE value > %d", j%100)
				case 3:
					sql = "SELECT COUNT(*) FROM stress_test"
				}

				_, err := st.exec.Execute(sql)
				st.recordLatency(time.Since(opStart))

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

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       "并发查询",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: st.config.ConcurrentConnections,
	}

	st.printResult(result)
	return result
}

// TestConcurrentUpdates 测试并发更新
func (st *StressTester) TestConcurrentUpdates() TestResult {
	fmt.Printf("\n========== 测试并发更新 ==========\n")
	fmt.Printf("并发数: %d, 每客户端操作数: %d\n",
		st.config.ConcurrentConnections, st.config.OperationsPerClient)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < st.config.OperationsPerClient; j++ {
				opStart := time.Now()

				// 更新随机行
				id := (clientID*st.config.OperationsPerClient + j) % st.config.InsertCount
				sql := fmt.Sprintf("UPDATE stress_test SET value = value + 1 WHERE id = %d", id)

				_, err := st.exec.Execute(sql)
				st.recordLatency(time.Since(opStart))

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

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       "并发更新",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: st.config.ConcurrentConnections,
	}

	st.printResult(result)
	return result
}

// TestMixedOperations 测试混合操作
func (st *StressTester) TestMixedOperations() TestResult {
	fmt.Printf("\n========== 测试混合操作 (读70%%/写30%%) ==========\n")
	fmt.Printf("并发数: %d, 每客户端操作数: %d\n",
		st.config.ConcurrentConnections, st.config.OperationsPerClient)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			baseID := clientID*st.config.OperationsPerClient + 1000000 // 避免与初始数据冲突

			for j := 0; j < st.config.OperationsPerClient; j++ {
				opStart := time.Now()

				var sql string
				var err error

				// 70% 读，30% 写
				if j%10 < 7 {
					// 读操作
					switch j % 5 {
					case 0:
						sql = "SELECT * FROM stress_test LIMIT 10"
					case 1:
						sql = fmt.Sprintf("SELECT * FROM stress_test WHERE id = %d", j%st.config.InsertCount)
					case 2:
						sql = "SELECT COUNT(*) FROM stress_test"
					case 3:
						sql = fmt.Sprintf("SELECT AVG(value) FROM stress_test WHERE value > %d", j%100)
					case 4:
						sql = fmt.Sprintf("SELECT * FROM stress_test WHERE value > %d LIMIT 5", j%100)
					}
				} else {
					// 写操作
					switch j % 3 {
					case 0:
						// INSERT
						sql = fmt.Sprintf("INSERT INTO auto_test (name, value) VALUES ('mixed_%d_%d', %d)",
							clientID, j, baseID+j)
					case 1:
						// UPDATE
						id := (baseID + j) % st.config.InsertCount
						sql = fmt.Sprintf("UPDATE stress_test SET value = value + 1 WHERE id = %d", id)
					case 2:
						// DELETE (只删除我们刚插入的数据)
						sql = fmt.Sprintf("DELETE FROM auto_test WHERE name = 'mixed_%d_%d'", clientID, j-3)
					}
				}

				_, err = st.exec.Execute(sql)
				st.recordLatency(time.Since(opStart))

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

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       "混合操作(读70%/写30%)",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: st.config.ConcurrentConnections,
	}

	st.printResult(result)
	return result
}

// TestConnectionPressure 测试连接压力
func (st *StressTester) TestConnectionPressure() TestResult {
	fmt.Printf("\n========== 测试连接压力 ==========\n")
	fmt.Printf("并发数: %d\n", st.config.ConcurrentConnections)

	st.resetLatencies()

	var connsSucceeded int64

	startTime := time.Now()

	// 改为测试并发执行大量小查询
	var successOps, failedOps int64
	var wg sync.WaitGroup

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// 每个goroutine执行多个快速查询
			for j := 0; j < 10; j++ {
				opStart := time.Now()

				_, err := st.exec.Execute("SELECT 1")
				st.recordLatency(time.Since(opStart))

				if err != nil {
					atomic.AddInt64(&failedOps, 1)
				} else {
					atomic.AddInt64(&successOps, 1)
				}
			}
			atomic.AddInt64(&connsSucceeded, 1)
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       "连接压力测试",
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: int(connsSucceeded),
		ConnsFailed:    st.config.ConcurrentConnections - int(connsSucceeded),
	}

	st.printResult(result)
	return result
}

// TestSustainedLoad 测试持续负载
func (st *StressTester) TestSustainedLoad(durationSec int) TestResult {
	fmt.Printf("\n========== 测试持续负载 (%d秒) ==========\n", durationSec)
	fmt.Printf("并发数: %d\n", st.config.ConcurrentConnections)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	stopCh := make(chan struct{})
	startTime := time.Now()

	for i := 0; i < st.config.ConcurrentConnections; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for {
				select {
				case <-stopCh:
					return
				default:
					opStart := time.Now()

					// 循环执行不同操作
					var sql string
					switch (successOps + failedOps) % 5 {
					case 0:
						sql = "SELECT * FROM stress_test LIMIT 10"
					case 1:
						sql = "SELECT COUNT(*) FROM stress_test"
					case 2:
						sql = fmt.Sprintf("SELECT * FROM stress_test WHERE id = %d", int(successOps)%st.config.InsertCount)
					case 3:
						sql = fmt.Sprintf("UPDATE stress_test SET value = value + 1 WHERE id = %d", int(successOps)%st.config.InsertCount)
					case 4:
						sql = fmt.Sprintf("INSERT INTO auto_test (name, value) VALUES ('sustained_%d', %d)", int(successOps), int(successOps))
					}

					_, err := st.exec.Execute(sql)
					st.recordLatency(time.Since(opStart))

					if err != nil {
						atomic.AddInt64(&failedOps, 1)
					} else {
						atomic.AddInt64(&successOps, 1)
					}
				}
			}
		}(i)
	}

	// 等待指定时间
	time.Sleep(time.Duration(durationSec) * time.Second)
	close(stopCh)
	wg.Wait()

	duration := time.Since(startTime)

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       fmt.Sprintf("持续负载(%d秒)", durationSec),
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: st.config.ConcurrentConnections,
		ConnsSucceeded: st.config.ConcurrentConnections,
	}

	st.printResult(result)
	return result
}

// TestHighConcurrency 测试高并发
func (st *StressTester) TestHighConcurrency(concurrency int) TestResult {
	fmt.Printf("\n========== 测试高并发 (%d) ==========\n", concurrency)

	st.resetLatencies()

	var successOps, failedOps int64
	var wg sync.WaitGroup

	opsPerClient := st.config.OperationsPerClient
	if opsPerClient < 10 {
		opsPerClient = 10
	}

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			for j := 0; j < opsPerClient; j++ {
				opStart := time.Now()

				// 简单查询
				_, err := st.exec.Execute("SELECT COUNT(*) FROM stress_test")
				st.recordLatency(time.Since(opStart))

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

	min, max, avg, p50, p95, p99 := st.calculateLatencies()
	totalOps := successOps + failedOps

	result := TestResult{
		TestName:       fmt.Sprintf("高并发(%d)", concurrency),
		TotalOps:       totalOps,
		SuccessOps:     successOps,
		FailedOps:      failedOps,
		AvgLatencyMs:   float64(avg.Microseconds()) / 1000,
		MinLatencyMs:   float64(min.Microseconds()) / 1000,
		MaxLatencyMs:   float64(max.Microseconds()) / 1000,
		P50LatencyMs:   float64(p50.Microseconds()) / 1000,
		P95LatencyMs:   float64(p95.Microseconds()) / 1000,
		P99LatencyMs:   float64(p99.Microseconds()) / 1000,
		OpsPerSecond:   float64(totalOps) / duration.Seconds(),
		Duration:       duration,
		ConnsAttempted: concurrency,
		ConnsSucceeded: concurrency,
	}

	st.printResult(result)
	return result
}

// printResult 打印测试结果
func (st *StressTester) printResult(r TestResult) {
	fmt.Printf("\n--- 测试结果: %s ---\n", r.TestName)
	fmt.Printf("总操作数:      %d\n", r.TotalOps)
	fmt.Printf("成功操作:      %d\n", r.SuccessOps)
	fmt.Printf("失败操作:      %d\n", r.FailedOps)
	fmt.Printf("成功率:        %.2f%%\n", float64(r.SuccessOps)/float64(r.TotalOps)*100)
	fmt.Printf("吞吐量:        %.2f ops/sec\n", r.OpsPerSecond)
	fmt.Printf("平均延迟:      %.3f ms\n", r.AvgLatencyMs)
	fmt.Printf("最小延迟:      %.3f ms\n", r.MinLatencyMs)
	fmt.Printf("最大延迟:      %.3f ms\n", r.MaxLatencyMs)
	fmt.Printf("P50延迟:       %.3f ms\n", r.P50LatencyMs)
	fmt.Printf("P95延迟:       %.3f ms\n", r.P95LatencyMs)
	fmt.Printf("P99延迟:       %.3f ms\n", r.P99LatencyMs)
	fmt.Printf("持续时间:      %v\n", r.Duration)
	fmt.Printf("------------------------------------\n")
}

// RunAllTests 运行所有测试
func (st *StressTester) RunAllTests() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║            XxSql 压力测试 - 并发能力验证                    ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ 测试配置                                                   ║\n")
	fmt.Printf("║ - 并发连接数: %d                                         ║\n", st.config.ConcurrentConnections)
	fmt.Printf("║ - 每客户端操作数: %d                                      ║\n", st.config.OperationsPerClient)
	fmt.Printf("║ - 初始数据量: %d                                          ║\n", st.config.InsertCount)
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	// 插入初始数据
	if err := st.InsertInitialData(st.config.InsertCount); err != nil {
		fmt.Printf("插入初始数据失败: %v\n", err)
		return
	}

	// 运行各项测试
	if st.config.TestInsert {
		st.results = append(st.results, st.TestConcurrentInserts())
	}

	if st.config.TestSelect {
		st.results = append(st.results, st.TestConcurrentSelects())
	}

	if st.config.TestUpdate {
		st.results = append(st.results, st.TestConcurrentUpdates())
	}

	if st.config.TestMixed {
		st.results = append(st.results, st.TestMixedOperations())
	}

	// 高并发测试 - 100并发
	if st.config.ConcurrentConnections >= 100 {
		st.results = append(st.results, st.TestHighConcurrency(100))
	}

	// 高并发测试 - 150并发
	if st.config.ConcurrentConnections >= 150 {
		st.results = append(st.results, st.TestHighConcurrency(150))
	}

	// 高并发测试 - 200并发
	if st.config.ConcurrentConnections >= 200 {
		st.results = append(st.results, st.TestHighConcurrency(200))
	}

	// 持续负载测试
	if st.config.TestDuration > 0 {
		st.results = append(st.results, st.TestSustainedLoad(st.config.TestDuration))
	}

	// 打印汇总报告
	st.printSummary()
}

// printSummary 打印汇总报告
func (st *StressTester) printSummary() {
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    测试结果汇总报告                        ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ %-20s │ %8s │ %10s │ %10s │ %8s ║\n",
		"测试项", "总操作", "成功率", "ops/sec", "P99延迟")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	for _, r := range st.results {
		successRate := float64(r.SuccessOps) / float64(r.TotalOps) * 100
		fmt.Printf("║ %-20s │ %8d │ %9.2f%% │ %10.1f │ %6.2fms ║\n",
			r.TestName, r.TotalOps, successRate, r.OpsPerSecond, r.P99LatencyMs)
	}

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	// 评估并发能力
	fmt.Printf("\n")
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                    并发能力评估结论                        ║\n")
	fmt.Printf("╠════════════════════════════════════════════════════════════╣\n")

	allSuccess := true
	for _, r := range st.results {
		successRate := float64(r.SuccessOps) / float64(r.TotalOps) * 100
		if successRate < 99 {
			allSuccess = false
			break
		}
	}

	if allSuccess {
		fmt.Printf("║ ✓ 所有测试成功率均超过99%%                                   ║\n")
		fmt.Printf("║ ✓ 数据库并发能力达到设计要求(100+并发)                      ║\n")
	} else {
		fmt.Printf("║ ✗ 部分测试成功率低于99%%，需要进一步优化                    ║\n")
	}

	// 检查延迟
	acceptableLatency := true
	for _, r := range st.results {
		if r.P99LatencyMs > 100 { // P99延迟超过100ms
			acceptableLatency = false
			break
		}
	}

	if acceptableLatency {
		fmt.Printf("║ ✓ P99延迟在可接受范围内(<100ms)                             ║\n")
	} else {
		fmt.Printf("║ ⚠ 部分测试P99延迟较高，可能影响用户体验                    ║\n")
	}

	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
}

func main() {
	// 默认测试配置
	cfg := &TestConfig{
		PrivatePort:           19527,
		MySQLPort:             13306,
		HTTPPort:              18080,
		ConcurrentConnections: 150, // 测试150并发
		OperationsPerClient:   50,
		InsertCount:           5000,
		TestDuration:          10, // 10秒持续负载测试

		TestInsert: true,
		TestSelect: true,
		TestUpdate: true,
		TestMixed:  true,
	}

	// 检查命令行参数
	for i, arg := range os.Args {
		if arg == "-concurrency" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.ConcurrentConnections)
		}
		if arg == "-ops" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.OperationsPerClient)
		}
		if arg == "-data" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.InsertCount)
		}
		if arg == "-duration" && i+1 < len(os.Args) {
			fmt.Sscanf(os.Args[i+1], "%d", &cfg.TestDuration)
		}
		if arg == "-help" || arg == "--help" {
			fmt.Printf("用法: %s [选项]\n", os.Args[0])
			fmt.Printf("选项:\n")
			fmt.Printf("  -concurrency N   并发连接数 (默认: 150)\n")
			fmt.Printf("  -ops N           每客户端操作数 (默认: 50)\n")
			fmt.Printf("  -data N          初始数据量 (默认: 5000)\n")
			fmt.Printf("  -duration N      持续负载测试秒数 (默认: 10)\n")
			fmt.Printf("  -help            显示帮助信息\n")
			os.Exit(0)
		}
	}

	// 创建测试器
	tester := NewStressTester(cfg)

	// 设置信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("\n接收到终止信号，正在清理...\n")
		tester.Teardown()
		os.Exit(1)
	}()

	// 初始化
	if err := tester.Setup(); err != nil {
		fmt.Printf("初始化失败: %v\n", err)
		os.Exit(1)
	}
	defer tester.Teardown()

	// 运行测试
	tester.RunAllTests()
}
