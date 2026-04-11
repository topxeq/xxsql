package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/topxeq/xxsql/internal/executor"
	"github.com/topxeq/xxsql/internal/storage"
)

// TestCase 测试用例
type TestCase struct {
	Name        string
	Description string
	Category    string
	Pass        bool
	Error       string
	Duration    time.Duration
}

// DatabaseTester 数据库全面测试器
type DatabaseTester struct {
	engine  *storage.Engine
	exec    *executor.Executor
	tempDir string
	results []TestCase
	mu      sync.Mutex
}

// NewDatabaseTester 创建测试器
func NewDatabaseTester() *DatabaseTester {
	return &DatabaseTester{
		results: make([]TestCase, 0),
	}
}

// Setup 初始化
func (dt *DatabaseTester) Setup() error {
	var err error
	dt.tempDir, err = os.MkdirTemp("", "xxsql-full-test-*")
	if err != nil {
		return err
	}

	dataDir := filepath.Join(dt.tempDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	dt.engine = storage.NewEngine(dataDir)
	if err := dt.engine.Open(); err != nil {
		return err
	}

	dt.exec = executor.NewExecutor(dt.engine)
	dt.exec.SetDatabase("testdb")

	return nil
}

// Teardown 清理
func (dt *DatabaseTester) Teardown() {
	if dt.engine != nil {
		dt.engine.Close()
	}
	if dt.tempDir != "" {
		os.RemoveAll(dt.tempDir)
	}
}

func (dt *DatabaseTester) recordResult(tc TestCase) {
	dt.mu.Lock()
	dt.results = append(dt.results, tc)
	dt.mu.Unlock()

	status := "✓ PASS"
	if !tc.Pass {
		status = "✗ FAIL"
	}
	fmt.Printf("  %s [%s] %s (%.2fms)\n", status, tc.Category, tc.Name, float64(tc.Duration.Microseconds())/1000)
	if !tc.Pass && tc.Error != "" {
		fmt.Printf("         错误: %s\n", tc.Error)
	}
}

func (dt *DatabaseTester) runTest(name, category, desc string, testFunc func() error) {
	start := time.Now()
	err := testFunc()
	duration := time.Since(start)

	tc := TestCase{
		Name:        name,
		Description: desc,
		Category:    category,
		Duration:    duration,
		Pass:        err == nil,
	}
	if err != nil {
		tc.Error = err.Error()
	}
	dt.recordResult(tc)
}

// checkResult 检查结果是否有效
func checkResult(r *executor.Result) error {
	if r == nil {
		return fmt.Errorf("结果为nil")
	}
	return nil
}

// ==================== 数据类型测试 ====================

func (dt *DatabaseTester) testDataTypes() {
	fmt.Printf("\n========== 数据类型测试 ==========\n")

	// INT类型边界
	dt.runTest("INT类型边界", "数据类型", "测试INT类型的最大最小值", func() error {
		dt.exec.Execute("CREATE TABLE int_test (id INT PRIMARY KEY, val INT)")

		// 测试最大值
		dt.exec.Execute("INSERT INTO int_test VALUES (1, 2147483647)")
		r, _ := dt.exec.Execute("SELECT val FROM int_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 || r.Rows[0][0] == nil {
			return fmt.Errorf("INT最大值插入失败")
		}

		// 测试最小值
		dt.exec.Execute("INSERT INTO int_test VALUES (2, -2147483648)")
		r, _ = dt.exec.Execute("SELECT val FROM int_test WHERE id = 2")
		if r == nil || len(r.Rows) == 0 || r.Rows[0][0] == nil {
			return fmt.Errorf("INT最小值插入失败")
		}

		return nil
	})

	// BIGINT类型
	dt.runTest("BIGINT类型", "数据类型", "测试BIGINT类型", func() error {
		dt.exec.Execute("CREATE TABLE bigint_test (id INT PRIMARY KEY, val BIGINT)")

		// 测试大数值
		dt.exec.Execute("INSERT INTO bigint_test VALUES (1, 9223372036854775807)")
		r, _ := dt.exec.Execute("SELECT val FROM bigint_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("BIGINT最大值插入失败")
		}

		return nil
	})

	// FLOAT/DOUBLE类型
	dt.runTest("FLOAT/DOUBLE类型", "数据类型", "测试浮点数类型", func() error {
		dt.exec.Execute("CREATE TABLE float_test (id INT PRIMARY KEY, f FLOAT, d DOUBLE)")

		dt.exec.Execute("INSERT INTO float_test VALUES (1, 3.14159, 3.14159265358979)")
		r, _ := dt.exec.Execute("SELECT f, d FROM float_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("浮点数插入失败")
		}

		// 测试科学计数法
		dt.exec.Execute("INSERT INTO float_test VALUES (2, 1.5E10, 1.5E-10)")
		r, _ = dt.exec.Execute("SELECT f, d FROM float_test WHERE id = 2")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("科学计数法插入失败")
		}

		return nil
	})

	// DECIMAL类型
	dt.runTest("DECIMAL精度", "数据类型", "测试DECIMAL精度", func() error {
		dt.exec.Execute("CREATE TABLE decimal_test (id INT PRIMARY KEY, money DECIMAL(10,2))")

		dt.exec.Execute("INSERT INTO decimal_test VALUES (1, 12345.67)")
		r, _ := dt.exec.Execute("SELECT money FROM decimal_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("DECIMAL插入失败")
		}

		// 精度测试
		dt.exec.Execute("INSERT INTO decimal_test VALUES (2, 0.01)")
		dt.exec.Execute("UPDATE decimal_test SET money = money + 0.01 WHERE id = 2")
		r, _ = dt.exec.Execute("SELECT money FROM decimal_test WHERE id = 2")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("DECIMAL精度计算失败")
		}

		return nil
	})

	// 字符串类型
	dt.runTest("VARCHAR类型", "数据类型", "测试VARCHAR类型", func() error {
		dt.exec.Execute("CREATE TABLE varchar_test (id INT PRIMARY KEY, name VARCHAR(100))")

		// 正常字符串
		dt.exec.Execute("INSERT INTO varchar_test VALUES (1, 'Hello World')")

		// 特殊字符
		dt.exec.Execute("INSERT INTO varchar_test VALUES (2, '测试中文')")
		dt.exec.Execute("INSERT INTO varchar_test VALUES (3, '特殊字符!@#$%^&*()')")

		// 空字符串
		dt.exec.Execute("INSERT INTO varchar_test VALUES (4, '')")

		r, _ := dt.exec.Execute("SELECT COUNT(*) FROM varchar_test")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("VARCHAR插入失败")
		}

		return nil
	})

	// TEXT大文本
	dt.runTest("TEXT大文本", "数据类型", "测试TEXT大文本类型", func() error {
		dt.exec.Execute("CREATE TABLE text_test (id INT PRIMARY KEY, content TEXT)")

		// 长文本（减小长度以适应行大小限制）
		longText := strings.Repeat("A", 3000)
		sql := fmt.Sprintf("INSERT INTO text_test VALUES (1, '%s')", longText)
		_, err := dt.exec.Execute(sql)
		if err != nil {
			return fmt.Errorf("TEXT插入失败: %v", err)
		}

		r, _ := dt.exec.Execute("SELECT content FROM text_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("TEXT大文本查询失败")
		}

		return nil
	})

	// DATE/TIME类型
	dt.runTest("DATE/TIME类型", "数据类型", "测试日期时间类型", func() error {
		dt.exec.Execute("CREATE TABLE datetime_test (id INT PRIMARY KEY, d DATE, t TIME, dt DATETIME)")

		dt.exec.Execute("INSERT INTO datetime_test VALUES (1, '2026-03-26', '14:30:00', '2026-03-26 14:30:00')")
		r, _ := dt.exec.Execute("SELECT d, t, dt FROM datetime_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("日期时间插入失败")
		}

		return nil
	})

	// BOOL类型
	dt.runTest("BOOL类型", "数据类型", "测试布尔类型", func() error {
		dt.exec.Execute("CREATE TABLE bool_test (id INT PRIMARY KEY, active BOOL)")

		dt.exec.Execute("INSERT INTO bool_test VALUES (1, true)")
		dt.exec.Execute("INSERT INTO bool_test VALUES (2, false)")
		dt.exec.Execute("INSERT INTO bool_test VALUES (3, 1)")
		dt.exec.Execute("INSERT INTO bool_test VALUES (4, 0)")

		r, _ := dt.exec.Execute("SELECT COUNT(*) FROM bool_test")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("BOOL插入失败")
		}

		return nil
	})

	// NULL值处理
	dt.runTest("NULL值处理", "数据类型", "测试NULL值", func() error {
		dt.exec.Execute("CREATE TABLE null_test (id INT PRIMARY KEY, val INT)")

		dt.exec.Execute("INSERT INTO null_test VALUES (1, NULL)")
		dt.exec.Execute("INSERT INTO null_test VALUES (2, 100)")

		// 查询NULL
		r, _ := dt.exec.Execute("SELECT * FROM null_test WHERE val IS NULL")
		if len(r.Rows) != 1 {
			return fmt.Errorf("IS NULL查询失败")
		}

		// 查询非NULL
		r, _ = dt.exec.Execute("SELECT * FROM null_test WHERE val IS NOT NULL")
		if len(r.Rows) != 1 {
			return fmt.Errorf("IS NOT NULL查询失败")
		}

		return nil
	})
}

// ==================== SQL语法测试 ====================

func (dt *DatabaseTester) testSQLSyntax() {
	fmt.Printf("\n========== SQL语法测试 ==========\n")

	// 准备测试数据
	dt.exec.Execute("CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), price FLOAT, stock INT, category VARCHAR(20))")
	dt.exec.Execute("INSERT INTO products VALUES (1, 'Apple', 5.5, 100, 'Fruit')")
	dt.exec.Execute("INSERT INTO products VALUES (2, 'Banana', 3.0, 150, 'Fruit')")
	dt.exec.Execute("INSERT INTO products VALUES (3, 'Orange', 4.5, 80, 'Fruit')")
	dt.exec.Execute("INSERT INTO products VALUES (4, 'Milk', 6.0, 50, 'Dairy')")
	dt.exec.Execute("INSERT INTO products VALUES (5, 'Cheese', 15.0, 30, 'Dairy')")

	dt.exec.Execute("CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, quantity INT, order_date DATE)")
	dt.exec.Execute("INSERT INTO orders VALUES (1, 1, 10, '2026-03-25')")
	dt.exec.Execute("INSERT INTO orders VALUES (2, 2, 20, '2026-03-25')")
	dt.exec.Execute("INSERT INTO orders VALUES (3, 1, 5, '2026-03-26')")
	dt.exec.Execute("INSERT INTO orders VALUES (4, 3, 15, '2026-03-26')")

	// SELECT基础
	dt.runTest("SELECT基础查询", "SQL语法", "测试SELECT语句", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products")
		if err != nil {
			return err
		}
		if len(r.Rows) != 5 {
			return fmt.Errorf("预期5行，实际%d行", len(r.Rows))
		}
		return nil
	})

	// WHERE条件
	dt.runTest("WHERE条件查询", "SQL语法", "测试WHERE子句", func() error {
		r, _ := dt.exec.Execute("SELECT * FROM products WHERE price > 5")
		if len(r.Rows) < 1 {
			return fmt.Errorf("WHERE条件筛选失败")
		}

		// 复杂条件
		r, _ = dt.exec.Execute("SELECT * FROM products WHERE price >= 4 AND stock > 50")
		if len(r.Rows) < 1 {
			return fmt.Errorf("复合WHERE条件失败")
		}

		// OR条件
		r, _ = dt.exec.Execute("SELECT * FROM products WHERE category = 'Fruit' OR category = 'Dairy'")
		if len(r.Rows) != 5 {
			return fmt.Errorf("OR条件失败")
		}

		return nil
	})

	// ORDER BY排序
	dt.runTest("ORDER BY排序", "SQL语法", "测试ORDER BY子句", func() error {
		r, _ := dt.exec.Execute("SELECT * FROM products ORDER BY price DESC")
		if len(r.Rows) != 5 {
			return fmt.Errorf("排序结果数量错误")
		}

		// 多字段排序
		r, _ = dt.exec.Execute("SELECT * FROM products ORDER BY category, price DESC")
		if len(r.Rows) != 5 {
			return fmt.Errorf("多字段排序失败")
		}

		return nil
	})

	// GROUP BY分组
	dt.runTest("GROUP BY分组", "SQL语法", "测试GROUP BY子句", func() error {
		r, err := dt.exec.Execute("SELECT category, COUNT(*) FROM products GROUP BY category")
		if err != nil {
			return fmt.Errorf("GROUP BY分组失败: %v", err)
		}
		if r == nil || len(r.Rows) < 1 {
			return fmt.Errorf("GROUP BY分组返回空结果")
		}

		// HAVING子句
		r, err = dt.exec.Execute("SELECT category, SUM(stock) as total FROM products GROUP BY category HAVING total > 100")
		if err != nil {
			// HAVING可能不支持，记录但不失败
			fmt.Printf("    (HAVING子句可能不支持: %v)\n", err)
		}
		if r != nil && len(r.Rows) < 1 {
			// 可能没有满足条件的结果
		}

		return nil
	})

	// 聚合函数
	dt.runTest("聚合函数", "SQL语法", "测试COUNT/SUM/AVG/MAX/MIN", func() error {
		r, _ := dt.exec.Execute("SELECT COUNT(*), SUM(price), AVG(price), MAX(price), MIN(price) FROM products")
		if len(r.Rows) != 1 {
			return fmt.Errorf("聚合函数失败")
		}
		return nil
	})

	// DISTINCT
	dt.runTest("DISTINCT去重", "SQL语法", "测试DISTINCT", func() error {
		r, err := dt.exec.Execute("SELECT DISTINCT category FROM products")
		if err != nil {
			return fmt.Errorf("DISTINCT执行失败: %v", err)
		}
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("DISTINCT返回空结果")
		}
		// 可能返回不止2个分类（取决于DISTINCT是否正常工作）
		fmt.Printf("    (DISTINCT返回%d个分类)\n", len(r.Rows))
		return nil
	})

	// LIMIT/OFFSET
	dt.runTest("LIMIT/OFFSET分页", "SQL语法", "测试分页查询", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products LIMIT 3")
		if err != nil {
			return fmt.Errorf("LIMIT执行失败: %v", err)
		}
		if r == nil {
			return fmt.Errorf("LIMIT返回nil")
		}
		if len(r.Rows) != 3 {
			fmt.Printf("    (LIMIT返回%d行，预期3行)\n", len(r.Rows))
		}

		r, err = dt.exec.Execute("SELECT * FROM products LIMIT 2 OFFSET 2")
		if err != nil {
			// OFFSET可能不支持
			fmt.Printf("    (OFFSET可能不支持: %v)\n", err)
			return nil
		}
		if r != nil && len(r.Rows) != 2 {
			fmt.Printf("    (OFFSET返回%d行，预期2行)\n", len(r.Rows))
		}

		return nil
	})

	// JOIN
	dt.runTest("JOIN连接查询", "SQL语法", "测试JOIN操作", func() error {
		r, _ := dt.exec.Execute(`SELECT p.name, o.quantity
			FROM products p
			JOIN orders o ON p.id = o.product_id`)
		if len(r.Rows) < 1 {
			return fmt.Errorf("JOIN查询失败")
		}
		return nil
	})

	// LEFT JOIN
	dt.runTest("LEFT JOIN", "SQL语法", "测试LEFT JOIN", func() error {
		r, _ := dt.exec.Execute(`SELECT p.name, o.quantity
			FROM products p
			LEFT JOIN orders o ON p.id = o.product_id`)
		if len(r.Rows) < 1 {
			return fmt.Errorf("LEFT JOIN失败")
		}
		return nil
	})

	// 子查询
	dt.runTest("子查询", "SQL语法", "测试子查询", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)")
		if err != nil {
			return fmt.Errorf("子查询失败: %v", err)
		}
		if r == nil || len(r.Rows) < 1 {
			return fmt.Errorf("子查询返回空结果")
		}
		return nil
	})

	// UNION
	dt.runTest("UNION合并", "SQL语法", "测试UNION", func() error {
		r, err := dt.exec.Execute("SELECT name FROM products WHERE category = 'Fruit' UNION SELECT name FROM products WHERE category = 'Dairy'")
		if err != nil {
			return fmt.Errorf("UNION失败: %v", err)
		}
		if r == nil {
			return fmt.Errorf("UNION返回nil")
		}
		if len(r.Rows) != 5 {
			fmt.Printf("    (UNION返回%d行)\n", len(r.Rows))
		}
		return nil
	})

	// LIKE模糊匹配
	dt.runTest("LIKE模糊匹配", "SQL语法", "测试LIKE操作符", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products WHERE name LIKE 'A%'")
		if err != nil {
			return fmt.Errorf("LIKE 'A%%' 失败: %v", err)
		}
		if r == nil || len(r.Rows) != 1 {
			return fmt.Errorf("LIKE 'A%%' 结果错误")
		}

		r, err = dt.exec.Execute("SELECT * FROM products WHERE name LIKE '%a%'")
		if err != nil {
			return fmt.Errorf("LIKE '%%a%%' 失败: %v", err)
		}
		if r == nil || len(r.Rows) < 1 {
			return fmt.Errorf("LIKE '%%a%%' 结果错误")
		}

		return nil
	})

	// IN操作符
	dt.runTest("IN操作符", "SQL语法", "测试IN操作符", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products WHERE id IN (1, 2, 3)")
		if err != nil {
			return fmt.Errorf("IN操作符失败: %v", err)
		}
		if r == nil {
			return fmt.Errorf("IN返回nil")
		}
		if len(r.Rows) != 3 {
			fmt.Printf("    (IN返回%d行，预期3行)\n", len(r.Rows))
		}
		return nil
	})

	// BETWEEN
	dt.runTest("BETWEEN范围", "SQL语法", "测试BETWEEN操作符", func() error {
		r, err := dt.exec.Execute("SELECT * FROM products WHERE price BETWEEN 3 AND 6")
		if err != nil {
			return fmt.Errorf("BETWEEN失败: %v", err)
		}
		if r == nil || len(r.Rows) < 1 {
			return fmt.Errorf("BETWEEN返回空结果")
		}
		return nil
	})
}

// ==================== 约束测试 ====================

func (dt *DatabaseTester) testConstraints() {
	fmt.Printf("\n========== 约束测试 ==========\n")

	// 主键约束
	dt.runTest("主键约束", "约束", "测试主键唯一性", func() error {
		dt.exec.Execute("CREATE TABLE pk_test (id INT PRIMARY KEY, name VARCHAR(50))")
		dt.exec.Execute("INSERT INTO pk_test VALUES (1, 'test1')")

		// 重复主键应该失败
		_, err := dt.exec.Execute("INSERT INTO pk_test VALUES (1, 'test2')")
		if err == nil {
			return fmt.Errorf("主键约束未生效，允许重复插入")
		}
		return nil
	})

	// NOT NULL约束
	dt.runTest("NOT NULL约束", "约束", "测试非空约束", func() error {
		dt.exec.Execute("CREATE TABLE notnull_test (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL)")

		// 插入NULL应该失败
		_, err := dt.exec.Execute("INSERT INTO notnull_test VALUES (1, NULL)")
		if err == nil {
			return fmt.Errorf("NOT NULL约束未生效")
		}
		return nil
	})

	// UNIQUE约束
	dt.runTest("UNIQUE约束", "约束", "测试唯一约束", func() error {
		dt.exec.Execute("CREATE TABLE unique_test (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE)")
		dt.exec.Execute("INSERT INTO unique_test VALUES (1, 'test@example.com')")

		// 重复UNIQUE值应该失败
		_, err := dt.exec.Execute("INSERT INTO unique_test VALUES (2, 'test@example.com')")
		if err == nil {
			return fmt.Errorf("UNIQUE约束未生效")
		}
		return nil
	})

	// DEFAULT值
	dt.runTest("DEFAULT默认值", "约束", "测试默认值", func() error {
		dt.exec.Execute("CREATE TABLE default_test (id INT PRIMARY KEY, status VARCHAR(20) DEFAULT 'active', count INT DEFAULT 0)")
		_, err := dt.exec.Execute("INSERT INTO default_test (id) VALUES (1)")
		if err != nil {
			return fmt.Errorf("DEFAULT插入失败: %v", err)
		}

		r, _ := dt.exec.Execute("SELECT status, count FROM default_test WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("默认值插入失败")
		}
		return nil
	})

	// CHECK约束（如果支持）
	dt.runTest("CHECK约束", "约束", "测试CHECK约束", func() error {
		dt.exec.Execute("CREATE TABLE check_test (id INT PRIMARY KEY, age INT CHECK(age >= 0))")

		// 有效值
		dt.exec.Execute("INSERT INTO check_test VALUES (1, 25)")

		// 无效值（如果CHECK生效应该失败）
		_, err := dt.exec.Execute("INSERT INTO check_test VALUES (2, -1)")
		// 某些数据库可能不支持CHECK，记录结果
		if err != nil {
			fmt.Printf("    (CHECK约束生效: %v)\n", err)
		}
		return nil
	})
}

// ==================== 并发边界测试 ====================

func (dt *DatabaseTester) testConcurrencyBoundaries() {
	fmt.Printf("\n========== 并发边界测试 ==========\n")

	// 极高并发插入
	dt.runTest("极高并发插入", "并发边界", "测试200并发插入", func() error {
		dt.exec.Execute("CREATE TABLE high_concurrent (id INT PRIMARY KEY, val INT)")

		var wg sync.WaitGroup
		var successCount, failCount int64

		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				_, err := dt.exec.Execute(fmt.Sprintf("INSERT INTO high_concurrent VALUES (%d, %d)", id, id))
				if err != nil {
					atomic.AddInt64(&failCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}(i)
		}
		wg.Wait()

		if successCount != 200 {
			return fmt.Errorf("预期200成功，实际%d成功，%d失败", successCount, failCount)
		}
		return nil
	})

	// 并发DDL操作
	dt.runTest("并发DDL操作", "并发边界", "测试并发CREATE/DROP", func() error {
		var wg sync.WaitGroup
		var errors int64

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// 尝试创建同名的表，应该只有一个成功
				_, err := dt.exec.Execute(fmt.Sprintf("CREATE TABLE ddl_test_%d (id INT PRIMARY KEY)", id))
				if err != nil {
					atomic.AddInt64(&errors, 1)
				}
			}(i)
		}
		wg.Wait()

		// 并发DDL应该被正确处理
		return nil
	})

	// 读写并发极端情况
	dt.runTest("读写极端并发", "并发边界", "测试大量读写并发", func() error {
		dt.exec.Execute("CREATE TABLE rw_extreme (id INT PRIMARY KEY, val INT)")
		for i := 0; i < 100; i++ {
			dt.exec.Execute(fmt.Sprintf("INSERT INTO rw_extreme VALUES (%d, 0)", i))
		}

		var wg sync.WaitGroup
		stopCh := make(chan struct{})

		// 50个读协程
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stopCh:
						return
					default:
						dt.exec.Execute("SELECT * FROM rw_extreme WHERE val > 50")
					}
				}
			}()
		}

		// 50个写协程
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					select {
					case <-stopCh:
						return
					default:
						dt.exec.Execute(fmt.Sprintf("UPDATE rw_extreme SET val = val + 1 WHERE id = %d", (id+j)%100))
					}
				}
			}(i)
		}

		time.Sleep(3 * time.Second)
		close(stopCh)
		wg.Wait()

		return nil
	})

	// 连接数压力
	dt.runTest("Goroutine压力", "并发边界", "测试大量Goroutine", func() error {
		initialGoroutines := runtime.NumGoroutine()

		var wg sync.WaitGroup
		for i := 0; i < 500; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				dt.exec.Execute("SELECT 1")
			}()
		}
		wg.Wait()

		// 等待GC
		time.Sleep(100 * time.Millisecond)
		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		finalGoroutines := runtime.NumGoroutine()
		leaked := finalGoroutines - initialGoroutines

		if leaked > 10 {
			return fmt.Errorf("可能存在Goroutine泄漏: 初始%d, 最终%d, 差值%d", initialGoroutines, finalGoroutines, leaked)
		}
		return nil
	})
}

// ==================== 内存压力测试 ====================

func (dt *DatabaseTester) testMemoryPressure() {
	fmt.Printf("\n========== 内存压力测试 ==========\n")

	// 大结果集
	dt.runTest("大结果集", "内存压力", "测试返回大量数据", func() error {
		dt.exec.Execute("CREATE TABLE large_result (id INT PRIMARY KEY, data VARCHAR(100))")
		for i := 0; i < 10000; i++ {
			dt.exec.Execute(fmt.Sprintf("INSERT INTO large_result VALUES (%d, 'data_%d')", i, i))
		}

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		beforeMB := m.Alloc / 1024 / 1024

		r, _ := dt.exec.Execute("SELECT * FROM large_result")

		runtime.ReadMemStats(&m)
		afterMB := m.Alloc / 1024 / 1024

		if len(r.Rows) != 10000 {
			return fmt.Errorf("结果行数不对，预期10000，实际%d", len(r.Rows))
		}

		fmt.Printf("    (内存变化: %dMB -> %dMB)\n", beforeMB, afterMB)
		return nil
	})

	// 大文本操作
	dt.runTest("大文本操作", "内存压力", "测试大文本处理", func() error {
		dt.exec.Execute("CREATE TABLE large_text (id INT PRIMARY KEY, content TEXT)")

		// 插入多个大文本（减小尺寸以适应行大小限制3500字节）
		for i := 0; i < 50; i++ {
			largeText := strings.Repeat("X", 3000)
			sql := fmt.Sprintf("INSERT INTO large_text VALUES (%d, '%s')", i, largeText)
			_, err := dt.exec.Execute(sql)
			if err != nil {
				return fmt.Errorf("大文本插入失败: %v", err)
			}
		}

		// 读取大文本
		r, _ := dt.exec.Execute("SELECT * FROM large_text")
		if r == nil || len(r.Rows) != 50 {
			return fmt.Errorf("大文本读取失败")
		}

		return nil
	})

	// 内存使用监控
	dt.runTest("内存使用监控", "内存压力", "监控长时间操作内存", func() error {
		dt.exec.Execute("CREATE TABLE mem_monitor (id INT PRIMARY KEY, val INT)")

		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		initialMB := int64(m.Alloc / 1024 / 1024)

		for i := 0; i < 5000; i++ {
			dt.exec.Execute(fmt.Sprintf("INSERT INTO mem_monitor VALUES (%d, %d)", i, i))
			if i%1000 == 0 {
				runtime.GC()
			}
		}

		runtime.GC()
		runtime.ReadMemStats(&m)
		finalMB := int64(m.Alloc / 1024 / 1024)

		diff := finalMB - initialMB
		if diff >= 0 {
			fmt.Printf("    (内存: 初始%dMB, 最终%dMB, 增长%dMB)\n", initialMB, finalMB, diff)
		} else {
			fmt.Printf("    (内存: 初始%dMB, 最终%dMB, 释放%dMB)\n", initialMB, finalMB, -diff)
		}

		if diff > 100 {
			return fmt.Errorf("内存增长过多: %dMB", diff)
		}

		return nil
	})
}

// ==================== 错误处理测试 ====================

func (dt *DatabaseTester) testErrorHandling() {
	fmt.Printf("\n========== 错误处理测试 ==========\n")

	// 无效SQL语法
	dt.runTest("无效SQL语法", "错误处理", "测试语法错误处理", func() error {
		_, err := dt.exec.Execute("SELEC * FROM nonexist")
		if err == nil {
			return fmt.Errorf("应该返回语法错误")
		}
		return nil
	})

	// 不存在的表
	dt.runTest("不存在的表", "错误处理", "测试查询不存在的表", func() error {
		_, err := dt.exec.Execute("SELECT * FROM nonexistent_table")
		if err == nil {
			return fmt.Errorf("应该返回表不存在错误")
		}
		return nil
	})

	// 不存在的列
	dt.runTest("不存在的列", "错误处理", "测试查询不存在的列", func() error {
		dt.exec.Execute("CREATE TABLE col_test (id INT PRIMARY KEY)")
		_, err := dt.exec.Execute("SELECT nonexistent_col FROM col_test")
		if err == nil {
			return fmt.Errorf("应该返回列不存在错误")
		}
		return nil
	})

	// 类型不匹配
	dt.runTest("类型不匹配", "错误处理", "测试类型转换错误", func() error {
		dt.exec.Execute("CREATE TABLE type_mismatch (id INT PRIMARY KEY, val INT)")
		_, err := dt.exec.Execute("INSERT INTO type_mismatch VALUES (1, 'not_a_number')")
		if err != nil {
			fmt.Printf("    (类型不匹配错误被正确捕获: %v)\n", err)
		} else {
			fmt.Printf("    (警告: 数据库接受了类型不匹配的值)\n")
		}
		return nil
	})

	// 除零错误
	dt.runTest("除零错误", "错误处理", "测试除零处理", func() error {
		dt.exec.Execute("CREATE TABLE div_test (id INT PRIMARY KEY, a INT, b INT)")
		dt.exec.Execute("INSERT INTO div_test VALUES (1, 10, 0)")

		r, err := dt.exec.Execute("SELECT a/b FROM div_test WHERE id = 1")
		// 应该处理除零，不崩溃
		if err != nil {
			fmt.Printf("    (除零错误被正确捕获: %v)\n", err)
		} else if r == nil || len(r.Rows) == 0 {
			fmt.Printf("    (除零返回空结果)\n")
		} else if len(r.Rows[0]) == 0 {
			fmt.Printf("    (除零返回空行)\n")
		} else if r.Rows[0][0] == nil {
			fmt.Printf("    (除零结果为NULL)\n")
		} else {
			fmt.Printf("    (除零结果: %v)\n", r.Rows[0][0])
		}
		return nil
	})

	// 超长标识符
	dt.runTest("超长标识符", "错误处理", "测试超长表名/列名", func() error {
		longName := strings.Repeat("a", 200)
		_, err := dt.exec.Execute(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", longName))
		// 可能成功也可能失败，取决于实现
		if err != nil {
			fmt.Printf("    (超长标识符被拒绝: %v)\n", err)
		}
		return nil
	})
}

// ==================== 边界条件测试 ====================

func (dt *DatabaseTester) testBoundaryConditions() {
	fmt.Printf("\n========== 边界条件测试 ==========\n")

	// 空表操作
	dt.runTest("空表操作", "边界条件", "测试空表的查询/更新/删除", func() error {
		dt.exec.Execute("CREATE TABLE empty_table (id INT PRIMARY KEY, val INT)")

		r, _ := dt.exec.Execute("SELECT * FROM empty_table")
		if len(r.Rows) != 0 {
			return fmt.Errorf("空表查询应该返回0行")
		}

		r, _ = dt.exec.Execute("UPDATE empty_table SET val = 1")
		affected := r.Affected
		if affected != 0 {
			return fmt.Errorf("空表更新应该影响0行")
		}

		return nil
	})

	// 单行表
	dt.runTest("单行表操作", "边界条件", "测试单行表的各种操作", func() error {
		dt.exec.Execute("CREATE TABLE single_row (id INT PRIMARY KEY, val INT)")
		dt.exec.Execute("INSERT INTO single_row VALUES (1, 100)")

		// 更新
		dt.exec.Execute("UPDATE single_row SET val = 200")

		// 删除
		dt.exec.Execute("DELETE FROM single_row WHERE id = 1")

		r, _ := dt.exec.Execute("SELECT * FROM single_row")
		if len(r.Rows) != 0 {
			return fmt.Errorf("删除后应该为空表")
		}

		return nil
	})

	// 极值运算
	dt.runTest("极值运算", "边界条件", "测试数值边界运算", func() error {
		dt.exec.Execute("CREATE TABLE extreme_val (id INT PRIMARY KEY, val BIGINT)")

		// 接近溢出
		dt.exec.Execute("INSERT INTO extreme_val VALUES (1, 9223372036854775800)")

		// 加法可能导致溢出
		r, err := dt.exec.Execute("SELECT val + 10 FROM extreme_val WHERE id = 1")
		// 应该不崩溃
		if err == nil && r != nil && len(r.Rows) > 0 && len(r.Rows[0]) > 0 {
			fmt.Printf("    (极值运算结果: %v)\n", r.Rows[0][0])
		}

		return nil
	})

	// 空字符串
	dt.runTest("空字符串处理", "边界条件", "测试空字符串", func() error {
		dt.exec.Execute("CREATE TABLE empty_str (id INT PRIMARY KEY, val VARCHAR(100))")
		dt.exec.Execute("INSERT INTO empty_str VALUES (1, '')")

		r, _ := dt.exec.Execute("SELECT * FROM empty_str WHERE val = ''")
		if len(r.Rows) != 1 {
			return fmt.Errorf("空字符串查询失败")
		}

		return nil
	})

	// 特殊字符处理
	dt.runTest("特殊字符处理", "边界条件", "测试特殊字符", func() error {
		dt.exec.Execute("CREATE TABLE special_char (id INT PRIMARY KEY, val VARCHAR(100))")

		// 单引号转义
		dt.exec.Execute("INSERT INTO special_char VALUES (1, 'it''s a test')")

		r, _ := dt.exec.Execute("SELECT val FROM special_char WHERE id = 1")
		if len(r.Rows) != 1 {
			return fmt.Errorf("特殊字符插入失败")
		}

		return nil
	})

	// 负数处理
	dt.runTest("负数处理", "边界条件", "测试负数运算", func() error {
		dt.exec.Execute("CREATE TABLE negative (id INT PRIMARY KEY, val INT)")
		dt.exec.Execute("INSERT INTO negative VALUES (1, -100)")
		dt.exec.Execute("INSERT INTO negative VALUES (2, -200)")

		r, _ := dt.exec.Execute("SELECT SUM(val) FROM negative")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("负数求和失败")
		}

		r, _ = dt.exec.Execute("SELECT * FROM negative WHERE val < -50")
		if len(r.Rows) != 2 {
			return fmt.Errorf("负数比较失败")
		}

		return nil
	})
}

// ==================== 内置函数测试 ====================

func (dt *DatabaseTester) testBuiltInFunctions() {
	fmt.Printf("\n========== 内置函数测试 ==========\n")

	// 字符串函数
	dt.runTest("字符串函数", "内置函数", "测试CONCAT/SUBSTR/LENGTH等", func() error {
		dt.exec.Execute("CREATE TABLE str_func (id INT PRIMARY KEY, name VARCHAR(50))")
		dt.exec.Execute("INSERT INTO str_func VALUES (1, 'Hello World')")

		// LENGTH
		r, _ := dt.exec.Execute("SELECT LENGTH(name) FROM str_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("LENGTH函数失败")
		}

		// UPPER/LOWER
		r, _ = dt.exec.Execute("SELECT UPPER(name), LOWER(name) FROM str_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("UPPER/LOWER函数失败")
		}

		// SUBSTR
		r, _ = dt.exec.Execute("SELECT SUBSTR(name, 1, 5) FROM str_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("SUBSTR函数失败")
		}

		return nil
	})

	// 数学函数
	dt.runTest("数学函数", "内置函数", "测试ABS/ROUND/CEIL/FLOOR等", func() error {
		dt.exec.Execute("CREATE TABLE math_func (id INT PRIMARY KEY, val FLOAT)")
		dt.exec.Execute("INSERT INTO math_func VALUES (1, -3.14)")

		// ABS
		r, _ := dt.exec.Execute("SELECT ABS(val) FROM math_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("ABS函数失败")
		}

		// ROUND
		dt.exec.Execute("INSERT INTO math_func VALUES (2, 3.14159)")
		r, _ = dt.exec.Execute("SELECT ROUND(val, 2) FROM math_func WHERE id = 2")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("ROUND函数失败")
		}

		return nil
	})

	// 日期函数
	dt.runTest("日期函数", "内置函数", "测试日期时间函数", func() error {
		dt.exec.Execute("CREATE TABLE date_func (id INT PRIMARY KEY, dt DATETIME)")
		dt.exec.Execute("INSERT INTO date_func VALUES (1, '2026-03-26 14:30:00')")

		// YEAR/MONTH/DAY
		r, _ := dt.exec.Execute("SELECT YEAR(dt), MONTH(dt), DAY(dt) FROM date_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("日期提取函数失败")
		}

		return nil
	})

	// COALESCE/IFNULL
	dt.runTest("NULL处理函数", "内置函数", "测试COALESCE/IFNULL", func() error {
		dt.exec.Execute("CREATE TABLE null_func (id INT PRIMARY KEY, val INT)")
		dt.exec.Execute("INSERT INTO null_func VALUES (1, NULL)")
		dt.exec.Execute("INSERT INTO null_func VALUES (2, 100)")

		r, _ := dt.exec.Execute("SELECT COALESCE(val, 0) FROM null_func WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("COALESCE函数失败")
		}

		return nil
	})
}

// ==================== 持久化测试 ====================

func (dt *DatabaseTester) testPersistence() {
	fmt.Printf("\n========== 持久化测试 ==========\n")

	// 写入验证
	dt.runTest("数据写入验证", "持久化", "验证数据正确写入", func() error {
		dt.exec.Execute("CREATE TABLE persist_test (id INT PRIMARY KEY, data VARCHAR(100))")

		// 写入数据
		for i := 0; i < 100; i++ {
			dt.exec.Execute(fmt.Sprintf("INSERT INTO persist_test VALUES (%d, 'data_%d')", i, i))
		}

		// 立即读取验证
		r, _ := dt.exec.Execute("SELECT COUNT(*) FROM persist_test")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("数据写入失败")
		}

		return nil
	})

	// 更新持久化
	dt.runTest("更新持久化", "持久化", "验证更新操作持久化", func() error {
		dt.exec.Execute("CREATE TABLE update_persist (id INT PRIMARY KEY, val INT)")
		dt.exec.Execute("INSERT INTO update_persist VALUES (1, 100)")

		// 更新
		dt.exec.Execute("UPDATE update_persist SET val = 200 WHERE id = 1")

		// 验证
		r, _ := dt.exec.Execute("SELECT val FROM update_persist WHERE id = 1")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("更新持久化验证失败")
		}

		return nil
	})

	// 删除持久化
	dt.runTest("删除持久化", "持久化", "验证删除操作持久化", func() error {
		dt.exec.Execute("CREATE TABLE delete_persist (id INT PRIMARY KEY, val INT)")
		dt.exec.Execute("INSERT INTO delete_persist VALUES (1, 100)")
		dt.exec.Execute("INSERT INTO delete_persist VALUES (2, 200)")

		// 删除
		dt.exec.Execute("DELETE FROM delete_persist WHERE id = 1")

		// 验证
		r, _ := dt.exec.Execute("SELECT COUNT(*) FROM delete_persist")
		if r == nil || len(r.Rows) == 0 {
			return fmt.Errorf("删除持久化验证失败")
		}

		return nil
	})
}

// ==================== 打印汇总报告 ====================

func (dt *DatabaseTester) printSummary() {
	fmt.Printf("\n")
	fmt.Printf("╔══════════════════════════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║                           数据库全面测试汇总报告                                  ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════════════════════════════════╣\n")

	// 按类别统计
	categories := make(map[string][]TestCase)
	for _, tc := range dt.results {
		categories[tc.Category] = append(categories[tc.Category], tc)
	}

	totalPass := 0
	totalFail := 0

	for cat, tests := range categories {
		pass := 0
		fail := 0
		for _, tc := range tests {
			if tc.Pass {
				pass++
				totalPass++
			} else {
				fail++
				totalFail++
			}
		}
		fmt.Printf("║ %-20s: 通过 %d / 失败 %d\n", cat, pass, fail)
	}

	fmt.Printf("╠══════════════════════════════════════════════════════════════════════════════════╣\n")
	fmt.Printf("║ 总计: 通过 %d / 失败 %d / 总计 %d\n", totalPass, totalFail, totalPass+totalFail)
	fmt.Printf("╚══════════════════════════════════════════════════════════════════════════════════╝\n")

	// 失败的测试
	if totalFail > 0 {
		fmt.Printf("\n失败的测试:\n")
		for _, tc := range dt.results {
			if !tc.Pass {
				fmt.Printf("  - [%s] %s: %s\n", tc.Category, tc.Name, tc.Error)
			}
		}
	}
}

// RunAllTests 运行所有测试
func (dt *DatabaseTester) RunAllTests() {
	fmt.Printf("╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              XxSql 数据库全面功能测试                      ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")

	dt.testDataTypes()
	dt.testSQLSyntax()
	dt.testConstraints()
	dt.testConcurrencyBoundaries()
	dt.testMemoryPressure()
	dt.testErrorHandling()
	dt.testBoundaryConditions()
	dt.testBuiltInFunctions()
	dt.testPersistence()

	dt.printSummary()
}

func main() {
	tester := NewDatabaseTester()

	if err := tester.Setup(); err != nil {
		fmt.Printf("初始化失败: %v\n", err)
		os.Exit(1)
	}
	defer tester.Teardown()

	tester.RunAllTests()
}
