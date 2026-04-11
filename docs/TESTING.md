# XxSql 测试规范

## 概述

本文档描述 XxSql 数据库项目的测试规范和最佳实践。

## 压力测试报告

### 并发能力测试结果 (2026-03-26)

| 并发数 | 测试项 | 吞吐量(ops/sec) | P99延迟(ms) | 数据完整性 |
|--------|--------|-----------------|-------------|------------|
| 100 | 热点行竞争 | 136,281 | 4.67 | ✓ |
| 100 | 多行更新(死锁检测) | 63,362 | 4.99 | ✓ |
| 100 | 账户转账 | 29,859 | 6.57 | ✓ |
| 100 | 商品秒杀 | 13,261 | 5.69 | ✓ |
| 100 | 读写混合 | 3,398 | 190.23 | ✓ |

### 并发问题检测结果

| 检测项 | 结果 |
|--------|------|
| 数据冲突 | 无 |
| 死锁 | 无 |
| 长时间阻塞 | 无 |
| 超时请求 | 0 |
| 内存泄漏 | 无 |
| Goroutine泄漏 | 无 |

### 功能测试结果

| 测试类别 | 通过 | 失败 | 通过率 |
|----------|------|------|--------|
| 数据类型 | 9 | 0 | 100% |
| SQL语法 | 11 | 3 | 78.6% |
| 约束 | 4 | 1 | 80% |
| 并发边界 | 4 | 0 | 100% |
| 内存压力 | 3 | 0 | 100% |
| 错误处理 | 6 | 0 | 100% |
| 边界条件 | 6 | 0 | 100% |
| 内置函数 | 3 | 1 | 75% |
| 持久化 | 3 | 0 | 100% |
| **总计** | **49** | **5** | **90.7%** |

详细测试报告位于 `result/` 目录。

## 测试结构

```
repo/
├── internal/
│   ├── auth/auth_test.go          # 单元测试
│   ├── storage/
│   │   ├── page/page_test.go
│   │   ├── row/row_test.go
│   │   └── ...
├── tests/
│   └── integration/
│       └── integration_test.go    # 集成测试
└── coverage.out                   # 覆盖率报告
```

## 运行测试

### 基本命令

```bash
# 运行所有测试
make test
# 或
go test ./...

# 运行带详细输出
go test -v ./...

# 运行带竞态检测
make test-race
# 或
go test -race ./...

# 运行集成测试
make test-integration
# 或
go test -v ./tests/integration/...
```

### 覆盖率

```bash
# 生成覆盖率报告
make test-coverage

# 查看覆盖率摘要
make coverage-report

# 在浏览器中查看详细报告
go tool cover -html=coverage.out
```

### 性能测试

```bash
# 运行所有基准测试
make bench

# 运行特定基准测试
go test -bench=BenchmarkInsert -benchmem ./tests/integration/...
```

## 测试原则

### 1. 表驱动测试

使用 `[]struct{}` 组织测试用例：

```go
func TestFunction(t *testing.T) {
    tests := []struct {
        name     string
        input    int
        expected int
        wantErr  bool
    }{
        {"positive", 1, 1, false},
        {"negative", -1, -1, false},
        {"zero", 0, 0, false},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := Function(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("Function() error = %v, wantErr %v", err, tt.wantErr)
            }
            if got != tt.expected {
                t.Errorf("Function() = %v, want %v", got, tt.expected)
            }
        })
    }
}
```

### 2. 子测试

使用 `t.Run()` 分组相关测试：

```go
func TestPage(t *testing.T) {
    t.Run("Header", func(t *testing.T) {
        // 测试页头操作
    })
    t.Run("Slots", func(t *testing.T) {
        // 测试槽位操作
    })
}
```

### 3. 错误路径测试

必须测试错误情况：

```go
func TestErrors(t *testing.T) {
    t.Run("NotFound", func(t *testing.T) {
        _, err := GetItem(999)
        if err == nil {
            t.Error("expected error for non-existent item")
        }
    })

    t.Run("InvalidInput", func(t *testing.T) {
        _, err := Parse("")
        if err == nil {
            t.Error("expected error for empty input")
        }
    })
}
```

### 4. 边界条件

测试空值、极限值、溢出：

```go
func TestBoundary(t *testing.T) {
    t.Run("Empty", func(t *testing.T) {
        // 测试空输入
    })
    t.Run("MaxValue", func(t *testing.T) {
        // 测试最大值
    })
    t.Run("MinValue", func(t *testing.T) {
        // 测试最小值
    })
}
```

### 5. 并发安全

存储层必须测试并发：

```go
func TestConcurrent(t *testing.T) {
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            // 并发操作
        }()
    }
    wg.Wait()
}
```

### 6. 资源清理

使用 `t.Cleanup()` 清理临时文件：

```go
func TestWithTempDir(t *testing.T) {
    tmpDir, err := os.MkdirTemp("", "test-*")
    if err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() {
        os.RemoveAll(tmpDir)
    })
    // 使用 tmpDir
}
```

## 测试分类

### 单元测试

- 测试单个函数或方法
- 快速执行（毫秒级）
- 无外部依赖
- 文件位置：`*_test.go` 在源码同目录

### 集成测试

- 测试多个组件协作
- 可能需要临时目录/文件
- 文件位置：`tests/integration/`

### 基准测试

- 测试性能
- 函数名以 `Benchmark` 开头
- 使用 `b.ResetTimer()` 和 `b.N`

```go
func BenchmarkOperation(b *testing.B) {
    // 设置
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        // 被测操作
    }
}
```

## 覆盖率目标

| 模块类型 | 目标覆盖率 | 当前覆盖率 |
|----------|------------|------------|
| 存储核心 | ≥ 85% | 89.4% ✅ |
| 协议层 | ≥ 80% | 86.4% ✅ |
| 执行器 | ≥ 80% | 81.5% ✅ |
| 认证安全 | ≥ 85% | 91.4% ✅ |
| 工具类 | ≥ 80% | 83.1% ✅ |
| **总体** | **≥ 80%** | **87.5%** ✅ |

### 各模块详细覆盖率

| 模块 | 覆盖率 |
|------|--------|
| pkg/errors | 98.0% |
| storage/page | 100.0% |
| config | 96.7% |
| auth | 93.9% |
| storage/catalog | 90.5% |
| storage | 89.4% |
| storage/row | 89.1% |
| storage/btree | 89.0% |
| storage/checkpoint | 88.9% |
| security | 88.8% |
| storage/types | 88.1% |
| protocol | 86.4% |
| storage/buffer | 86.2% |
| web | 86.1% |
| storage/table | 85.9% |
| cmd/xxsqls | 85.7% |
| storage/lock | 84.4% |
| mysql | 84.8% |
| storage/wal | 84.8% |
| sql | 84.3% |
| storage/sequence | 85.1% |
| storage/recovery | 83.3% |
| pkg/xxsql | 83.5% |
| server | 83.8% |
| backup | 83.1% |
| log | 82.0% |
| executor | 81.5% |
| cmd/xxsqlc | 77.9% |

## CI/CD 集成

GitHub Actions 工作流在 `.github/workflows/test.yml` 中定义：

- **每次 push/PR** 自动运行测试
- **多平台**：Linux, Windows, macOS
- **多 Go 版本**：1.21, 1.22
- **竞态检测**：`go test -race`
- **覆盖率上传**：Codecov

## 常见问题

### Q: 如何跳过长时间运行的测试？

```go
func TestLongRunning(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping in short mode")
    }
    // 长时间测试
}
```

运行时使用 `go test -short` 跳过。

### Q: 如何测试私有函数？

在同一个包中编写测试文件，使用 `package_test` 导入。

### Q: 如何 mock 依赖？

对于接口，可以创建 mock 实现：

```go
type MockStorage struct {
    data map[string]string
}

func (m *MockStorage) Get(key string) (string, error) {
    return m.data[key], nil
}
```

## 相关文档

- [Go Testing 文档](https://golang.org/pkg/testing/)
- [表驱动测试](https://dave.cheney.net/2019/05/07/prefer-table-driven-tests)
- [测试覆盖率](https://blog.golang.org/cover)

## 压力测试结果

### 并发能力验证 (2026-03-26)

| 并发数 | 测试项 | 吞吐量(ops/sec) | P99延迟(ms) | 数据完整性 |
|--------|--------|-----------------|-------------|------------|
| 100 | 热点行竞争 | 136,281 | 4.67 | ✓ |
| 100 | 多行更新 | 63,362 | 4.99 | ✓ |
| 100 | 账户转账 | 29,859 | 6.57 | ✓ |
| 100 | 商品秒杀 | 13,261 | 5.69 | ✓ |
| 100 | 读写混合 | 3,398 | 190.23 | ✓ |

### 并发问题检测结果

| 检测项 | 结果 |
|--------|------|
| 数据冲突 | 无 |
| 死锁 | 无 |
| 长时间阻塞 | 无 |
| 超时请求 | 0 |
| 内存泄漏 | 无 |
| Goroutine泄漏 | 无 |

### 全面功能测试结果

| 测试类别 | 通过 | 失败 | 通过率 |
|----------|------|------|--------|
| 数据类型 | 9 | 0 | 100% |
| SQL语法 | 11 | 3 | 78.6% |
| 约束 | 4 | 1 | 80% |
| 并发边界 | 4 | 0 | 100% |
| 内存压力 | 3 | 0 | 100% |
| 错误处理 | 6 | 0 | 100% |
| 边界条件 | 6 | 0 | 100% |
| 内置函数 | 3 | 1 | 75% |
| 持久化 | 3 | 0 | 100% |
| **总计** | **49** | **5** | **90.7%** |

### 已知限制

1. **SQL语法限制**:
   - IN 操作符: 可能解析失败
   - BETWEEN 操作符: 可能返回空结果
   - 子查询: 可能返回空结果
   - LIMIT: 可能不正确限制行数
   - DISTINCT: 可能不去重

2. **行大小限制**:
   - 最大行大小: 3500 字节

3. **日期函数限制**:
   - YEAR/MONTH/DAY 可能不正常工作

### 规避方案

```sql
-- 替代 IN
WHERE id = 1 OR id = 2 OR id = 3

-- 替代 BETWEEN
WHERE price >= 10 AND price <= 20

-- 替代日期函数
SELECT STRFTIME('%Y', date_column) AS year
```
