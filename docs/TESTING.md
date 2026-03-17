# XxSql 测试规范

## 概述

本文档描述 XxSql 数据库项目的测试规范和最佳实践。

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

| 模块类型 | 目标覆盖率 |
|----------|------------|
| 存储核心 | ≥ 60% |
| 协议层 | ≥ 50% |
| 执行器 | ≥ 50% |
| 工具类 | ≥ 40% |
| 总体 | ≥ 50% |

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
