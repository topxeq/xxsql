# Contributing to XxSql

Thank you for your interest in contributing to XxSql! This document provides guidelines and instructions for contributing.

## Table of Contents

- [How to Contribute](#how-to-contribute)
- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Code of Conduct](#code-of-conduct)

## How to Contribute

### Reporting Issues

If you find a bug or have a feature request:

1. Check if the issue already exists in the [Issues](https://github.com/topxeq/xxsql/issues) section
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce (for bugs)
   - Expected vs actual behavior
   - Environment details (OS, Go version, etc.)

### Submitting Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Run tests: `go test ./...`
5. Commit with clear messages
6. Push to your fork
7. Create a Pull Request

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/xxsql.git
cd xxsql

# Install dependencies
go mod download

# Build the project
go build ./...

# Run all tests
go test ./...

# Run tests with coverage
go test -cover ./...

# Run integration tests
go test -v ./tests/integration/...
```

## Code Style

- Follow standard Go conventions
- Run `go fmt` before committing
- Add tests for new functionality
- Update documentation if needed
- Use meaningful variable and function names
- Add comments for complex logic

### Commit Messages

Use conventional commit format:

```
<type>(<scope>): <description>

[optional body]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding/updating tests
- `refactor`: Code refactoring
- `chore`: Maintenance tasks

## Testing

XxSql maintains high test coverage (87.5% average). Please ensure:

1. **Unit tests** for new functions/methods
2. **Integration tests** for multi-component features
3. **Edge cases** are covered
4. **Error paths** are tested

Run specific test categories:

```bash
# Unit tests only
go test -short ./...

# All tests including integration
go test ./...

# With race detection
go test -race ./...

# Benchmark tests
go test -bench=. ./...
```

See [docs/TESTING.md](docs/TESTING.md) for detailed testing guidelines.

## Pull Request Process

1. Ensure all tests pass
2. Update documentation for user-facing changes
3. Add entry to CHANGELOG if applicable
4. Request review from maintainers
5. Address review feedback

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow
- Welcome newcomers

## Questions?

Feel free to open an issue for any questions about contributing.
