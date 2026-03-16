# Contributing to XxSql

Thank you for your interest in contributing to XxSql! This document provides guidelines and instructions for contributing.

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

### Code Style

- Follow standard Go conventions
- Run `go fmt` before committing
- Add tests for new functionality
- Update documentation if needed

### Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/xxsql.git
cd xxsql

# Install dependencies
go mod download

# Run tests
go test ./...

# Build
go build ./...
```

## Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

## Questions?

Feel free to open an issue for any questions about contributing.
