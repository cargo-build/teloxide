# Show available recipes
help:
    just --list

# Format code
fmt:
    cargo +nightly fmt --all

# Check formatting and run clippy
lint:
    cargo +nightly fmt --all --check || (echo "Run 'just fmt' to fix formatting!" && exit 1)
    cargo +stable clippy --all-targets --features full

# Run tests
test:
    cargo +stable test --features full

# Test documentation
docs:
    cargo +nightly docs

# Run all checks we do in CI (lint + test + docs)
ci:
    just lint
    just test
    just docs

# Same as 'just ci' but in clean environment
clean_ci:
    cargo clean
    just ci
