# Checks this crate. Run from the repository root:
#
#   make check   formatting, lints, the unit suites, the layout and surface
#                tests, the documentation snippets and the docs build
#   make live    the acceptance suites against a real Celeris stack, from .env
#
# Releasing is `cargo publish`; nothing here does it.

.PHONY: check live

check:
	cargo fmt --check
	cargo clippy --all-targets --locked -- -D warnings
	cargo clippy --test live --locked -- -D warnings
	cargo test --locked
	RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked

live:
	cargo test --locked --test live -- --test-threads=1
