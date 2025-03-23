#!/bin/bash
# $ cargo version --verbose
# cargo 1.87.0-nightly (6cf826701 2025-03-14)
# release: 1.87.0-nightly
# commit-hash: 6cf8267012570f63d6b86e85a2ae5627de52df9e
# commit-date: 2025-03-14
# host: x86_64-unknown-linux-gnu
# libgit2: 1.9.0 (sys:0.20.0 vendored)
# libcurl: 8.12.1-DEV (sys:0.4.80+curl-8.12.1 vendored ssl:OpenSSL/3.4.1)
# ssl: OpenSSL 3.4.1 11 Feb 2025
# os: Ubuntu 24.4.0 (noble) [64-bit]
LOOM_CHECKPOINT_INTERVAL=1 LOOM_CHECKPOINT_FILE=my_test.json cargo test --features check-loom,loom/checkpoint --test arc --release -- correctness::count_sync --nocapture --test-threads 1