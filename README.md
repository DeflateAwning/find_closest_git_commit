# find_closest_git_commit
Quick tool to find which Git commit is closest to an arbitrary non-git folder

## Info

* After prototyping with Python, built using Rust
* Writes a JSONL file, which can be analyzed to determine the closest commit.

## Getting Started

```bash
cargo run --release -- --help

cargo run --release -- --git ./some-clean-git-repo --non-git ./some-folder-that-was-copied-from-the-git-repo --jsonl-output ./commit_compare.jsonl
```
