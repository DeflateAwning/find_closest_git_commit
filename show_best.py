import polars as pl
from pathlib import Path

print("Higher score means closer match.")


df = pl.read_ndjson("commit_scores.jsonl")

print(f"Loaded data: {df}")

df = df.sort("score", maintain_order=True, descending=True)

print(f"Sorted by best match on top: {df}")
