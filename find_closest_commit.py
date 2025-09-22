# /// script
# dependencies = [
#   "loguru",
#   "gitpython",
# ]
# ///

import tempfile
import shutil
import hashlib
import argparse
from pathlib import Path
import json
from typing import Literal

from loguru import logger
import git


def file_hash(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def collect_file_hashes(base_dir: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for path in base_dir.rglob("*"):
        if path.is_file() and ".git" not in path.parts:
            rel_path = path.relative_to(base_dir).as_posix()
            hashes[rel_path] = file_hash(path)
    return hashes


def compare_dirs(
    hashes_git: dict[str, str], hashes_offline: dict[str, str]
) -> dict[
    Literal["matches", "mismatches", "one_sided_files", "total_matched_hashes"], int
]:
    common_files = set(hashes_git.keys()) & set(hashes_offline.keys())
    matches = sum(1 for f in common_files if hashes_git[f] == hashes_offline[f])
    mismatches = len(common_files) - matches
    one_sided_files = len(set(hashes_git.keys()) ^ set(hashes_offline.keys()))
    total_matched_hashes = set(hashes_git.values()) & set(hashes_offline.values())

    return {
        "matches": matches,
        "mismatches": mismatches,
        "one_sided_files": one_sided_files,
        "total_matched_hashes": len(total_matched_hashes),
    }


def get_all_commits(repo: git.Repo) -> list[git.Commit]:
    commits = list(repo.iter_commits("--all"))
    return commits


def get_filtered_commits(
    repo: git.Repo, latest_commit: str | None, latest_date: str | None
) -> list[git.Commit]:
    if latest_commit and latest_date:
        raise ValueError("Cannot specify both latest_commit and latest_date.")

    commits = get_all_commits(repo)

    if latest_commit:
        for i, commit in enumerate(commits):
            if commit.hexsha.startswith(latest_commit):
                return commits[i:]
        raise ValueError(f"Starting commit {latest_commit} not found in repo.")

    if latest_date:
        for i, commit in enumerate(commits):
            if commit.committed_datetime.isoformat() <= latest_date:
                return commits[i:]
        raise ValueError(f"Starting date {latest_date} not found in repo.")

    return commits


def find_most_similar_commit(
    git_repo_path: Path,
    non_git_folder_path: Path,
    *,
    jsonl_output_path: Path | None = None,
    commits: list[git.Commit],
    unchanged_files_hint_list: list[str] | None = None,
) -> tuple[str | None, int]:
    repo = git.Repo(git_repo_path)

    # Log the current checkout, just for info.
    logger.info(f"Current checkout (for info only): {repo.head.commit.name_rev}")

    best_score = -1_000_000_000
    best_commit = None

    with tempfile.TemporaryDirectory() as offline_temp_str:
        offline_temp_path = Path(offline_temp_str)
        shutil.copytree(non_git_folder_path, offline_temp_str, dirs_exist_ok=True)

        hashes_offline = collect_file_hashes(offline_temp_path)

        for commit_number, commit in enumerate(commits, start=1):
            commit_sha = commit.hexsha
            commit_time: str = commit.committed_datetime.isoformat()

            repo.git.checkout(commit_sha)

            hashes_git = collect_file_hashes(git_repo_path)
            comparison = compare_dirs(hashes_git, hashes_offline)

            # Calculate score.
            score = (
                comparison["matches"]
                - comparison["mismatches"]
                - comparison["one_sided_files"]
            )

            data_row = {
                "commit_number": commit_number,
                "commit_hash": commit_sha,
                "datetime": commit_time,
                "score": score,
            } | comparison

            if unchanged_files_hint_list:
                data_row["matched_hint_files"] = sum(
                    1
                    for f in unchanged_files_hint_list
                    if hashes_git.get(f, "GIT_HASH_NOT_EXIST") == hashes_offline.get(f, "OFFLINE_HASH_NOT_EXIST")
                )

            if score > best_score:
                best_score = score
                best_commit = commit_sha

                # Add indicator that it's a "new best".
                data_row["best"] = "NEW BEST 🟢"

            data_row_json: str = json.dumps(data_row, ensure_ascii=False)
            logger.info(data_row_json)
            if jsonl_output_path:
                with open(jsonl_output_path, "a") as f:
                    f.write(data_row_json + "\n")

    return best_commit, best_score


def execute_search(
    git_repo_path: Path,
    non_git_folder_path: Path,
    *,
    jsonl_output_path: Path | None = None,
    latest_commit: str | None = None,
    latest_date: str | None = None,
    unchanged_files_hint_list_path: Path | None = None,
):
    if not (git_repo_path / ".git").exists():
        msg = "The online repo must be a valid git repository."
        raise ValueError(msg)
    if not non_git_folder_path.exists():
        msg = "The offline repo must exist."
        raise ValueError(msg)

    repo = git.Repo(git_repo_path)

    logger.info(f"Found {len(get_all_commits(repo))} total commits in the repo.")
    commits = get_filtered_commits(
        repo, latest_commit=latest_commit, latest_date=latest_date
    )
    logger.info(f"Considering {len(commits)} commits after filtering.")

    unchanged_files_hint_list: list[str] | None = None
    if unchanged_files_hint_list_path:
        unchanged_files_hint_list = (
            unchanged_files_hint_list_path.read_text().splitlines()
        )

    return find_most_similar_commit(
        git_repo_path=git_repo_path,
        non_git_folder_path=non_git_folder_path,
        jsonl_output_path=jsonl_output_path,
        commits=commits,
        unchanged_files_hint_list=unchanged_files_hint_list,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Find the commit most similar to an offline repo snapshot."
    )
    parser.add_argument(
        "--git",
        required=True,
        help="Path to the tracked Git repository",
        dest="git_repo_path",
    )
    parser.add_argument(
        "--non-git",
        required=True,
        help="Path to the untracked/modified copy (doesn't require/use .git folder)",
        dest="non_git_folder_path",
    )
    parser.add_argument(
        "--jsonl-output",
        "--jsonl",
        help="Path to output JSONL file",
        default=None,
        dest="jsonl_output_path",
    )
    parser.add_argument(
        "--latest-commit",
        help="The latest-in-time commit hash to start searching from (default: latest commit)",
        dest="latest_commit",
        default=None,
    )
    parser.add_argument(
        "--latest-date",
        help="The latest-in-time commit date (ISO 8601) to start searching from (default: latest commit)",
        dest="latest_date",
        default=None,
    )
    parser.add_argument(
        "--unchanged-files-hint-list",
        help="Path to a text file with a list of files (one per line) that are expected to match when we find the closest commit.",
        dest="unchanged_files_hint_list_path",
    )
    args = parser.parse_args()

    execute_search(
        git_repo_path=Path(args.git_repo_path),
        non_git_folder_path=Path(args.non_git_folder_path),
        jsonl_output_path=(
            Path(args.jsonl_output_path) if args.jsonl_output_path else None
        ),
        latest_commit=args.latest_commit,
        latest_date=args.latest_date,
        unchanged_files_hint_list_path=(
            Path(args.unchanged_files_hint_list_path)
            if args.unchanged_files_hint_list_path
            else None
        ),
    )


if __name__ == "__main__":
    main()
