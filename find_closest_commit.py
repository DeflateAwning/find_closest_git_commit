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


def compare_dirs(dir1: Path, dir2: Path) -> tuple[int, int, int, int]:
    hashes1 = collect_file_hashes(dir1)
    hashes2 = collect_file_hashes(dir2)

    common_files = set(hashes1.keys()) & set(hashes2.keys())
    matches = sum(1 for f in common_files if hashes1[f] == hashes2[f])
    mismatches = len(common_files) - matches
    file_in_one_side = len(set(hashes1.keys()) ^ set(hashes2.keys()))
    total_matched_hashes = set(hashes1.values()) & set(hashes2.values())
    return matches, mismatches, file_in_one_side, len(total_matched_hashes)


def get_all_commits(repo: git.Repo) -> list[git.Commit]:
    return list(repo.iter_commits("--all"))


def find_most_similar_commit(
    git_repo_path: Path,
    non_git_folder_path: Path,
    *,
    jsonl_output_path: Path | None = None,
) -> tuple[str | None, int]:
    assert (git_repo_path / ".git").exists(), (
        "The online repo must be a valid git repository."
    )
    assert non_git_folder_path.exists(), "The offline repo must exist."

    repo = git.Repo(git_repo_path)

    # Store the current checkout so we can set the repo back to that at the end.
    current_checkout = repo.head.commit.name_rev
    logger.info(f"Current checkout: {current_checkout}")

    commits = get_all_commits(repo)
    logger.info(f"Loaded commits: {len(commits):,}")

    best_score = -1_000_000_000
    best_commit = None

    with tempfile.TemporaryDirectory() as offline_temp:
        shutil.copytree(non_git_folder_path, offline_temp, dirs_exist_ok=True)

        for commit_number, commit in enumerate(commits, start=1):
            commit_sha = commit.hexsha
            commit_time: str = commit.committed_datetime.isoformat()

            repo.git.checkout(commit_sha)
            matches, mismatches, one_sided_files, total_matched_hashes = compare_dirs(
                git_repo_path, Path(offline_temp)
            )
            score = matches - mismatches - one_sided_files

            data_row = {
                "commit_number": commit_number,
                "commit_hash": commit_sha,
                "datetime": commit_time,
                "matches": matches,
                "mismatches": mismatches,
                "one_sided_files": one_sided_files,
                "total_matched_hashes": total_matched_hashes,
                "score": score,
            }

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

    try:
        repo.git.checkout(current_checkout)
    except Exception as e:
        logger.warning(f"Failed to checkout {current_checkout}: {e}")

    return best_commit, best_score


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
        "--jsonl-output", "--jsonl",
        help="Path to output JSONL file",
        default=None,
        dest="jsonl_output_path",
    )
    args = parser.parse_args()

    commit, score = find_most_similar_commit(
        git_repo_path=Path(args.git_repo_path),
        non_git_folder_path=Path(args.non_git_folder_path),
        jsonl_output_path=Path(args.jsonl_output_path)
        if args.jsonl_output_path
        else None,
    )
    if commit:
        logger.success(f"Most similar commit: {commit} with score {score}")
    else:
        logger.error("No matching commit found.")


if __name__ == "__main__":
    main()
