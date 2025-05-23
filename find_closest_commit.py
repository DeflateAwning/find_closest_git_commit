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


def compare_dirs(dir1: Path, dir2: Path) -> tuple[int, int, int]:
    hashes1 = collect_file_hashes(dir1)
    hashes2 = collect_file_hashes(dir2)

    common_files = set(hashes1.keys()) & set(hashes2.keys())
    matches = sum(1 for f in common_files if hashes1[f] == hashes2[f])
    mismatches = len(common_files) - matches
    file_in_one_side = len(set(hashes1.keys()) ^ set(hashes2.keys()))
    return matches, mismatches, file_in_one_side


def get_all_commits(repo: git.Repo) -> list[git.Commit]:
    return list(repo.iter_commits("--all"))


def find_most_similar_commit(
    git_repo_path: Path, non_git_folder_path: Path
) -> tuple[str | None, int]:
    assert (git_repo_path / ".git").exists(), (
        "The online repo must be a valid git repository."
    )
    assert non_git_folder_path.exists(), "The offline repo must exist."

    repo = git.Repo(git_repo_path)
    commits = get_all_commits(repo)
    logger.info(f"Loaded commits: {len(commits):,}")

    best_score = -1
    best_commit = None

    with tempfile.TemporaryDirectory() as offline_temp:
        shutil.copytree(non_git_folder_path, offline_temp, dirs_exist_ok=True)

        for commit_number, commit in enumerate(commits, start=1):
            commit_sha = commit.hexsha
            commit_time: str = commit.committed_datetime.isoformat()

            repo.git.checkout(commit_sha)
            matches, mismatches, one_sided_files = compare_dirs(
                git_repo_path, Path(offline_temp)
            )
            score = matches - mismatches - one_sided_files
            if score > best_score:
                best_score = score
                best_commit = commit_sha

            data_row = {
                "commit_number": commit_number,
                "commit_hash": commit_sha,
                "datetime": commit_time,
                "matches": matches,
                "mismatches": mismatches,
                "one_sided_files": one_sided_files,
                "score": score,
            }
            logger.info(f"{data_row}")
            with open("commit_scores.jsonl", "a") as f:
                f.write(json.dumps(data_row) + "\n")

    repo.git.checkout("main")  # Or "master" or saved HEAD
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
    args = parser.parse_args()

    commit, score = find_most_similar_commit(
        git_repo_path=Path(args.git_repo_path),
        non_git_folder_path=Path(args.non_git_folder_path),
    )
    if commit:
        logger.success(f"Most similar commit: {commit} with score {score}")
    else:
        logger.error("No matching commit found.")


if __name__ == "__main__":
    main()
