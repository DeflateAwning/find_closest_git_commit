use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use env_logger::Env;
use git2::{Oid, Repository, Time};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(
    name = "commit-similarity-search",
    about = "Find the commit most similar to an offline repo snapshot."
)]
struct Args {
    /// Path to the tracked Git repository
    #[arg(long = "git")]
    git_repo_path: PathBuf,

    /// Path to the untracked/modified copy (doesn't require/use .git folder)
    #[arg(long = "non-git")]
    non_git_folder_path: PathBuf,

    /// Path to output JSONL file
    #[arg(long = "jsonl-output")] // TODO: Add - long_alias = "jsonl")]
    jsonl_output_path: Option<PathBuf>,

    /// Latest-in-time commit hash to start searching from
    #[arg(long = "latest-commit")]
    latest_commit: Option<String>,

    /// Latest-in-time commit date (ISO 8601) to start searching from
    #[arg(long = "latest-date")]
    latest_date: Option<String>,

    /// Path to a text file with files expected to match in closest commit (one per line)
    #[arg(long = "unchanged-files-hint-list")]
    unchanged_files_hint_list_path: Option<PathBuf>,
}

#[derive(Serialize)]
struct DataRow<'a> {
    commit_number: usize,
    commit_hash: &'a str,
    datetime: String,
    score: i64,
    in_master_lineage: bool,
    matches: usize,
    mismatches: usize,
    one_sided_files: usize,
    total_matched_hashes: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    matched_hint_files: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    best: Option<&'a str>,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("Starting commit similarity search...");

    let args = Args::parse();

    if !args.git_repo_path.join(".git").exists() {
        bail!("The online repo must be a valid git repository.");
    }
    if !args.non_git_folder_path.exists() {
        bail!("The offline repo must exist.");
    }

    let repo = Repository::open(&args.git_repo_path).context("open repo")?;
    log::info!(
        "Opened git repo at {}",
        args.git_repo_path.to_string_lossy()
    );
    let all_commits = get_all_commits(&repo).context("walk commits")?;
    log::info!("Found {} total commits in the repo.", all_commits.len());

    let commits = get_filtered_commits(
        &repo,
        &all_commits,
        args.latest_commit.as_deref(),
        args.latest_date.as_deref(),
    )?;
    log::info!("Considering {} commits after filtering.", commits.len());

    let hint_list = match &args.unchanged_files_hint_list_path {
        Some(p) => {
            let f = File::open(p).context("open hint list")?;
            let r = BufReader::new(f);
            let mut v = Vec::new();
            for line in r.lines() {
                let l = line?;
                if !l.trim().is_empty() {
                    // Normalize to POSIX-style like the Python code does via as_posix()
                    v.push(l.replace('\\', "/"));
                }
            }
            Some(v)
        }
        None => None,
    };

    let (best_commit, best_score) = find_most_similar_commit(
        &repo,
        &args.git_repo_path,
        &args.non_git_folder_path,
        args.jsonl_output_path.as_deref(),
        &commits,
        hint_list.as_deref(),
    )?;

    match best_commit {
        Some(sha) => {
            log::info!("Best commit: {sha} with score {best_score}");
        }
        None => {
            log::warn!("No best commit found.");
        }
    }

    Ok(())
}

/// Calculate SHA256 hash of a file (read in 1 MiB chunks).
fn file_hash(path: &Path) -> Result<String> {
    let mut h = Sha256::new();
    let mut f = File::open(path)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}

/// Collect file hashes for a directory tree, skipping anything with ".git" in its path components.
fn collect_file_hashes(base_dir: &Path) -> Result<BTreeMap<String, String>> {
    let mut hashes = BTreeMap::new();
    for entry in WalkDir::new(base_dir)
        .into_iter()
        .filter_entry(|e| !e.file_name().to_string_lossy().contains(".git"))
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if path.components().any(|c| c.as_os_str() == ".git") {
                continue;
            }
            let rel = pathdiff::diff_paths(path, base_dir).unwrap_or_else(|| path.to_path_buf());
            let relp = rel.to_string_lossy().replace('\\', "/");
            let h = file_hash(path)?;
            hashes.insert(relp, h);
        }
    }
    Ok(hashes)
}

/// Compare two directories represented by file-hash maps.
fn compare_dirs(
    hashes_git: &BTreeMap<String, String>,
    hashes_offline: &BTreeMap<String, String>,
) -> (usize, usize, usize, usize) {
    let git_keys: BTreeSet<_> = hashes_git.keys().collect();
    let off_keys: BTreeSet<_> = hashes_offline.keys().collect();
    let common: BTreeSet<_> = git_keys.intersection(&off_keys).collect();

    let mut matches = 0usize;
    for f in &common {
        if hashes_git.get(**f) == hashes_offline.get(**f) {
            matches += 1;
        }
    }
    let mismatches = common.len() - matches;
    let one_sided_files = git_keys.symmetric_difference(&off_keys).count();

    let git_vals: BTreeSet<_> = hashes_git.values().collect();
    let off_vals: BTreeSet<_> = hashes_offline.values().collect();
    let total_matched_hashes = git_vals.intersection(&off_vals).count();

    (matches, mismatches, one_sided_files, total_matched_hashes)
}

/// Traverse all commits reachable from any ref (like `git rev-list --all`), newest-first.
fn get_all_commits(repo: &Repository) -> Result<Vec<Oid>> {
    let mut walk = repo.revwalk()?;
    // Push *all* refs
    for r in repo.references()? {
        let r = r?;
        if let Some(oid) = r.target() {
            walk.push(oid)?;
        }
    }
    walk.set_sorting(git2::Sort::TIME | git2::Sort::TOPOLOGICAL)?;
    let mut commits = Vec::new();
    for oid in walk {
        commits.push(oid?);
    }
    Ok(commits)
}

/// Filter the commit list by latest_commit or latest_date (ISO 8601), newest-first.
fn get_filtered_commits(
    repo: &Repository,
    commits_newest_first: &[Oid],
    latest_commit: Option<&str>,
    latest_date_iso: Option<&str>,
) -> Result<Vec<Oid>> {
    if latest_commit.is_some() && latest_date_iso.is_some() {
        bail!("Cannot specify both latest_commit and latest_date.");
    }

    if let Some(commit_prefix) = latest_commit {
        for (i, &oid) in commits_newest_first.iter().enumerate() {
            if oid.to_string().starts_with(commit_prefix) {
                return Ok(commits_newest_first[i..].to_vec());
            }
        }
        bail!("Starting commit {commit_prefix} not found in repo.");
    }

    if let Some(date_str) = latest_date_iso {
        // Parse ISO-8601 (assume naive -> UTC). chrono can parse many forms via DateTime::parse_from_rfc3339
        // but we’ll try RFC3339 first, then fall back to flexible parsing if needed.
        let ts =
            parse_iso_to_unix(date_str).with_context(|| format!("parse latest_date {date_str}"))?;
        for (i, &oid) in commits_newest_first.iter().enumerate() {
            let c = repo.find_commit(oid)?;
            let c_ts = c.time().seconds();
            if c_ts <= ts {
                return Ok(commits_newest_first[i..].to_vec());
            }
        }
        bail!("Starting date {date_str} not found in repo.");
    }

    Ok(commits_newest_first.to_vec())
}

fn parse_iso_to_unix(s: &str) -> Result<i64> {
    use chrono::TimeZone;

    // Try yyyy-mm-dd first (assume midnight UTC).
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = chrono::Utc.from_utc_datetime(
            &d.and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow!("Invalid time"))?,
        );
        return Ok(dt.timestamp());
    }

    // Try RFC3339.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }

    // Try a looser parser (e.g., "2024-01-01T00:00:00").
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        let dt = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
        return Ok(dt.timestamp());
    }
    bail!("Unrecognized ISO-8601 format: {s}")
}

/// Resolve a “master-like” ref, specific to the repository.
fn resolve_master_like_ref(repo: &Repository) -> Result<String> {
    let candidates = [
        "refs/heads/master",
        "refs/heads/main",
        "refs/remotes/origin/master",
        "refs/remotes/origin/main",
    ];
    for name in candidates {
        if repo.refname_to_id(name).is_ok() {
            return Ok(name.to_string());
        }
    }
    Err(anyhow!("No master-like ref found"))
}

/// True if `commit` is an ancestor of `master_ref` (master/main line).
fn is_commit_in_master_lineage(repo: &Repository, commit: Oid, master_ref: &str) -> Result<bool> {
    let head_oid = repo.refname_to_id(master_ref)?;
    // “A is ancestor of B” ≈ graph_descendant_of(B, A)
    Ok(repo.graph_descendant_of(head_oid, commit).unwrap_or(false))
}

fn check_out_commit(repo: &Repository, commit_oid: Oid) -> Result<()> {
    let commit = repo.find_commit(commit_oid)?;
    let tree = commit.tree()?;
    repo.checkout_tree(&tree.as_object(), None)?;
    repo.set_head_detached(commit_oid)?;
    Ok(())
}

fn find_most_similar_commit(
    repo: &Repository,
    git_repo_path: &Path,
    non_git_folder_path: &Path,
    jsonl_output_path: Option<&Path>,
    commits: &[Oid],
    unchanged_files_hint_list: Option<&[String]>,
) -> Result<(Option<String>, i64)> {
    let master_ref = resolve_master_like_ref(repo)?;
    log::info!(
        "Using \"{}\" as the master-like lineage root.",
        master_ref
    );

    // Prepare offline hashes.
    let hashes_offline = collect_file_hashes(&non_git_folder_path)?;

    let mut best_score: i64 = i64::MIN;
    let mut best_commit: Option<String> = None;

    let mut jsonl_file = match jsonl_output_path {
        Some(p) => Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)?,
        ),
        None => None,
    };

    for (commit_num, &commit_oid) in commits.iter().enumerate() {
        let c = repo.find_commit(commit_oid)?;
        let commit_hash = c.id().to_string();
        let commit_time = time_to_iso8601(c.time());

        // Check out the commit (detached HEAD).
        check_out_commit(repo, commit_oid)?;

        let hashes_git = collect_file_hashes(git_repo_path)?;
        let (matches, mismatches, one_sided_files, total_matched_hashes) =
            compare_dirs(&hashes_git, &hashes_offline);

        let score = (matches as i64) - (mismatches as i64) - (one_sided_files as i64);

        let in_master_lineage = is_commit_in_master_lineage(repo, commit_oid, &master_ref)?;

        let matched_hint_files = unchanged_files_hint_list.map(|hint_list| {
            hint_list
                .iter()
                .filter(|f| {
                    let lhs_default_str_binding = "GIT_HASH_NOT_EXIST".to_string();
                    let lhs = hashes_git.get(*f).unwrap_or(&lhs_default_str_binding);
                    let rhs_default_str_binding = "OFFLINE_HASH_NOT_EXIST".to_string();
                    let rhs = hashes_offline.get(*f).unwrap_or(&rhs_default_str_binding);
                    lhs == rhs
                })
                .count()
        });

        let mut best = None;
        if score > best_score {
            best_score = score;
            best_commit = Some(commit_hash.clone());
            best = Some("NEW BEST 🟢");
        }

        let row = DataRow {
            commit_number: commit_num + 1,
            commit_hash: &commit_hash,
            datetime: commit_time,
            score,
            in_master_lineage,
            matches,
            mismatches,
            one_sided_files,
            total_matched_hashes,
            matched_hint_files,
            best,
        };

        let line = serde_json::to_string(&row)?;
        log::info!("{}", line);
        if let Some(f) = jsonl_file.as_mut() {
            use std::io::Write;
            writeln!(f, "{line}")?;
        }
    }

    Ok((best_commit, best_score))
}

fn time_to_iso8601(t: Time) -> String {
    use chrono::TimeZone;

    let utc_offset = chrono::FixedOffset::east_opt(0).unwrap();

    // libgit2 stores time as seconds + offset (minutes). Convert to chrono.
    let secs = t.seconds();
    let offset_min = t.offset_minutes();
    let offset = chrono::FixedOffset::east_opt(offset_min * 60).unwrap_or(utc_offset);
    offset
        .timestamp_opt(secs, 0)
        .single()
        .unwrap_or_else(|| {
            // Fallback to UTC if conversion fails
            utc_offset.timestamp_opt(secs, 0).unwrap()
        })
        .to_rfc3339()
}
