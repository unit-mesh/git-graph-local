use dashmap::DashMap;
use gix::bstr::{BStr, BString, ByteSlice};
use gix::object::tree::diff::{Action, Change};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;
use std::{time};
use gix::ObjectId;

use crate::blame;

#[derive(Debug)]
pub(crate) struct Candidate {
  pub(crate) path: Option<BString>,
  pub(crate) locations: Vec<Range<u32>>,
  pub(crate) touched_lines: u32,
  pub(crate) weight: f32,
  pub(crate) commit: ObjectId,
}

struct InnerGraph {
  repo: gix::ThreadSafeRepository,
  disk_cache: Box<dyn crate::cache::Cache>,
  blame_cache: DashMap<BString, Arc<blame::LazyBlame>>,
}

impl InnerGraph {
  pub async fn load_blame(self: &Arc<Self>, revision: Option<ObjectId>, filepath: &BStr, recursive: bool) -> anyhow::Result<Arc<blame::LazyBlame>> {
    match self.blame_cache.entry(filepath.to_owned()) {
      dashmap::Entry::Occupied(e) => Ok(e.get().clone()),
      dashmap::Entry::Vacant(e) => {
        let blame = Arc::new(blame::LazyBlame::new(filepath.to_owned()));
        let blame = e.insert(blame);

        let blame_owned = blame.clone();
        let repo_path_owned = self.repo.work_dir().unwrap().to_owned();
        let filepath_owned = filepath.to_owned();
        let inner = self.clone();

        tokio::spawn(async move {
          let mut seen = HashSet::new();
          let blame_owned_inner = blame_owned.clone();
          let _ = blame::native_git_blame::parse(&repo_path_owned, revision, filepath_owned.as_bstr(), move |chunk| {
            let entry = blame::BlameEntry {
              range_in_blamed_file: chunk.line_final..chunk.line_final + chunk.num_lines,
              range_in_original_file: chunk.line_original..chunk.line_original + chunk.num_lines,
              commit_id: chunk.sha,
            };

            blame_owned_inner.add_entry(entry);
            if recursive && seen.insert(chunk.sha) {
              let inner = inner.clone();
              rayon::spawn(move || {
                inner.load_cached_commit(&chunk.sha).unwrap();
              });
            }
          }).await;
          blame_owned.mark_as_finished();
        });

        Ok(blame.clone())
      }
    }
  }


  async fn find_related_locations(
    self: &Arc<Self>,
    revision: Option<ObjectId>,
    path: &BStr,
    interesting_shas: &HashSet<ObjectId>,
  ) -> Option<Vec<Range<u32>>> {
    let blame = match self.load_blame(revision, path.as_bstr(), false).await {
      Ok(blame) => blame,
      Err(_) => return None,
    };

    if !blame.is_ready() {
      let _ = tokio::time::timeout(time::Duration::from_millis(250), blame.wait_for_ready()).await;
    }

    let related_locations: Vec<Range<u32>> = blame.lines().into_iter().filter_map(|chunk| {
      if interesting_shas.contains(&chunk.commit_id) {
        Some(chunk.range_in_blamed_file)
      } else {
        None
      }
    }).collect();

    if related_locations.is_empty() {
      None
    } else {
      Some(related_locations)
    }
  }

  fn load_cached_commit(self: &Arc<Self>, commit_sha: &ObjectId) -> anyhow::Result<()> {
    if self.disk_cache.is_commit_cached(commit_sha)? {
      return Ok(());
    }

    let path_cache = &self.disk_cache;
    let repo = self.repo.to_thread_local();
    let commit = repo.find_commit(*commit_sha)?;
    let tree = commit.tree()?;
    let ancestors = commit.parent_ids().next().unwrap();

    let parent_tree = repo.find_commit(ancestors)?.tree()?;
    let mut changed = Vec::new();

    let mut diff = parent_tree.changes()?;
    diff.for_each_to_obtain_tree(&tree, |change| -> anyhow::Result<Action> {
      match change {
        Change::Addition {
          entry_mode,
          location,
          ..
        } => {
          if entry_mode.is_blob_or_symlink() {
            changed.push(path_cache.cache_path(location)?);
          }
        }
        Change::Deletion { .. } => {
          // not interesting
        }
        Change::Modification {
          entry_mode,
          location,
          ..
        } => {
          if entry_mode.is_blob_or_symlink() {
            changed.push(path_cache.cache_path(location)?);
          }
        }
        Change::Rewrite {
          entry_mode,
          location,
          ..
        } => {
          if entry_mode.is_blob_or_symlink() {
            changed.push(path_cache.cache_path(location)?);
          }
        }
      }
      Ok(Action::Continue)
    })?;

    changed.sort();
    self.disk_cache.update_cached_commit(
      commit_sha,
      crate::cache::CachedCommit {
        changed_paths: changed,
      },
    )?;

    Ok(())
  }
}

pub(crate) struct LocalGitGraph {
  inner: Arc<InnerGraph>,
}

impl Clone for LocalGitGraph {
  fn clone(&self) -> Self {
    LocalGitGraph {
      inner: self.inner.clone(),
    }
  }
}

impl LocalGitGraph {
  pub(crate) fn new(repo: &str) -> anyhow::Result<Self> {
    let mut repo = gix::open(repo)?;
    repo.object_cache_size(Some(16 * 1024 * 1024));

    let inner = Arc::new(InnerGraph {
      repo: repo.into_sync(),
      disk_cache: Box::new(crate::sqlite::SqliteCache::new()?),
      blame_cache: DashMap::new(),
    });

    Ok(LocalGitGraph { inner })
  }

  pub(crate) async fn related_files(
    &self,
    blame: &Arc<blame::LazyBlame>,
    lineno: usize,
  ) -> anyhow::Result<Vec<Candidate>> {
    let blame_lines = blame.lines();

    let search = blame_lines
      .binary_search_by(|x| {
        return x.range_in_blamed_file.start.cmp(&(lineno as u32));
      })
      .unwrap_or_else(|x| x) as isize;

    const BLAME_CHUNK_RANGE: isize = 6;
    let inner = self.inner.clone();
    let mut candidate_files: HashMap<u32, Candidate> = HashMap::new();

    let start_ofs = max(0, search - BLAME_CHUNK_RANGE / 2);
    let end_ofs = min(search + BLAME_CHUNK_RANGE / 2, blame_lines.len() as isize);
    let mut interesting_shas: HashSet<ObjectId> = HashSet::new();

    for rng in start_ofs..end_ofs {
      let blame_root = &blame_lines[rng as usize];
      let dist_from_search = (rng - search).abs() as f32;

      if let Some(commit) = inner.disk_cache.cached_commit(&blame_root.commit_id)? {
        interesting_shas.insert(blame_root.commit_id);

        for path_id in commit.changed_paths.iter() {
          let entry = candidate_files.entry(*path_id).or_insert_with(|| {
            Candidate{
              path: None,
              locations: vec![],
              touched_lines: 0,
              weight: 0.0,
              commit: blame_root.commit_id,
            }
          });

          entry.weight += 2.0f32 - dist_from_search * 0.2;
        }
      }
    }

    if candidate_files.is_empty() {
      return Ok(Vec::new());
    }

    let mut candidate_files: Vec<_> = candidate_files.into_iter().collect();
    candidate_files.sort_by(|a, b| a.0.cmp(&b.0));
    candidate_files.sort_by(|a, b| b.1.weight.partial_cmp(&a.1.weight).unwrap());
    candidate_files.truncate(20);

    let mut joinset = tokio::task::JoinSet::new();
    let interesting_shas = Arc::new(interesting_shas);

    for (index, (path_id, w)) in candidate_files.iter_mut().enumerate() {
      if let Some(path) = inner.disk_cache.resolve_path(*path_id)? {
        w.path = Some(path.clone());

        let inner = inner.clone();
        let interesting_shas = interesting_shas.clone();
        let commit_sha = w.commit.clone();
        joinset.spawn(async move {
          (
            index,
            inner
              .find_related_locations(Some(commit_sha), path.as_ref(), &interesting_shas)
              .await,
          )
        });
      }
    }

    while let Some(res) = joinset.join_next().await {
      if let (index, Some(related_locs)) = res? {
        let w = &mut candidate_files[index].1;
        w.locations = related_locs;
        w.touched_lines = w.locations.iter().map(|loc| loc.end - loc.start).sum();
        assert_ne!(w.touched_lines, 0);
      }
    }

    let largest_touched_file = candidate_files
      .iter()
      .max_by(|a, b| a.1.touched_lines.cmp(&b.1.touched_lines))
      .map(|x| x.1.touched_lines)
      .unwrap() as f32;

    for (_, candidate) in candidate_files.iter_mut() {
      candidate.weight *= candidate.touched_lines as f32 / largest_touched_file;
    }

    candidate_files.sort_by(|a, b| b.1.weight.partial_cmp(&a.1.weight).unwrap());
    Ok(
      candidate_files
        .into_iter()
        .filter_map(|cand| {
          if cand.1.touched_lines > 0 {
            Some(cand.1)
          } else {
            None
          }
        })
        .collect(),
    )
  }

  pub async fn blame(&self, filepath: &BStr) -> anyhow::Result<Arc<blame::LazyBlame>> {
    self.inner.load_blame(None, filepath, true).await
  }
}

#[cfg(test)]
mod test {
  use std::time;

  use super::*;

  #[tokio::test]
  async fn test_basic() -> anyhow::Result<()> {
    // use the local everysphere monorepo for testing
    let gg = LocalGitGraph::new("../../../../../")?;

    let blame = gg
      .blame("vscode/src/vs/editor/browser/coreCommands.ts".into())
      .await?;

    tokio::time::sleep(time::Duration::from_secs(1)).await;

    for _ in 1..10 {
      let now = time::Instant::now();
      let related_files = gg.related_files(&blame, 43).await?;
      assert!(related_files.len() > 0);
      println!("Related files: {:?}", &related_files);
      println!("Took {}ms", now.elapsed().as_millis());
    }

    Ok(())
  }
}
