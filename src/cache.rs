use gix::bstr::{BStr, BString};
use gix::ObjectId;

#[derive(Clone)]
pub(crate) struct CachedCommit {
  pub(crate) changed_paths: Vec<u32>,
}

pub(crate) trait Cache: Send + Sync {
  fn cache_path(&self, path: &BStr) -> anyhow::Result<u32>;
  fn cache_rename(&self, old_path: &BStr, new_path: u32) -> anyhow::Result<()>;
  fn resolve_path(&self, path_id: u32) -> anyhow::Result<Option<BString>>;
  fn cached_commit(&self, id: &ObjectId) -> anyhow::Result<Option<CachedCommit>>;
  fn update_cached_commit(&self, id: &ObjectId, commit: CachedCommit) -> anyhow::Result<()>;
  fn is_commit_cached(&self, id: &ObjectId) -> anyhow::Result<bool>;
}
