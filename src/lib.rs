use std::sync::Arc;
use gix::bstr::{BString, ByteSlice};
use crate::blame::LazyBlame;

mod blame;
mod cache;
mod gitgraph;
mod sqlite;

#[macro_use]
extern crate napi_derive;

#[napi(object)]
pub struct Candidate {
  pub path: String,
  pub locations: Vec<u32>,
  pub weight: f64,
}

#[napi]
pub struct GitFile {
  graph: gitgraph::LocalGitGraph,
  blame: Arc<LazyBlame>,
}

#[napi]
impl GitFile {
  #[napi]
  pub async fn find_similar_files(&self, lineno: u32) -> napi::Result<Vec<Candidate>> {
    let related_files = self
      .graph
      .related_files(&self.blame, lineno as usize)
      .await
      .map_err(|e| napi::Error::from_reason(e.to_string()))?;

    Ok(
      related_files
        .into_iter()
        .map(|c| Candidate {
          path: c.path.as_ref().unwrap().to_string(),
          locations: c.locations.iter().map(|loc| loc.start).collect(),
          weight: c.weight as f64,
        })
        .collect(),
    )
  }
}

#[napi]
pub struct LocalGitGraph {
  inner: gitgraph::LocalGitGraph,
}

#[napi]
impl LocalGitGraph {
  #[napi(constructor)]
  pub fn new(repo: String) -> Self {
    LocalGitGraph {
      inner: gitgraph::LocalGitGraph::new(&repo).unwrap(),
    }
  }

  #[napi]
  pub async fn open_file(&self, path: String) -> napi::Result<GitFile> {
    let path: BString = path.into();
    let blame = self
      .inner
      .blame(path.as_bstr())
      .await
      .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    Ok(GitFile {
      graph: self.inner.clone(),
      blame,
    })
  }
}
