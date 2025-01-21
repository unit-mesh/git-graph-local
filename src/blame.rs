use gix::bstr::{BString};
use std::ops::Range;
use std::sync::Mutex;
use gix::ObjectId;

#[derive(Clone, Debug)]
pub struct BlameEntry {
  pub range_in_blamed_file: Range<u32>,
  pub range_in_original_file: Range<u32>,
  pub commit_id: ObjectId,
}

struct LazyBlameInner {
  blame: Vec<BlameEntry>,
  sorted: usize,
  ready: bool,
}

impl LazyBlameInner {
  fn new() -> Self {
    LazyBlameInner {
      blame: vec![],
      sorted: 0,
      ready: false,
    }
  }

  fn blame_lines(&mut self) -> &Vec<BlameEntry> {
    if self.sorted < self.blame.len() {
      self.blame.sort_by(|a, b| a.range_in_blamed_file.start.cmp(&b.range_in_blamed_file.start));
      self.sorted = self.blame.len();
    }
    &self.blame
  }
}

pub struct LazyBlame {
  pub file_path: BString,
  inner: Mutex<LazyBlameInner>,
  notify: tokio::sync::Notify,
}

impl LazyBlame {
  pub fn new(file_path: BString) -> Self {
    LazyBlame {
      file_path,
      inner: Mutex::new(LazyBlameInner::new()),
      notify: tokio::sync::Notify::new(),
    }
  }

  pub fn lines(&self) -> Vec<BlameEntry> {
    let mut inner = self.inner.lock().unwrap();
    inner.blame_lines().to_vec()
  }

  pub fn add_entry(&self, entry: BlameEntry) {
    let mut inner = self.inner.lock().unwrap();
    inner.blame.push(entry);
  }

  pub fn is_ready(&self) -> bool {
    let inner = self.inner.lock().unwrap();
    inner.ready
  }

  pub(crate) fn mark_as_finished(&self) {
    {
      let mut inner = self.inner.lock().unwrap();
      inner.ready = true;
    }

    self.notify.notify_waiters();
  }

  pub(crate) async fn wait_for_ready(&self) {
    loop {
      let future = self.notify.notified();
      if self.is_ready() {
        return;
      }
      future.await;
    }
  }
}

pub(crate) mod native_git_blame {
  use std::ffi::OsStr;
  use std::os::unix::ffi::OsStrExt;
  use std::process::Stdio;
  use anyhow::anyhow;
  use gix::bstr::{BStr, BString};
  use gix::hash::Kind;
  use gix::ObjectId;
  use tokio::io::AsyncBufReadExt;

  #[derive(Debug)]
  pub(crate) struct BlameChunk {
    pub(crate) sha: ObjectId,
    pub(crate) line_original: u32,
    pub(crate) line_final: u32,
    pub(crate) num_lines: u32,
    pub(crate) previous_filename: Option<BString>,
  }

  pub(crate) async fn parse<F: FnMut(BlameChunk)>(repo_path: &std::path::Path, revision: Option<ObjectId>, filepath: &BStr, mut lazy_blame: F) -> anyhow::Result<()> {
    let mut git_blame_cmd = tokio::process::Command::new("git");
    let mut child = git_blame_cmd
        .arg("-C")
        .arg(repo_path)
        .arg("blame")
        .arg("--incremental")
        .arg(revision.map_or_else(|| "HEAD".into(), |r| r.to_string()))
        .arg("--")
        .arg(OsStr::from_bytes(filepath))
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().unwrap();
    let mut reader = tokio::io::BufReader::new(stdout).lines();

    let (tx_status, rx_status) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
      let status = child.wait().await;
      if let Err(_) = tx_status.send(status) {}
    });

    let mut current_chunk: Option<BlameChunk> = None;

    while let Some(line) = reader.next_line().await? {
      if let Some(chunk) = current_chunk.as_mut() {
        if line.starts_with("previous ") {
          let previous_filename = line.split(' ').skip(1).next().unwrap();
          chunk.previous_filename = Some(previous_filename.into());

        } else if line.starts_with("filename ") {
          if let Some(chunk) = current_chunk.take() {
            lazy_blame(chunk);
          }
        }
      } else {
        let mut splits = line.split(' ');

        let mut chunk = BlameChunk {
          sha: ObjectId::null(Kind::Sha1),
          line_original: 0,
          line_final: 0,
          num_lines: 0,
          previous_filename: None,
        };

        let sha_hex = splits.next().unwrap();
        hex::decode_to_slice(sha_hex, chunk.sha.as_mut_slice())?;

        chunk.line_original = splits.next().unwrap().parse()?;
        chunk.line_final = splits.next().unwrap().parse()?;
        chunk.num_lines = splits.next().unwrap().parse()?;

        current_chunk = Some(chunk)
      }
    }

    match rx_status.await {
      Ok(Ok(status)) => {
        if status.success() {
          Ok(())
        } else {
          Err(anyhow!("git-blame: exited with error code {}", status.code().unwrap_or(255)))
        }
      }
      Ok(Err(e)) => Err(e.into()),
      Err(_) => Err(anyhow::Error::msg("Failed to wait for git blame")),
    }
  }

}