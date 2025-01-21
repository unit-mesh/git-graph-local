use std::sync::Mutex;

use gix::bstr::{BStr, BString};
use gix::ObjectId;
use integer_encoding::{VarIntReader, VarIntWriter};
use rusqlite::OptionalExtension;

use crate::cache::{Cache, CachedCommit};

pub(crate) struct SqliteCache {
  conn: Mutex<rusqlite::Connection>,
}

impl SqliteCache {
  pub(crate) fn new() -> anyhow::Result<Self> {
    let conn = rusqlite::Connection::open_in_memory()?;
    let cache = SqliteCache {
      conn: Mutex::new(conn),
    };
    cache.create_tables()?;
    Ok(cache)
  }

  fn create_tables(&self) -> rusqlite::Result<()> {
    let conn = self.conn.lock().unwrap();
    conn.execute(
      "CREATE TABLE paths (id INTEGER PRIMARY KEY, path BLOB NOT NULL, renamed_to INTEGER)",
      (),
    )?;
    conn.execute("CREATE UNIQUE INDEX paths_by_path ON paths(path)", ())?;

    conn.execute(
      "CREATE TABLE commits (sha BLOB PRIMARY KEY, changes BLOB)",
      (),
    )?;
    Ok(())
  }
}

impl Cache for SqliteCache {
  fn cache_path(&self, path: &BStr) -> anyhow::Result<u32> {
    let path: &[u8] = path.into();
    let conn = self.conn.lock().unwrap();
    let changed = conn.execute(
      "INSERT OR IGNORE INTO paths (path) VALUES (?)",
      rusqlite::params![path],
    )?;

    if changed > 0 {
      Ok(conn.last_insert_rowid() as u32)
    } else {
      let existing_id = conn.query_row(
        "SELECT id FROM paths WHERE path = ?",
        rusqlite::params![path],
        |row| row.get(0),
      )?;
      Ok(existing_id)
    }
  }

  fn cache_rename(&self, old_path: &BStr, new_path: u32) -> anyhow::Result<()> {
    // TODO: come up with a efficient way to store this
    Ok(())
  }

  fn resolve_path(&self, path_id: u32) -> anyhow::Result<Option<BString>> {
    let conn = self.conn.lock().unwrap();
    let row = conn
      .query_row(
        "SELECT path, renamed_to FROM paths WHERE id = ?",
        rusqlite::params![path_id],
        |row| {
          let path: Vec<u8> = row.get(0)?;
          let renamed_to: Option<u32> = row.get(1)?;
          Ok((path, renamed_to))
        },
      )
      .optional()?;

    match row {
      Some((path, renamed_to)) => {
        if let Some(renamed_to) = renamed_to {
          self.resolve_path(renamed_to)
        } else {
          Ok(Some(BString::new(path)))
        }
      }
      None => Ok(None),
    }
  }

  fn cached_commit(&self, id: &ObjectId) -> anyhow::Result<Option<CachedCommit>> {
    let conn = self.conn.lock().unwrap();
    let row: Option<Vec<u8>> = conn.query_row(
      "SELECT changes FROM commits WHERE sha = ?",
      rusqlite::params![id.as_slice()],
      |row| Ok(row.get(0)?),
    ).optional()?;

    Ok(row.map(|serialized_changes| {
      let mut changed_paths = Vec::new();
      let mut cursor = std::io::Cursor::new(serialized_changes);
      while let Ok(p) = cursor.read_varint() {
        changed_paths.push(p);
      }
      CachedCommit {
        changed_paths,
      }
    }))
  }

  fn update_cached_commit(&self, id: &ObjectId, commit: CachedCommit) -> anyhow::Result<()> {
    let mut serialized_bitmap = Vec::new();
    for p in commit.changed_paths.iter() {
      serialized_bitmap.write_varint(*p)?;
    }

    let conn = self.conn.lock().unwrap();
    conn.execute(
      "INSERT INTO commits(sha, changes) VALUES (?, ?) ON CONFLICT(sha) DO NOTHING;",
      rusqlite::params![id.as_slice(), &serialized_bitmap],
    )?;
    Ok(())
  }

  fn is_commit_cached(&self, id: &ObjectId) -> anyhow::Result<bool> {
    let conn = self.conn.lock().unwrap();
    let row: Option<()> = conn.query_row(
      "SELECT 1 FROM commits WHERE sha = ?",
      rusqlite::params![id.as_slice()],
      |_| Ok(()),
    ).optional()?;
    Ok(row.is_some())
  }
}
