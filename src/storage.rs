use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sled::Db;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub fn resolve_namespace(requested: &str) -> String {
    requested.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub id: String,
    pub namespace: String,
    pub key: String,
    pub value: String,
    pub tags: Vec<String>,
    pub agent_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub ttl_seconds: Option<i64>,
    #[serde(default)]
    pub version: u64,
    #[serde(default)]
    pub blob_ref: Option<String>,
}

impl MemoryEntry {
    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl_seconds {
            let expiry = self.created_at + chrono::Duration::seconds(ttl);
            Utc::now() > expiry
        } else {
            false
        }
    }

    pub fn as_summary(&self, max_chars: usize) -> String {
        let s = &self.value;
        if s.len() <= max_chars {
            s.clone()
        } else {
            format!("{}… ({} chars)", &s[..max_chars], s.len())
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub description: String,
    pub entries: Vec<MemoryEntry>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobEntry {
    pub hash: String,
    pub content: String,
    pub byte_len: usize,
    pub summary: String,
    pub ref_count: u32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobStats {
    pub total_blobs: usize,
    pub total_bytes: usize,
    pub total_refs: u32,
    pub bytes_saved: usize,
}

fn blob_summary(content: &str) -> String {
    const PREVIEW: usize = 380;
    let words: usize = content.split_whitespace().count();
    let chars = content.len();
    let preview = if content.len() > PREVIEW {
        format!("{}…", &content[..PREVIEW])
    } else {
        content.to_string()
    };
    format!("{preview}\n[{words} words, {chars} chars]")
}

pub fn sha256_hex(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub seq: u64,
    pub namespace: String,
    pub agent_id: String,
    pub event_type: String,
    pub payload: String,
    pub tags: Vec<String>,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Claimed,
    InProgress,
    Blocked,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Cancelled)
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Done => "done",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub namespace: String,
    pub title: String,
    pub description: String,
    pub status: TaskStatus,
    pub owner: Option<String>,
    pub created_by: String,
    pub dependencies: Vec<String>,
    pub tags: Vec<String>,
    pub priority: i32,
    pub result: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub claim_ttl_seconds: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Task {
    pub fn is_claim_expired(&self) -> bool {
        if let (Some(claimed_at), Some(ttl)) = (self.claimed_at, self.claim_ttl_seconds) {
            let expiry = claimed_at + chrono::Duration::seconds(ttl);
            Utc::now() > expiry
        } else {
            false
        }
    }

    pub fn format_compact(&self) -> String {
        let deps = if self.dependencies.is_empty() {
            String::new()
        } else {
            format!(" deps=[{}]", self.dependencies.join(","))
        };
        let owner = self
            .owner
            .as_deref()
            .map(|o| format!(" @{o}"))
            .unwrap_or_default();
        format!(
            "[{}] p{} {} — {}{}{} ({})",
            self.id,
            self.priority,
            self.status.as_str(),
            self.title,
            owner,
            deps,
            self.updated_at.format("%m/%d %H:%M")
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedEntry {
    pub key: String,
    pub version: u64,
    pub value_hash: String,
    pub author: String,
    pub change_note: Option<String>,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceToc {
    pub namespace: String,
    pub version: u64,
    pub entries: Vec<TocEntry>,
    pub total_bytes: usize,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TocEntry {
    pub key: String,
    pub tags: Vec<String>,
    pub byte_len: usize,
    pub version: u64,
    pub preview: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct Storage {
    db: Db,
    embedder: Arc<Mutex<Option<TextEmbedding>>>,
}

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot_product = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) {
        dot_product += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot_product / (norm_a.sqrt() * norm_b.sqrt())
    }
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self> {
        let db = sled::open(path).context("Failed to open sled database")?;
        tracing::info!("Storage opened at {:?}", path);
        Ok(Self {
            db,
            embedder: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn embed_text(&self, text: String) -> Result<Vec<f32>> {
        let mut guard = self.embedder.lock().await;
        if guard.is_none() {
            tracing::info!("Initializing fastembed (BGESmallENV15, ~130MB)...");
            let model = tokio::task::spawn_blocking(|| {
                TextEmbedding::try_new(InitOptions::new(EmbeddingModel::BGESmallENV15))
            })
            .await
            .context("Task join error")?
            .context("Failed to initialize text embedding model")?;
            *guard = Some(model);
        }
        let model = guard
            .as_mut()
            .context("TextEmbedding model not initialized")?;
        let result = model.embed(vec![text], None)?;
        result
            .into_iter()
            .next()
            .context("Model failed to output embedding")
    }

    fn memory_tree(&self, namespace: &str) -> Result<sled::Tree> {
        let ns = resolve_namespace(namespace);
        Ok(self.db.open_tree(format!("mem:{ns}"))?)
    }

    fn snapshot_tree(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree("snapshots")?)
    }

    fn blob_tree(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree("blobs")?)
    }

    fn stream_tree(&self, namespace: &str) -> Result<sled::Tree> {
        let ns = resolve_namespace(namespace);
        Ok(self.db.open_tree(format!("stream:{ns}"))?)
    }

    fn stream_cursor_tree(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree("stream_cursors")?)
    }

    fn task_tree(&self, namespace: &str) -> Result<sled::Tree> {
        let ns = resolve_namespace(namespace);
        Ok(self.db.open_tree(format!("tasks:{ns}"))?)
    }

    fn version_tree(&self, namespace: &str) -> Result<sled::Tree> {
        let ns = resolve_namespace(namespace);
        Ok(self.db.open_tree(format!("versions:{ns}"))?)
    }

    fn embed_tree(&self, namespace: &str) -> Result<sled::Tree> {
        let ns = resolve_namespace(namespace);
        Ok(self.db.open_tree(format!("embed:{ns}"))?)
    }

    fn ns_version_tree(&self) -> Result<sled::Tree> {
        Ok(self.db.open_tree("ns_version")?)
    }

    pub fn set_memory(&self, mut entry: MemoryEntry) -> Result<MemoryEntry> {
        let tree = self.memory_tree(&entry.namespace)?;

        let new_version = self.increment_ns_version(&entry.namespace)?;
        entry.version = new_version;

        let ve = VersionedEntry {
            key: entry.key.clone(),
            version: new_version,
            value_hash: sha256_hex(&entry.value)[..16].to_string(),
            author: entry.agent_id.clone(),
            change_note: None,
            ts: entry.updated_at,
        };
        let vk = format!("{}:{:016x}", entry.key, new_version);
        self.version_tree(&entry.namespace)?
            .insert(vk.as_bytes(), serde_json::to_vec(&ve)?)?;

        let bytes = serde_json::to_vec(&entry)?;
        tree.insert(entry.key.as_bytes(), bytes)?;
        tree.flush()?;

        let embed_text = format!("Key: {}\nValue: {}", entry.key, entry.value);
        let storage_clone = self.clone();
        let namespace_clone = entry.namespace.clone();
        let key_clone = entry.key.clone();

        tokio::spawn(async move {
            match storage_clone.embed_text(embed_text).await {
                Ok(vec) => {
                    if let Ok(embed_tree) = storage_clone.embed_tree(&namespace_clone)
                        && let Ok(bytes) = serde_json::to_vec(&vec) {
                            let _ = embed_tree.insert(key_clone.as_bytes(), bytes);
                        }
                }
                Err(e) => tracing::debug!("RAG embedding skipped for key '{}': {}", key_clone, e),
            }
        });

        Ok(entry)
    }

    pub fn get_memory(&self, namespace: &str, key: &str) -> Result<Option<MemoryEntry>> {
        let tree = self.memory_tree(namespace)?;
        match tree.get(key.as_bytes())? {
            None => Ok(None),
            Some(bytes) => {
                let entry: MemoryEntry = serde_json::from_slice(&bytes)?;
                if entry.is_expired() {
                    tree.remove(key.as_bytes())?;
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
        }
    }

    pub fn delete_memory(&self, namespace: &str, key: &str) -> Result<bool> {
        let tree = self.memory_tree(namespace)?;
        let existed = tree.remove(key.as_bytes())?.is_some();
        tree.flush()?;
        Ok(existed)
    }

    pub fn list_memories(
        &self,
        namespace: &str,
        tag_filter: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let tree = self.memory_tree(namespace)?;
        let mut results = Vec::new();
        let mut expired_keys = Vec::new();

        for item in tree.iter() {
            let (key, val) = item?;
            match serde_json::from_slice::<MemoryEntry>(&val) {
                Ok(entry) => {
                    if entry.is_expired() {
                        expired_keys.push(key.to_vec());
                        continue;
                    }
                    if let Some(filter) = tag_filter {
                        let has_all = filter.iter().all(|t| entry.tags.iter().any(|et| et == t));
                        if !has_all {
                            continue;
                        }
                    }
                    results.push(entry);
                    if results.len() >= limit {
                        break;
                    }
                }
                Err(e) => tracing::warn!("Failed to deserialize entry: {e}"),
            }
        }
        for k in expired_keys {
            tree.remove(k)?;
        }
        results.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        Ok(results)
    }

    pub async fn search_memories(
        &self,
        namespace: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>> {
        let q = query.to_lowercase();
        let all_memories = self.list_memories(namespace, None, usize::MAX)?;

        if let Ok(query_embed) = self.embed_text(query.to_string()).await
            && let Ok(embed_tree) = self.embed_tree(namespace) {
                let mut scored = Vec::new();
                for memory in &all_memories {
                    let mut score = 0.0;

                    if memory.value.to_lowercase().contains(&q)
                        || memory.key.to_lowercase().contains(&q)
                        || memory.tags.iter().any(|t| t.to_lowercase().contains(&q))
                    {
                        score = 0.5;
                    }

                    if let Ok(Some(bytes)) = embed_tree.get(memory.key.as_bytes())
                        && let Ok(vec) = serde_json::from_slice::<Vec<f32>>(&bytes) {
                            let sim = cosine_similarity(&query_embed, &vec);
                            if sim > score {
                                score = sim;
                            }
                        }
                    scored.push((score, memory.clone()));
                }

                scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                return Ok(scored
                    .into_iter()
                    .filter(|(score, _)| *score > 0.4 || limit == 1)
                    .take(limit)
                    .map(|(_, m)| m)
                    .collect());
            }

        Ok(all_memories
            .into_iter()
            .filter(|e| {
                e.value.to_lowercase().contains(&q)
                    || e.key.to_lowercase().contains(&q)
                    || e.tags.iter().any(|t| t.to_lowercase().contains(&q))
            })
            .take(limit)
            .collect())
    }

    pub fn clear_namespace(&self, namespace: &str) -> Result<usize> {
        let tree = self.memory_tree(namespace)?;
        let count = tree.len();
        tree.clear()?;
        tree.flush()?;
        if let Ok(embed_tree) = self.embed_tree(namespace) {
            let _ = embed_tree.clear();
        }
        Ok(count)
    }

    pub fn list_namespaces(&self) -> Result<Vec<String>> {
        let expected_prefix = "mem:";

        Ok(self
            .db
            .tree_names()
            .into_iter()
            .filter_map(|n| {
                let s = String::from_utf8(n.to_vec()).ok()?;
                s.strip_prefix(expected_prefix).map(|ns| ns.to_string())
            })
            .collect())
    }

    pub fn save_snapshot(&self, snapshot: ContextSnapshot) -> Result<ContextSnapshot> {
        let tree = self.snapshot_tree()?;
        tree.insert(snapshot.id.as_bytes(), serde_json::to_vec(&snapshot)?)?;
        tree.flush()?;
        Ok(snapshot)
    }

    pub fn get_snapshot(&self, id: &str) -> Result<Option<ContextSnapshot>> {
        let tree = self.snapshot_tree()?;
        match tree.get(id.as_bytes())? {
            None => Ok(None),
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        }
    }

    pub fn list_snapshots(
        &self,
        namespace_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ContextSnapshot>> {
        let tree = self.snapshot_tree()?;
        let mut snaps: Vec<ContextSnapshot> = tree
            .iter()
            .filter_map(|r| r.ok())
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .filter(|s: &ContextSnapshot| namespace_filter.is_none_or(|ns| s.namespace == ns))
            .collect();
        snaps.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        snaps.truncate(limit);
        Ok(snaps)
    }

    pub fn delete_snapshot(&self, id: &str) -> Result<bool> {
        let tree = self.snapshot_tree()?;
        let existed = tree.remove(id.as_bytes())?.is_some();
        tree.flush()?;
        Ok(existed)
    }

    pub fn store_blob(&self, content: String) -> Result<BlobEntry> {
        let tree = self.blob_tree()?;
        let hash = sha256_hex(&content);

        if let Some(existing) = tree.get(hash.as_bytes())? {
            let mut blob: BlobEntry = serde_json::from_slice(&existing)?;
            blob.ref_count += 1;
            tree.insert(hash.as_bytes(), serde_json::to_vec(&blob)?)?;
            tree.flush()?;
            return Ok(blob);
        }

        let blob = BlobEntry {
            hash: hash.clone(),
            byte_len: content.len(),
            summary: blob_summary(&content),
            content,
            ref_count: 1,
            created_at: Utc::now(),
        };
        tree.insert(hash.as_bytes(), serde_json::to_vec(&blob)?)?;
        tree.flush()?;
        Ok(blob)
    }

    pub fn get_blob(&self, hash: &str) -> Result<Option<BlobEntry>> {
        let tree = self.blob_tree()?;
        match tree.get(hash.as_bytes())? {
            None => Ok(None),
            Some(b) => Ok(Some(serde_json::from_slice(&b)?)),
        }
    }

    #[allow(dead_code)]
    pub fn release_blob(&self, hash: &str) -> Result<()> {
        let tree = self.blob_tree()?;
        if let Some(existing) = tree.get(hash.as_bytes())? {
            let mut blob: BlobEntry = serde_json::from_slice(&existing)?;
            if blob.ref_count <= 1 {
                tree.remove(hash.as_bytes())?;
            } else {
                blob.ref_count -= 1;
                tree.insert(hash.as_bytes(), serde_json::to_vec(&blob)?)?;
            }
            tree.flush()?;
        }
        Ok(())
    }

    pub fn blob_stats(&self) -> Result<BlobStats> {
        let tree = self.blob_tree()?;
        let mut total_blobs = 0usize;
        let mut total_bytes = 0usize;
        let mut total_refs = 0u32;

        for item in tree.iter() {
            let (_, val) = item?;
            if let Ok(blob) = serde_json::from_slice::<BlobEntry>(&val) {
                total_blobs += 1;
                total_bytes += blob.byte_len;
                total_refs += blob.ref_count;
            }
        }

        let bytes_saved = if total_refs > total_blobs as u32 {
            total_bytes * (total_refs as usize - total_blobs)
        } else {
            0
        };

        Ok(BlobStats {
            total_blobs,
            total_bytes,
            total_refs,
            bytes_saved,
        })
    }

    fn next_stream_seq(&self, namespace: &str) -> Result<u64> {
        let tree = self.stream_tree(namespace)?;
        let meta_key = b"__seq__";
        let next = tree
            .fetch_and_update(meta_key, |old| {
                let cur = old
                    .and_then(|b| b.try_into().ok().map(u64::from_be_bytes))
                    .unwrap_or(0);
                Some((cur + 1).to_be_bytes().to_vec())
            })?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0)
            + 1;
        Ok(next)
    }

    pub fn stream_publish(
        &self,
        namespace: String,
        agent_id: String,
        event_type: String,
        payload: String,
        tags: Vec<String>,
    ) -> Result<StreamEvent> {
        let seq = self.next_stream_seq(&namespace)?;
        let event = StreamEvent {
            seq,
            namespace: namespace.clone(),
            agent_id,
            event_type,
            payload,
            tags,
            ts: Utc::now(),
        };
        let tree = self.stream_tree(&namespace)?;
        let key = format!("{:016x}", seq);
        tree.insert(key.as_bytes(), serde_json::to_vec(&event)?)?;
        tree.flush()?;
        Ok(event)
    }

    pub fn stream_read(
        &self,
        namespace: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>> {
        let tree = self.stream_tree(namespace)?;
        let start_key = format!("{:016x}", since_seq + 1);

        let events: Vec<StreamEvent> = tree
            .range(start_key.as_bytes()..)
            .filter_map(|r| r.ok())
            .filter_map(|(k, v)| {
                if k.starts_with(b"_") {
                    return None;
                }
                serde_json::from_slice(&v).ok()
            })
            .take(limit)
            .collect();

        Ok(events)
    }

    pub fn stream_read_delta(
        &self,
        namespace: &str,
        agent_id: &str,
    ) -> Result<(Vec<StreamEvent>, u64)> {
        let cursor_tree = self.stream_cursor_tree()?;
        let cursor_key = format!("{namespace}:{agent_id}");
        let since: u64 = cursor_tree
            .get(cursor_key.as_bytes())?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0);

        let events = self.stream_read(namespace, since, 100)?;
        let new_cursor = events.last().map(|e| e.seq).unwrap_or(since);

        if new_cursor > since {
            cursor_tree.insert(cursor_key.as_bytes(), new_cursor.to_be_bytes().to_vec())?;
            cursor_tree.flush()?;
        }

        Ok((events, new_cursor))
    }

    pub fn stream_latest_seq(&self, namespace: &str) -> Result<u64> {
        let tree = self.stream_tree(namespace)?;
        Ok(tree
            .get(b"__seq__")?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0))
    }

    pub fn stream_get_cursor(&self, namespace: &str, agent_id: &str) -> Result<u64> {
        let tree = self.stream_cursor_tree()?;
        let key = format!("{namespace}:{agent_id}");
        Ok(tree
            .get(key.as_bytes())?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0))
    }

    pub fn stream_trim(&self, namespace: &str, keep_last_n: usize) -> Result<usize> {
        let tree = self.stream_tree(namespace)?;
        let all_keys: Vec<Vec<u8>> = tree
            .iter()
            .filter_map(|r| r.ok())
            .filter_map(|(k, _)| {
                if !k.starts_with(b"_") {
                    Some(k.to_vec())
                } else {
                    None
                }
            })
            .collect();

        let to_remove = if all_keys.len() > keep_last_n {
            all_keys.len() - keep_last_n
        } else {
            0
        };

        for key in all_keys.iter().take(to_remove) {
            tree.remove(key)?;
        }
        tree.flush()?;
        Ok(to_remove)
    }

    pub fn task_create(&self, task: Task) -> Result<Task> {
        let tree = self.task_tree(&task.namespace)?;
        tree.insert(task.id.as_bytes(), serde_json::to_vec(&task)?)?;
        tree.flush()?;
        Ok(task)
    }

    pub fn task_get(&self, namespace: &str, task_id: &str) -> Result<Option<Task>> {
        let tree = self.task_tree(namespace)?;
        match tree.get(task_id.as_bytes())? {
            None => Ok(None),
            Some(b) => {
                let mut task: Task = serde_json::from_slice(&b)?;
                if task.status == TaskStatus::Claimed && task.is_claim_expired() {
                    task.status = TaskStatus::Pending;
                    task.owner = None;
                    task.claimed_at = None;
                    task.updated_at = Utc::now();
                    tree.insert(task.id.as_bytes(), serde_json::to_vec(&task)?)?;
                }
                Ok(Some(task))
            }
        }
    }

    fn task_save(&self, task: &Task) -> Result<()> {
        let tree = self.task_tree(&task.namespace)?;
        tree.insert(task.id.as_bytes(), serde_json::to_vec(task)?)?;
        tree.flush()?;
        Ok(())
    }

    pub fn task_claim(&self, namespace: &str, task_id: &str, agent_id: &str) -> Result<Task> {
        let tree = self.task_tree(namespace)?;
        let mut task = self
            .task_get(namespace, task_id)?
            .ok_or_else(|| anyhow::anyhow!("Task '{task_id}' not found"))?;

        if (task.status == TaskStatus::Claimed || task.status == TaskStatus::InProgress)
            && let Some(owner) = &task.owner
                && owner != agent_id && !task.is_claim_expired() {
                    return Err(anyhow::anyhow!("Task already owned by '{owner}'"));
                }

        if !task.status.is_terminal() {
            let blocked_by: Vec<String> = task
                .dependencies
                .iter()
                .filter(|dep_id| {
                    self.task_get(namespace, dep_id)
                        .ok()
                        .flatten()
                        .map(|t| t.status != TaskStatus::Done)
                        .unwrap_or(true)
                })
                .cloned()
                .collect();

            if !blocked_by.is_empty() {
                return Err(anyhow::anyhow!(
                    "Task blocked by unfinished dependencies: {}",
                    blocked_by.join(", ")
                ));
            }
        }

        let now = Utc::now();
        task.status = TaskStatus::Claimed;
        task.owner = Some(agent_id.to_string());
        task.claimed_at = Some(now);
        task.updated_at = now;

        tree.insert(task.id.as_bytes(), serde_json::to_vec(&task)?)?;
        tree.flush()?;
        Ok(task)
    }

    pub fn task_update_status(
        &self,
        namespace: &str,
        task_id: &str,
        agent_id: &str,
        status: TaskStatus,
        result: Option<String>,
    ) -> Result<Task> {
        let mut task = self
            .task_get(namespace, task_id)?
            .ok_or_else(|| anyhow::anyhow!("Task '{task_id}' not found"))?;

        if let Some(owner) = &task.owner
            && owner != agent_id {
                return Err(anyhow::anyhow!("Only owner '{owner}' can update this task"));
            }

        let now = Utc::now();
        task.status = status.clone();
        task.updated_at = now;
        if let Some(r) = result {
            task.result = Some(r);
        }
        if status.is_terminal() {
            task.completed_at = Some(now);
        }

        self.task_save(&task)?;
        Ok(task)
    }

    pub fn task_handoff(
        &self,
        namespace: &str,
        task_id: &str,
        from_agent: &str,
        to_agent: &str,
    ) -> Result<Task> {
        let mut task = self
            .task_get(namespace, task_id)?
            .ok_or_else(|| anyhow::anyhow!("Task '{task_id}' not found"))?;

        if task.owner.as_deref() != Some(from_agent) {
            return Err(anyhow::anyhow!("Only current owner can hand off the task"));
        }

        task.owner = Some(to_agent.to_string());
        task.claimed_at = Some(Utc::now());
        task.updated_at = Utc::now();
        self.task_save(&task)?;
        Ok(task)
    }

    pub fn task_list(
        &self,
        namespace: &str,
        status_filter: Option<&str>,
        tag_filter: Option<&[String]>,
        limit: usize,
    ) -> Result<Vec<Task>> {
        let tree = self.task_tree(namespace)?;
        let mut tasks: Vec<Task> = tree
            .iter()
            .filter_map(|r| r.ok())
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .filter(|t: &Task| {
                let status_ok = status_filter
                    .map(|sf| t.status.as_str() == sf)
                    .unwrap_or(true);
                let tag_ok = tag_filter
                    .map(|tf| tf.iter().all(|tag| t.tags.contains(tag)))
                    .unwrap_or(true);
                status_ok && tag_ok
            })
            .collect();

        let now = Utc::now();
        for task in tasks.iter_mut() {
            if task.status == TaskStatus::Claimed && task.is_claim_expired() {
                task.status = TaskStatus::Pending;
                task.owner = None;
                task.claimed_at = None;
                task.updated_at = now;
                let _ = self.task_save(task);
            }
        }

        tasks.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.created_at.cmp(&b.created_at))
        });
        tasks.truncate(limit);
        Ok(tasks)
    }

    pub fn task_next(&self, namespace: &str, _agent_id: &str) -> Result<Option<Task>> {
        Ok(self
            .task_list(namespace, Some("pending"), None, 100)?
            .into_iter()
            .find(|task| {
                task.dependencies.iter().all(|dep_id| {
                    self.task_get(namespace, dep_id)
                        .ok()
                        .flatten()
                        .map(|t| t.status == TaskStatus::Done)
                        .unwrap_or(false)
                })
            }))
    }

    fn increment_ns_version(&self, namespace: &str) -> Result<u64> {
        let tree = self.ns_version_tree()?;
        let next = tree
            .fetch_and_update(namespace.as_bytes(), |old| {
                let cur = old
                    .and_then(|b| b.try_into().ok().map(u64::from_be_bytes))
                    .unwrap_or(0);
                Some((cur + 1).to_be_bytes().to_vec())
            })?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0)
            + 1;
        Ok(next)
    }

    pub fn get_namespace_version(&self, namespace: &str) -> Result<u64> {
        let tree = self.ns_version_tree()?;
        Ok(tree
            .get(namespace.as_bytes())?
            .and_then(|b| b.as_ref().try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0))
    }

    pub fn get_version_delta(
        &self,
        namespace: &str,
        since_version: u64,
    ) -> Result<Vec<VersionedEntry>> {
        let tree = self.version_tree(namespace)?;
        let mut entries: Vec<VersionedEntry> = tree
            .iter()
            .filter_map(|r| r.ok())
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .filter(|ve: &VersionedEntry| ve.version > since_version)
            .collect();
        entries.sort_by_key(|ve| ve.version);
        Ok(entries)
    }

    pub fn get_namespace_toc(&self, namespace: &str) -> Result<NamespaceToc> {
        let entries = self.list_memories(namespace, None, usize::MAX)?;
        let version = self.get_namespace_version(namespace)?;
        let mut total_bytes = 0usize;

        let toc_entries: Vec<TocEntry> = entries
            .iter()
            .map(|e| {
                total_bytes += e.value.len();
                TocEntry {
                    key: e.key.clone(),
                    tags: e.tags.clone(),
                    byte_len: e.value.len(),
                    version: e.version,
                    preview: e.as_summary(80),
                    updated_at: e.updated_at,
                }
            })
            .collect();

        Ok(NamespaceToc {
            namespace: namespace.to_string(),
            version,
            entries: toc_entries,
            total_bytes,
            generated_at: Utc::now(),
        })
    }

    pub fn get_entry_history(
        &self,
        namespace: &str,
        key: &str,
        limit: usize,
    ) -> Result<Vec<VersionedEntry>> {
        let tree = self.version_tree(namespace)?;
        let prefix = format!("{key}:");
        let mut history: Vec<VersionedEntry> = tree
            .scan_prefix(prefix.as_bytes())
            .filter_map(|r| r.ok())
            .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
            .collect();
        history.sort_by(|a, b| b.version.cmp(&a.version));
        history.truncate(limit);
        Ok(history)
    }
}

pub fn new_entry(
    namespace: String,
    key: String,
    value: String,
    agent_id: String,
    tags: Vec<String>,
    ttl_seconds: Option<i64>,
) -> MemoryEntry {
    let now = Utc::now();
    MemoryEntry {
        id: Uuid::new_v4().to_string(),
        namespace,
        key,
        value,
        agent_id,
        tags,
        created_at: now,
        updated_at: now,
        ttl_seconds,
        version: 0,
        blob_ref: None,
    }
}

pub fn new_snapshot(
    name: String,
    namespace: String,
    description: String,
    entries: Vec<MemoryEntry>,
    created_by: String,
) -> ContextSnapshot {
    ContextSnapshot {
        id: Uuid::new_v4().to_string(),
        name,
        namespace,
        description,
        entries,
        created_at: Utc::now(),
        created_by,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn new_task(
    namespace: String,
    title: String,
    description: String,
    created_by: String,
    dependencies: Vec<String>,
    tags: Vec<String>,
    priority: i32,
    claim_ttl_seconds: Option<i64>,
) -> Task {
    let now = Utc::now();
    Task {
        id: Uuid::new_v4().to_string(),
        namespace,
        title,
        description,
        status: TaskStatus::Pending,
        owner: None,
        created_by,
        dependencies,
        tags,
        priority,
        result: None,
        claimed_at: None,
        completed_at: None,
        claim_ttl_seconds,
        created_at: now,
        updated_at: now,
    }
}
