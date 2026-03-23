use crate::storage::{Storage, TaskStatus, new_entry, new_snapshot, new_task};
use chrono::Utc;
use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemorySetParams {
    /// Namespace to isolate contexts (e.g. "project-alpha", "session-xyz")
    pub namespace: String,
    /// Key identifying this piece of context (e.g. "architecture_decisions")
    pub key: String,
    /// The value/content to store (plain text, JSON, markdown, etc.)
    pub value: String,
    /// ID of the calling agent (for attribution)
    pub agent_id: String,
    /// Optional comma-separated tags (e.g. "architecture,backend,critical")
    pub tags: Option<String>,
    /// Optional TTL in seconds - omit for permanent storage
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemoryGetParams {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemoryListParams {
    pub namespace: String,
    pub tag_filter: Option<String>,
    /// Max entries (default 50, max 500)
    pub limit: Option<usize>,
    /// "compact" (default) or "json"
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemorySearchParams {
    pub namespace: String,
    pub query: String,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemoryDeleteParams {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MemoryClearParams {
    pub namespace: String,
    /// Must be exactly "CONFIRM"
    pub confirm: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SnapshotCreateParams {
    pub namespace: String,
    pub name: String,
    pub description: String,
    pub created_by: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SnapshotRestoreParams {
    pub snapshot_id: String,
    pub target_namespace: Option<String>,
    pub restored_by: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SnapshotListParams {
    pub namespace_filter: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SnapshotDeleteParams {
    pub snapshot_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ExportContextParams {
    pub namespace: String,
    pub tag_filter: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ContextStoreParams {
    /// Large content to store (document, code, analysis, etc.)
    pub content: String,
    /// Optional: link this blob to a memory key for quick lookup
    pub link_namespace: Option<String>,
    /// Optional: memory key to link to
    pub link_key: Option<String>,
    /// Optional: agent creating this blob
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ContextGetParams {
    /// SHA-256 hash returned by context_store
    pub hash: String,
    /// "summary" (default, token-efficient) or "full"
    pub mode: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StreamPublishParams {
    pub namespace: String,
    pub agent_id: String,
    /// "finding" | "update" | "signal" | "result" | "error"
    pub event_type: String,
    /// The finding/update content
    pub payload: String,
    /// Optional comma-separated tags
    pub tags: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StreamReadParams {
    pub namespace: String,
    /// Read events with seq > this value. Use 0 for all.
    pub since_seq: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StreamDeltaParams {
    pub namespace: String,
    /// Agent ID — cursor is tracked per-agent, auto-advances after each call
    pub agent_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StreamStatusParams {
    pub namespace: String,
    /// Optional agent_id to include cursor position
    pub agent_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct StreamTrimParams {
    pub namespace: String,
    /// Keep this many most recent events
    pub keep_last_n: usize,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskCreateParams {
    pub namespace: String,
    pub title: String,
    pub description: String,
    pub created_by: String,
    /// Comma-separated task IDs that must be Done first
    pub dependencies: Option<String>,
    /// Comma-separated tags
    pub tags: Option<String>,
    /// 0 = low, 10 = critical (default 5)
    pub priority: Option<i32>,
    /// Auto-release claim after N seconds of silence (default: no timeout)
    pub claim_ttl_seconds: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskClaimParams {
    pub namespace: String,
    pub task_id: String,
    pub agent_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskCompleteParams {
    pub namespace: String,
    pub task_id: String,
    pub agent_id: String,
    /// Result or output of the completed task
    pub result: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskFailParams {
    pub namespace: String,
    pub task_id: String,
    pub agent_id: String,
    /// Reason for failure
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskHandoffParams {
    pub namespace: String,
    pub task_id: String,
    pub from_agent: String,
    pub to_agent: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskNextParams {
    pub namespace: String,
    pub agent_id: String,
    /// If true, automatically claim the next available task
    pub auto_claim: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskListParams {
    pub namespace: String,
    /// Filter by status: "pending" | "claimed" | "in_progress" | "done" | "failed" | "cancelled"
    pub status_filter: Option<String>,
    pub tag_filter: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct TaskStatusParams {
    pub namespace: String,
    pub task_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ContextTocParams {
    pub namespace: String,
    /// "compact" (default) or "json"
    pub format: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ContextDeltaParams {
    pub namespace: String,
    /// Return only entries changed after this version number
    pub since_version: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ContextVersionParams {
    pub namespace: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct EntryHistoryParams {
    pub namespace: String,
    pub key: String,
    pub limit: Option<usize>,
}

#[derive(Clone)]
pub struct SharedMemoryServer {
    storage: Arc<RwLock<Storage>>,
    tool_router: ToolRouter<Self>,
}

impl SharedMemoryServer {
    pub fn new(storage: Storage) -> Self {
        Self {
            storage: Arc::new(RwLock::new(storage)),
            tool_router: Self::tool_router(),
        }
    }

    fn format_entries_compact(entries: &[crate::storage::MemoryEntry]) -> String {
        if entries.is_empty() {
            return "No entries found.".to_string();
        }
        entries
            .iter()
            .map(|e| {
                let tags = if e.tags.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", e.tags.join(","))
                };
                format!("{}={}{}", e.key, e.value, tags)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn parse_tags(raw: Option<String>) -> Vec<String> {
        raw.unwrap_or_default()
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    }

    fn parse_ids(raw: Option<String>) -> Vec<String> {
        raw.unwrap_or_default()
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect()
    }

    fn format_stream_events(events: &[crate::storage::StreamEvent]) -> String {
        if events.is_empty() {
            return "No events.".to_string();
        }
        events
            .iter()
            .map(|e| {
                let tags = if e.tags.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", e.tags.join(","))
                };
                format!(
                    "[#{:06}] @{} ({}) {}: {}{}",
                    e.seq,
                    e.agent_id,
                    e.ts.format("%H:%M:%S"),
                    e.event_type,
                    e.payload,
                    tags
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[tool_router]
impl SharedMemoryServer {
    #[tool(
        description = "Store a key-value pair in shared memory. Use namespaces to isolate projects. Supports optional tags and TTL. Each write is versioned automatically."
    )]
    async fn memory_set(
        &self,
        Parameters(MemorySetParams {
            namespace,
            key,
            value,
            agent_id,
            tags,
            ttl_seconds,
        }): Parameters<MemorySetParams>,
    ) -> String {
        let tags = Self::parse_tags(tags);
        let entry = new_entry(
            namespace.clone(),
            key.clone(),
            value,
            agent_id.clone(),
            tags.clone(),
            ttl_seconds,
        );
        let storage = self.storage.read().await;
        match storage.set_memory(entry) {
            Ok(e) => format!(
                "✓ Stored [{namespace}:{key}] v{} by {agent_id} (tags: {}{})",
                e.version,
                if tags.is_empty() {
                    "none".to_string()
                } else {
                    tags.join(",")
                },
                if let Some(ttl) = ttl_seconds {
                    format!(", TTL: {ttl}s")
                } else {
                    String::new()
                }
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "Get a specific value from shared memory by namespace and key.")]
    async fn memory_get(
        &self,
        Parameters(MemoryGetParams { namespace, key }): Parameters<MemoryGetParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.get_memory(&namespace, &key) {
            Ok(Some(entry)) => serde_json::to_string_pretty(&json!({
                "key": entry.key,
                "value": entry.value,
                "tags": entry.tags,
                "agent_id": entry.agent_id,
                "version": entry.version,
                "updated_at": entry.updated_at.to_rfc3339(),
                "ttl_seconds": entry.ttl_seconds,
                "blob_ref": entry.blob_ref
            }))
            .unwrap_or_else(|_| "ERROR: serialization failed".to_string()),
            Ok(None) => format!("No entry found for [{namespace}:{key}]"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "List all entries in a namespace. format='compact' saves tokens; format='json' gives full metadata."
    )]
    async fn memory_list(
        &self,
        Parameters(MemoryListParams {
            namespace,
            tag_filter,
            limit,
            format,
        }): Parameters<MemoryListParams>,
    ) -> String {
        let tags = tag_filter.map(|tf| Self::parse_tags(Some(tf)));
        let limit = limit.unwrap_or(50).min(500);
        let fmt = format.unwrap_or_else(|| "compact".to_string());
        let storage = self.storage.read().await;

        match storage.list_memories(&namespace, tags.as_deref(), limit) {
            Ok(entries) => {
                let count = entries.len();
                if fmt == "json" {
                    let items: Vec<serde_json::Value> = entries
                        .iter()
                        .map(|e| json!({"key": e.key, "value": e.value, "tags": e.tags, "version": e.version, "updated_at": e.updated_at.to_rfc3339()}))
                        .collect();
                    serde_json::to_string_pretty(&items).unwrap_or_default()
                } else {
                    format!(
                        "# Namespace: {namespace} ({count} entries)\n\n{}",
                        Self::format_entries_compact(&entries)
                    )
                }
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "Search memory entries by keyword across keys, values, and tags.")]
    async fn memory_search(
        &self,
        Parameters(MemorySearchParams {
            namespace,
            query,
            limit,
        }): Parameters<MemorySearchParams>,
    ) -> String {
        let limit = limit.unwrap_or(20).min(100);
        let storage = self.storage.read().await;
        match storage.search_memories(&namespace, &query, limit).await {
            Ok(entries) => format!(
                "# Search: '{query}' in [{namespace}] ({} hits)\n\n{}",
                entries.len(),
                Self::format_entries_compact(&entries)
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "Delete a specific memory entry by namespace and key.")]
    async fn memory_delete(
        &self,
        Parameters(MemoryDeleteParams { namespace, key }): Parameters<MemoryDeleteParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.delete_memory(&namespace, &key) {
            Ok(true) => format!("✓ Deleted [{namespace}:{key}]"),
            Ok(false) => format!("Entry [{namespace}:{key}] not found"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Clear ALL memory entries in a namespace. IRREVERSIBLE! Set confirm='CONFIRM'. Snapshot first!"
    )]
    async fn memory_clear(
        &self,
        Parameters(MemoryClearParams { namespace, confirm }): Parameters<MemoryClearParams>,
    ) -> String {
        if confirm != "CONFIRM" {
            return "ERROR: 'confirm' must be exactly 'CONFIRM'".to_string();
        }
        let storage = self.storage.read().await;
        match storage.clear_namespace(&namespace) {
            Ok(count) => format!("✓ Cleared {count} entries from namespace [{namespace}]"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "List all namespaces that have stored memory entries.")]
    async fn namespace_list(&self) -> String {
        let storage = self.storage.read().await;
        match storage.list_namespaces() {
            Ok(ns) if ns.is_empty() => "No namespaces found.".to_string(),
            Ok(ns) => format!(
                "Namespaces ({}):\n{}",
                ns.len(),
                ns.iter()
                    .map(|n| format!("  - {n}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "Create a named checkpoint of all entries in a namespace.")]
    async fn snapshot_create(
        &self,
        Parameters(SnapshotCreateParams {
            namespace,
            name,
            description,
            created_by,
        }): Parameters<SnapshotCreateParams>,
    ) -> String {
        let storage = self.storage.read().await;
        let entries = match storage.list_memories(&namespace, None, usize::MAX) {
            Ok(e) => e,
            Err(e) => return format!("ERROR: {e}"),
        };
        let count = entries.len();
        let snap = new_snapshot(
            name.clone(),
            namespace.clone(),
            description,
            entries,
            created_by.clone(),
        );
        match storage.save_snapshot(snap) {
            Ok(s) => format!(
                "✓ Snapshot '{name}' created\n  ID: {}\n  Namespace: {namespace}\n  Entries: {count}\n  By: {created_by}",
                s.id
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Restore memory entries from a snapshot, optionally to a different namespace."
    )]
    async fn snapshot_restore(
        &self,
        Parameters(SnapshotRestoreParams {
            snapshot_id,
            target_namespace,
            restored_by,
        }): Parameters<SnapshotRestoreParams>,
    ) -> String {
        let storage = self.storage.read().await;
        let snap = match storage.get_snapshot(&snapshot_id) {
            Ok(Some(s)) => s,
            Ok(None) => return format!("ERROR: Snapshot '{snapshot_id}' not found"),
            Err(e) => return format!("ERROR: {e}"),
        };
        let ns = target_namespace.unwrap_or_else(|| snap.namespace.clone());
        let now = Utc::now();
        let mut count = 0usize;
        for mut entry in snap.entries {
            entry.namespace = ns.clone();
            entry.agent_id = format!("restored-by:{restored_by}");
            entry.updated_at = now;
            if storage.set_memory(entry).is_ok() {
                count += 1;
            }
        }
        format!(
            "✓ Restored {count} entries from '{}' into [{ns}]",
            snap.name
        )
    }

    #[tool(description = "List all context snapshots, optionally filtered by namespace.")]
    async fn snapshot_list(
        &self,
        Parameters(SnapshotListParams {
            namespace_filter,
            limit,
        }): Parameters<SnapshotListParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.list_snapshots(namespace_filter.as_deref(), limit.unwrap_or(20).min(100)) {
            Ok(snaps) if snaps.is_empty() => "No snapshots found.".to_string(),
            Ok(snaps) => {
                let lines: Vec<String> = snaps
                    .iter()
                    .map(|s| {
                        format!(
                            "- [{}] '{}' in [{}] ({} entries) by {}\n  {}\n  ID: {}",
                            s.created_at.format("%Y-%m-%d %H:%M UTC"),
                            s.name,
                            s.namespace,
                            s.entries.len(),
                            s.created_by,
                            s.description,
                            s.id
                        )
                    })
                    .collect();
                format!("# Snapshots ({})\n\n{}", snaps.len(), lines.join("\n\n"))
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(description = "Delete a context snapshot by ID.")]
    async fn snapshot_delete(
        &self,
        Parameters(SnapshotDeleteParams { snapshot_id }): Parameters<SnapshotDeleteParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.delete_snapshot(&snapshot_id) {
            Ok(true) => format!("✓ Snapshot '{snapshot_id}' deleted"),
            Ok(false) => format!("Snapshot '{snapshot_id}' not found"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Export namespace as a token-efficient context block grouped by tags. Inject the output directly into agent system prompts."
    )]
    async fn export_context(
        &self,
        Parameters(ExportContextParams {
            namespace,
            tag_filter,
        }): Parameters<ExportContextParams>,
    ) -> String {
        let tags = tag_filter.map(|tf| Self::parse_tags(Some(tf)));
        let storage = self.storage.read().await;
        let version = storage.get_namespace_version(&namespace).unwrap_or(0);
        match storage.list_memories(&namespace, tags.as_deref(), 200) {
            Ok(entries) if entries.is_empty() => "No entries to export.".to_string(),
            Ok(entries) => {
                let mut sections = vec![format!(
                    "=== SHARED CONTEXT: {namespace} | v{version} | {} entries | {} ===",
                    entries.len(),
                    Utc::now().format("%Y-%m-%d %H:%M UTC")
                )];
                let mut tagged: Vec<&crate::storage::MemoryEntry> =
                    entries.iter().filter(|e| !e.tags.is_empty()).collect();
                let untagged: Vec<_> = entries.iter().filter(|e| e.tags.is_empty()).collect();
                tagged.sort_by(|a, b| a.tags[0].cmp(&b.tags[0]));
                let mut cur_tag = "";
                for e in &tagged {
                    let t = e.tags[0].as_str();
                    if t != cur_tag {
                        sections.push(format!("\n[{t}]"));
                        cur_tag = t;
                    }
                    sections.push(format!("{}={}", e.key, e.value));
                }
                if !untagged.is_empty() {
                    sections.push("\n[general]".to_string());
                    for e in untagged {
                        sections.push(format!("{}={}", e.key, e.value));
                    }
                }
                sections.push(format!("\n=== END CONTEXT: {namespace} ==="));
                sections.join("\n")
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Store large content (documents, analysis, code) with deduplication. Same content stored once even if multiple agents reference it. Returns hash + 100-token summary. Use context_get with mode='full' to fetch complete content."
    )]
    async fn context_store(
        &self,
        Parameters(ContextStoreParams {
            content,
            link_namespace,
            link_key,
            agent_id,
        }): Parameters<ContextStoreParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.store_blob(content) {
            Ok(blob) => {
                let short_hash = &blob.hash[..16];
                let mut result = format!(
                    "✓ Blob stored\n  Hash: {}\n  Size: {} chars\n  Ref count: {}\n\nSummary:\n{}",
                    short_hash, blob.byte_len, blob.ref_count, blob.summary
                );

                if let (Some(ns), Some(key), Some(aid)) = (link_namespace, link_key, agent_id) {
                    let mut entry = new_entry(
                        ns.clone(),
                        key.clone(),
                        String::new(),
                        aid,
                        Vec::new(),
                        None,
                    );
                    entry.blob_ref = Some(blob.hash.clone());
                    entry.value = format!("<blob:{}>", &blob.hash[..16]);
                    if let Ok(saved) = storage.set_memory(entry) {
                        result.push_str(&format!("\n  Linked to [{ns}:{key}] v{}", saved.version));
                    }
                }
                result
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Retrieve a stored blob by hash. mode='summary' (default) returns 100-token preview. mode='full' returns complete content. Always check summary first!"
    )]
    async fn context_get(
        &self,
        Parameters(ContextGetParams { hash, mode }): Parameters<ContextGetParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.get_blob(&hash) {
            Ok(Some(blob)) => {
                let mode = mode.unwrap_or_else(|| "summary".to_string());
                if mode == "full" {
                    format!(
                        "# Full blob ({} chars, {} refs)\n\n{}",
                        blob.byte_len, blob.ref_count, blob.content
                    )
                } else {
                    format!(
                        "# Blob summary ({} chars, {} refs)\n\n{}\n\n[Use mode='full' to get complete content]",
                        blob.byte_len, blob.ref_count, blob.summary
                    )
                }
            }
            Ok(None) => format!("No blob found for hash '{hash}'"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get blob deduplication statistics: total blobs, total bytes, how many references, and estimated bytes saved vs inline storage."
    )]
    async fn context_stats(&self) -> String {
        let storage = self.storage.read().await;
        match storage.blob_stats() {
            Ok(stats) => format!(
                "# Blob Storage Stats\n  Unique blobs: {}\n  Total size: {} bytes\n  Total references: {}\n  Estimated bytes saved: {} ({:.1}x compression)",
                stats.total_blobs,
                stats.total_bytes,
                stats.total_refs,
                stats.bytes_saved,
                if stats.total_bytes > 0 {
                    stats.bytes_saved as f64 / stats.total_bytes as f64 + 1.0
                } else {
                    1.0
                }
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Publish a finding, update, or signal to the shared stream. Other agents instantly see this via stream_delta without polling memory. event_type: 'finding' | 'update' | 'signal' | 'result' | 'error'"
    )]
    async fn stream_publish(
        &self,
        Parameters(StreamPublishParams {
            namespace,
            agent_id,
            event_type,
            payload,
            tags,
        }): Parameters<StreamPublishParams>,
    ) -> String {
        let tags = Self::parse_tags(tags);
        let storage = self.storage.read().await;
        match storage.stream_publish(
            namespace.clone(),
            agent_id.clone(),
            event_type.clone(),
            payload,
            tags,
        ) {
            Ok(ev) => format!(
                "✓ Published to [{namespace}] #{} by {agent_id} ({})",
                ev.seq, ev.event_type
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Read stream events since seq N. Use since_seq=0 for all events. For efficient polling, prefer stream_delta which tracks your position automatically."
    )]
    async fn stream_read(
        &self,
        Parameters(StreamReadParams {
            namespace,
            since_seq,
            limit,
        }): Parameters<StreamReadParams>,
    ) -> String {
        let since = since_seq.unwrap_or(0);
        let limit = limit.unwrap_or(50).min(200);
        let storage = self.storage.read().await;
        match storage.stream_read(&namespace, since, limit) {
            Ok(events) => format!(
                "# Stream [{namespace}] since #{since} ({} events)\n\n{}",
                events.len(),
                Self::format_stream_events(&events)
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Read ONLY new stream events since your last call. Cursor auto-advances per agent_id. Call repeatedly to get incremental updates. Returns empty if no new events."
    )]
    async fn stream_delta(
        &self,
        Parameters(StreamDeltaParams {
            namespace,
            agent_id,
        }): Parameters<StreamDeltaParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.stream_read_delta(&namespace, &agent_id) {
            Ok((events, new_cursor)) => {
                if events.is_empty() {
                    format!(
                        "No new events in [{namespace}] for @{agent_id} (cursor at #{new_cursor})"
                    )
                } else {
                    format!(
                        "# Delta [{namespace}] for @{agent_id} ({} new events, cursor→#{new_cursor})\n\n{}",
                        events.len(),
                        Self::format_stream_events(&events)
                    )
                }
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get stream status: total events, latest seq, and optionally your cursor position."
    )]
    async fn stream_status(
        &self,
        Parameters(StreamStatusParams {
            namespace,
            agent_id,
        }): Parameters<StreamStatusParams>,
    ) -> String {
        let storage = self.storage.read().await;
        let latest = storage.stream_latest_seq(&namespace).unwrap_or(0);
        let cursor_info = if let Some(aid) = agent_id {
            let cursor = storage.stream_get_cursor(&namespace, &aid).unwrap_or(0);
            let behind = latest.saturating_sub(cursor);
            format!("\n  @{aid} cursor: #{cursor} ({behind} events behind)")
        } else {
            String::new()
        };
        format!("# Stream [{namespace}]\n  Latest seq: #{latest}{cursor_info}")
    }

    #[tool(
        description = "Trim old stream events, keeping only the last N events. Use to prevent unbounded stream growth."
    )]
    async fn stream_trim(
        &self,
        Parameters(StreamTrimParams {
            namespace,
            keep_last_n,
        }): Parameters<StreamTrimParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.stream_trim(&namespace, keep_last_n) {
            Ok(removed) => format!("✓ Trimmed {removed} old events from [{namespace}] stream"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Create a work task in the shared pool. Set dependencies to block on other tasks. Set claim_ttl_seconds to auto-release if the claiming agent goes silent."
    )]
    async fn task_create(
        &self,
        Parameters(TaskCreateParams {
            namespace,
            title,
            description,
            created_by,
            dependencies,
            tags,
            priority,
            claim_ttl_seconds,
        }): Parameters<TaskCreateParams>,
    ) -> String {
        let deps = Self::parse_ids(dependencies);
        let tags = Self::parse_tags(tags);
        let priority = priority.unwrap_or(5).clamp(0, 10);
        let task = new_task(
            namespace.clone(),
            title.clone(),
            description,
            created_by.clone(),
            deps,
            tags,
            priority,
            claim_ttl_seconds,
        );
        let storage = self.storage.read().await;
        match storage.task_create(task) {
            Ok(t) => format!(
                "✓ Task created\n  ID: {}\n  Title: {title}\n  Priority: {priority}\n  Namespace: {namespace}\n  By: {created_by}",
                t.id
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Atomically claim a task. Fails if already claimed by another active agent. Blocked if dependencies are not Done. On success, task transitions to 'claimed'."
    )]
    async fn task_claim(
        &self,
        Parameters(TaskClaimParams {
            namespace,
            task_id,
            agent_id,
        }): Parameters<TaskClaimParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_claim(&namespace, &task_id, &agent_id) {
            Ok(t) => format!(
                "✓ Task '{task_id}' claimed by @{agent_id}\n  Title: {}\n  Priority: {}",
                t.title, t.priority
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Mark your task as Done. Provide a result string to share output with other agents. This unblocks dependent tasks."
    )]
    async fn task_complete(
        &self,
        Parameters(TaskCompleteParams {
            namespace,
            task_id,
            agent_id,
            result,
        }): Parameters<TaskCompleteParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_update_status(&namespace, &task_id, &agent_id, TaskStatus::Done, result)
        {
            Ok(t) => format!(
                "✓ Task '{task_id}' completed by @{agent_id}\n  Title: {}\n  Result: {}",
                t.title,
                t.result.as_deref().unwrap_or("(no result)")
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Mark your task as Failed with a reason. This frees the task for retry or reassignment."
    )]
    async fn task_fail(
        &self,
        Parameters(TaskFailParams {
            namespace,
            task_id,
            agent_id,
            reason,
        }): Parameters<TaskFailParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_update_status(
            &namespace,
            &task_id,
            &agent_id,
            TaskStatus::Failed,
            reason,
        ) {
            Ok(t) => format!(
                "✗ Task '{task_id}' failed\n  Title: {}\n  Reason: {}",
                t.title,
                t.result.as_deref().unwrap_or("(no reason)")
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Hand off your current task to another agent. The new agent becomes the owner and can update status."
    )]
    async fn task_handoff(
        &self,
        Parameters(TaskHandoffParams {
            namespace,
            task_id,
            from_agent,
            to_agent,
        }): Parameters<TaskHandoffParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_handoff(&namespace, &task_id, &from_agent, &to_agent) {
            Ok(t) => format!(
                "✓ Task '{task_id}' handed off @{from_agent} → @{to_agent}\n  Title: {}",
                t.title
            ),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Find the next available task for this agent. Set auto_claim=true to atomically claim it. Respects dependencies and priority order."
    )]
    async fn task_next(
        &self,
        Parameters(TaskNextParams {
            namespace,
            agent_id,
            auto_claim,
        }): Parameters<TaskNextParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_next(&namespace, &agent_id) {
            Ok(None) => format!("No available tasks in [{namespace}]"),
            Ok(Some(task)) => {
                if auto_claim.unwrap_or(false) {
                    drop(storage);
                    let storage = self.storage.read().await;
                    match storage.task_claim(&namespace, &task.id, &agent_id) {
                        Ok(t) => format!("✓ Claimed next task\n{}", t.format_compact()),
                        Err(e) => format!("ERROR claiming: {e}"),
                    }
                } else {
                    format!("Next task:\n{}", task.format_compact())
                }
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "List tasks filtered by status and/or tags. status_filter: 'pending'|'claimed'|'in_progress'|'blocked'|'done'|'failed'|'cancelled'. Sorted by priority desc."
    )]
    async fn task_list(
        &self,
        Parameters(TaskListParams {
            namespace,
            status_filter,
            tag_filter,
            limit,
        }): Parameters<TaskListParams>,
    ) -> String {
        let tags = tag_filter.map(|tf| Self::parse_tags(Some(tf)));
        let limit = limit.unwrap_or(30).min(200);
        let storage = self.storage.read().await;
        match storage.task_list(&namespace, status_filter.as_deref(), tags.as_deref(), limit) {
            Ok(tasks) if tasks.is_empty() => "No tasks found.".to_string(),
            Ok(tasks) => {
                let lines: Vec<String> = tasks.iter().map(|t| t.format_compact()).collect();
                format!(
                    "# Tasks in [{namespace}] ({})\n\n{}",
                    tasks.len(),
                    lines.join("\n")
                )
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get complete details of a specific task including status, owner, dependencies, and result."
    )]
    async fn task_status(
        &self,
        Parameters(TaskStatusParams { namespace, task_id }): Parameters<TaskStatusParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.task_get(&namespace, &task_id) {
            Ok(None) => format!("Task '{task_id}' not found in [{namespace}]"),
            Ok(Some(t)) => serde_json::to_string_pretty(&json!({
                "id": t.id,
                "title": t.title,
                "description": t.description,
                "status": t.status.as_str(),
                "owner": t.owner,
                "priority": t.priority,
                "dependencies": t.dependencies,
                "tags": t.tags,
                "result": t.result,
                "created_by": t.created_by,
                "claimed_at": t.claimed_at.map(|d| d.to_rfc3339()),
                "completed_at": t.completed_at.map(|d| d.to_rfc3339()),
                "created_at": t.created_at.to_rfc3339(),
                "updated_at": t.updated_at.to_rfc3339()
            }))
            .unwrap_or_default(),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get namespace Table of Contents: key names, sizes, tag groups, and 80-char previews — WITHOUT full values. Use this first, then memory_get for specific keys. Saves up to 95% tokens vs memory_list."
    )]
    async fn context_toc(
        &self,
        Parameters(ContextTocParams { namespace, format }): Parameters<ContextTocParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.get_namespace_toc(&namespace) {
            Ok(toc) => {
                let fmt = format.unwrap_or_else(|| "compact".to_string());
                if fmt == "json" {
                    return serde_json::to_string_pretty(&json!({
                        "namespace": toc.namespace,
                        "version": toc.version,
                        "entry_count": toc.entries.len(),
                        "total_bytes": toc.total_bytes,
                        "entries": toc.entries.iter().map(|e| json!({
                            "key": e.key,
                            "tags": e.tags,
                            "byte_len": e.byte_len,
                            "version": e.version,
                            "preview": e.preview
                        })).collect::<Vec<_>>()
                    }))
                    .unwrap_or_default();
                }

                let mut lines = vec![format!(
                    "# TOC: {namespace} | v{} | {} entries | {} bytes total\n",
                    toc.version,
                    toc.entries.len(),
                    toc.total_bytes
                )];
                for e in &toc.entries {
                    let tags = if e.tags.is_empty() {
                        String::new()
                    } else {
                        format!(" [{}]", e.tags.join(","))
                    };
                    lines.push(format!(
                        "  {} ({}B v{}){}  ↳ {}",
                        e.key, e.byte_len, e.version, tags, e.preview
                    ));
                }
                lines.push("\n[Use memory_get KEY to expand any entry]".to_string());
                lines.join("\n")
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get ONLY entries changed since version N. Use context_version first to know current version, then call this to see what changed. Returns changed keys + new values for true delta sync."
    )]
    async fn context_delta(
        &self,
        Parameters(ContextDeltaParams {
            namespace,
            since_version,
        }): Parameters<ContextDeltaParams>,
    ) -> String {
        let storage = self.storage.read().await;
        let current_version = storage.get_namespace_version(&namespace).unwrap_or(0);

        if since_version >= current_version {
            return format!(
                "No changes in [{namespace}] since v{since_version} (current: v{current_version})"
            );
        }

        match storage.get_version_delta(&namespace, since_version) {
            Ok(deltas) => {
                let mut seen_keys = std::collections::HashSet::new();
                let unique_deltas: Vec<_> = deltas
                    .into_iter()
                    .rev()
                    .filter(|ve| seen_keys.insert(ve.key.clone()))
                    .collect();

                let mut lines = vec![format!(
                    "# Delta [{namespace}] v{since_version} → v{current_version} ({} keys changed)\n",
                    unique_deltas.len()
                )];

                for ve in &unique_deltas {
                    let value_preview = storage
                        .get_memory(&namespace, &ve.key)
                        .ok()
                        .flatten()
                        .map(|e| e.as_summary(120))
                        .unwrap_or_else(|| "(deleted)".to_string());
                    lines.push(format!(
                        "  {} [v{}] by {} → {}",
                        ve.key, ve.version, ve.author, value_preview
                    ));
                }
                lines.join("\n")
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get the current version number of a namespace. Use this to track changes: save this version, do work, then use context_delta to see what changed."
    )]
    async fn context_version(
        &self,
        Parameters(ContextVersionParams { namespace }): Parameters<ContextVersionParams>,
    ) -> String {
        let storage = self.storage.read().await;
        match storage.get_namespace_version(&namespace) {
            Ok(v) => format!("Namespace [{namespace}] is at version {v}"),
            Err(e) => format!("ERROR: {e}"),
        }
    }

    #[tool(
        description = "Get write history for a specific key: who changed it, when, and at what versions. Useful for auditing and understanding how context evolved."
    )]
    async fn entry_history(
        &self,
        Parameters(EntryHistoryParams {
            namespace,
            key,
            limit,
        }): Parameters<EntryHistoryParams>,
    ) -> String {
        let limit = limit.unwrap_or(10).min(50);
        let storage = self.storage.read().await;
        match storage.get_entry_history(&namespace, &key, limit) {
            Ok(history) if history.is_empty() => format!("No history for [{namespace}:{key}]"),
            Ok(history) => {
                let lines: Vec<String> = history
                    .iter()
                    .map(|ve| {
                        format!(
                            "  v{} by {} at {} (hash:{})",
                            ve.version,
                            ve.author,
                            ve.ts.format("%Y-%m-%d %H:%M:%S UTC"),
                            ve.value_hash
                        )
                    })
                    .collect();
                format!(
                    "# History for [{namespace}:{key}] ({} versions)\n\n{}",
                    history.len(),
                    lines.join("\n")
                )
            }
            Err(e) => format!("ERROR: {e}"),
        }
    }
}

#[tool_handler]
impl ServerHandler for SharedMemoryServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build()).with_instructions(
            "Shared persistent memory for AI agent clusters. \
                RECOMMENDED WORKFLOW: \
                (1) context_toc — inspect namespace cheaply. \
                (2) context_delta — pull only changes since version N. \
                (3) stream_delta — get real-time findings from other agents. \
                (4) task_next — grab next work unit. \
                (5) memory_set/get for key-value state. \
                (6) context_store for large blobs (deduplication). \
                (7) snapshot_create before risky operations."
                .to_string(),
        )
    }
}
