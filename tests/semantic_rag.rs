use chrono::Utc;
use hive_memory::storage::{MemoryEntry, Storage};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_semantic_rag_robustness() {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let ns = "test_project_rag_robust";

    // 1. Insert documents
    let docs = vec![
        ("doc_auth", "We use JWT tokens (Bearer) to authenticate users with our Axum API.", vec!["auth".to_string()]),
        ("doc_db", "The system relies on PostgreSQL for persistence and Redis for caching.", vec!["db".to_string()]),
        ("doc_k8s", "Deployment to Kubernetes uses Helm charts scaling pod replicas via HPA.", vec!["infra".to_string()]),
    ];

    for (key, val, tags) in docs {
        let entry = MemoryEntry {
            id: uuid::Uuid::new_v4().to_string(),
            namespace: ns.to_string(),
            key: key.to_string(),
            value: val.to_string(),
            tags,
            agent_id: "agent_qa".to_string(),
            ttl_seconds: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            blob_ref: None,
            version: 0,
        };
        storage.set_memory(entry).unwrap();
    }

    // Since fastembed model loading and embedding calculation runs asynchronously in the background layer,
    // we need to wait / poll until the embeddings are persisted for RAG search to work.
    let mut hit = false;
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Search query implicitly looking for auth ("sign in")
        let results = storage.search_memories(ns, "how do people sign in to the system?", 3).await.unwrap();
        if !results.is_empty() {
            assert_eq!(results[0].key, "doc_auth", "The top result MUST be doc_auth");
            hit = true;
            break;
        }
    }

    assert!(hit, "Timeout waiting for RAG embeddings to generate");

    // Exact fallback tests (testing if fallback substring match works)
    let db_results = storage.search_memories(ns, "PostgreSQL", 1).await.unwrap();
    assert_eq!(db_results.len(), 1);
    assert_eq!(db_results[0].key, "doc_db", "Fallbacks should catch exact word 'PostgreSQL'");
}
