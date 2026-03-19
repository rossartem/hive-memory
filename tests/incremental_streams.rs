use tempfile::tempdir;
use hive_memory::storage::Storage;

#[tokio::test]
async fn test_incremental_streams_multi_agent() {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let ns = "test_project_streams";

    // Publish 2 initial events
    storage.stream_publish(ns.to_string(), "agent_sec".to_string(), "alert".to_string(), "vuln 1".to_string(), vec![]).unwrap();
    storage.stream_publish(ns.to_string(), "agent_sec".to_string(), "alert".to_string(), "vuln 2".to_string(), vec![]).unwrap();

    // Agent QA joins, reads 2 events
    let (evs_qa, cursor_qa1) = storage.stream_read_delta(ns, "agent_qa").unwrap();
    assert_eq!(evs_qa.len(), 2, "Agent QA should see 2 historical events");
    assert_eq!(cursor_qa1, 2);

    // Publish 1 new event
    storage.stream_publish(ns.to_string(), "agent_admin".to_string(), "general".to_string(), "hello".to_string(), vec![]).unwrap();

    // Agent DEV joins fresh, should read all 3 events
    let (evs_dev, cursor_dev1) = storage.stream_read_delta(ns, "agent_dev").unwrap();
    assert_eq!(evs_dev.len(), 3, "Agent DEV should see all 3 events as it just joined");
    assert_eq!(cursor_dev1, 3);
    assert_eq!(evs_dev[2].payload, "hello");

    // Agent QA reads again, should only see 1 new event
    let (evs_qa2, cursor_qa2) = storage.stream_read_delta(ns, "agent_qa").unwrap();
    assert_eq!(evs_qa2.len(), 1, "Agent QA should only see the 1 new event since last read");
    assert_eq!(evs_qa2[0].payload, "hello");
    assert_eq!(cursor_qa2, 3);

    // Reading without any new events should return empty
    let (evs_qa3, _) = storage.stream_read_delta(ns, "agent_qa").unwrap();
    assert_eq!(evs_qa3.len(), 0, "Agent QA should see 0 new events");
}
