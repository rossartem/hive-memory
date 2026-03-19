use chrono::Utc;
use hive_memory::storage::{Storage, Task, TaskStatus};
use tempfile::tempdir;

#[tokio::test]
async fn task_coordination_dependencies_and_priority() {
    let dir = tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    let ns = "test_project_tasks";

    // Create Task A (Priority 1)
    let mut task_a = create_dummy_task(ns, "task_a", 1);
    task_a.dependencies = vec![];
    storage.task_create(task_a.clone()).unwrap();

    // Create Task B (Priority 10) depending on Task A
    let mut task_b = create_dummy_task(ns, "task_b", 10);
    task_b.dependencies = vec!["task_a".to_string()];
    storage.task_create(task_b.clone()).unwrap();

    // Create Task C (Priority 5) with no dependencies
    let mut task_c = create_dummy_task(ns, "task_c", 5);
    task_c.dependencies = vec![];
    storage.task_create(task_c.clone()).unwrap();

    // 1. Although Task B has the highest priority (10), it should be blocked by Task A.
    // Therefore, an agent calling `task_next` should get Task C (highest priority among unblocked, 5 > 1).
    let next_task = storage
        .task_next(ns, "agent_worker_1")
        .unwrap()
        .expect("Should have a task to claim");
    assert_eq!(next_task.id, "task_c", "Task C should be selected because B is blocked and C > A in priority");
    storage.task_claim(ns, &next_task.id, "agent_worker_1").unwrap();

    // 2. Next agent should get Task A
    let next_task = storage
        .task_next(ns, "agent_worker_2")
        .unwrap()
        .expect("Should have a task to claim");
    assert_eq!(next_task.id, "task_a", "Task A should be selected now");
    storage.task_claim(ns, &next_task.id, "agent_worker_2").unwrap();

    // 3. Next agent should get None because Task B is STILL blocked (A is claimed but not Done)
    let next_task = storage.task_next(ns, "agent_worker_3").unwrap();
    assert!(next_task.is_none(), "Task B is blocked, shouldn't return anything");

    // 4. agent_worker_2 completes Task A
    storage
        .task_update_status(ns, "task_a", "agent_worker_2", TaskStatus::Done, Some("Finished A".to_string()))
        .unwrap();

    // 5. Now Task B is unblocked, and can be claimed
    let next_task = storage
        .task_next(ns, "agent_worker_3")
        .unwrap()
        .expect("Should have a task to claim");
    assert_eq!(next_task.id, "task_b", "Task B is unblocked and claimable");
    storage.task_claim(ns, &next_task.id, "agent_worker_3").unwrap();
}

fn create_dummy_task(ns: &str, id: &str, priority: i32) -> Task {
    Task {
        id: id.to_string(),
        namespace: ns.to_string(),
        title: format!("Task {}", id),
        description: "description".to_string(),
        status: TaskStatus::Pending,
        owner: None,
        created_by: "agent_pm".to_string(),
        dependencies: vec![],
        tags: vec![],
        priority,
        result: None,
        claimed_at: None,
        completed_at: None,
        claim_ttl_seconds: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}
