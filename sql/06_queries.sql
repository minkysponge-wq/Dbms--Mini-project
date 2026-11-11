-- =====================================================================
-- 06_queries.sql - Data Manipulation Language showcase
--
-- Every query here answers a real operational question about the
-- cluster. They are grouped by the construct they demonstrate so the
-- report can cite them directly.
--
-- Run with:  mysql raftkv < sql/06_queries.sql
-- =====================================================================

USE raftkv;

-- =====================================================================
-- 1. JOIN QUERIES
-- =====================================================================

-- 1.1 Inner join: which node leads each term, and from where.
SELECT t.term_number, n.node_id, n.host_address, n.port, t.started_at
FROM term t
JOIN node n ON n.node_id = t.leader_id
ORDER BY t.term_number;

-- 1.2 Left join: every node with its vote in the newest term, including
-- nodes that did not vote at all (which is how you spot a partition).
SELECT n.node_id, n.role, n.current_term,
       v.candidate_id AS voted_for_candidate,
       COALESCE(v.granted, FALSE) AS granted
FROM node n
LEFT JOIN vote v
       ON v.voter_id = n.node_id
      AND v.term_number = (SELECT MAX(term_number) FROM term)
ORDER BY n.node_id;

-- 1.3 Self join: pairs of nodes that disagree at the same log index.
-- Under the log-matching property, any row here must be an uncommitted
-- entry; a committed one would be a safety violation.
SELECT a.node_id AS node_a, b.node_id AS node_b, a.log_index,
       a.term_number AS term_a, b.term_number AS term_b
FROM log_entry a
JOIN log_entry b
  ON b.log_index = a.log_index
 AND b.node_id > a.node_id
 AND b.term_number <> a.term_number
ORDER BY a.log_index;

-- 1.4 Three-table join through both bridge relations: who can do what.
SELECT u.username, r.role_name, p.permission_code
FROM app_user u
JOIN user_role ur       ON ur.user_id = u.user_id
JOIN app_role r         ON r.role_id = ur.role_id
JOIN role_permission rp ON rp.role_id = r.role_id
JOIN app_permission p   ON p.permission_id = rp.permission_id
ORDER BY u.username, p.permission_code;


-- =====================================================================
-- 2. NESTED SUBQUERIES
-- =====================================================================

-- 2.1 Nodes whose log is shorter than the cluster's longest.
SELECT node_id, COUNT(*) AS entries
FROM log_entry
GROUP BY node_id
HAVING COUNT(*) < (
    SELECT MAX(c) FROM (
        SELECT COUNT(*) AS c FROM log_entry GROUP BY node_id
    ) AS counts
);

-- 2.2 Keys written by the most recent committed entry on the leader.
SELECT key_name, value_text
FROM kv_state
WHERE node_id = (SELECT node_id FROM node WHERE role = 'leader' LIMIT 1)
  AND last_applied_index = (
      SELECT MAX(last_applied_index) FROM kv_state
      WHERE node_id = (SELECT node_id FROM node WHERE role = 'leader' LIMIT 1)
  );

-- 2.3 IN subquery: terms in which somebody voted but no leader emerged -
-- these are the split votes.
SELECT term_number, started_at
FROM term
WHERE leader_id IS NULL
  AND term_number IN (SELECT DISTINCT term_number FROM vote)
ORDER BY term_number;


-- =====================================================================
-- 3. CORRELATED SUBQUERIES
--    The inner query references the outer row, so it is re-evaluated
--    per row rather than once.
-- =====================================================================

-- 3.1 For each node, the entries it is missing relative to the leader.
-- This is the query a leader conceptually runs to decide what to send.
SELECT n.node_id,
       (SELECT COUNT(*)
          FROM log_entry ldr      -- not `lead`: reserved in MySQL 8 (LEAD())
         WHERE ldr.node_id = (SELECT node_id FROM node WHERE role='leader' LIMIT 1)
           AND ldr.log_index NOT IN (
               SELECT f.log_index FROM log_entry f WHERE f.node_id = n.node_id
           )
       ) AS entries_missing
FROM node n
WHERE n.role <> 'leader'
ORDER BY entries_missing DESC;

-- 3.2 Nodes that have voted in every term recorded so far.
SELECT n.node_id, n.role
FROM node n
WHERE NOT EXISTS (
    SELECT 1 FROM term t
    WHERE NOT EXISTS (
        SELECT 1 FROM vote v
        WHERE v.term_number = t.term_number AND v.voter_id = n.node_id
    )
    AND t.term_number > 0
);

-- 3.3 Each node's newest audit event, correlated on the node.
SELECT n.node_id, n.role,
       (SELECT e.description FROM node_event e
         WHERE e.node_id = n.node_id
         ORDER BY e.occurred_at DESC LIMIT 1) AS latest_event
FROM node n
ORDER BY n.node_id;


-- =====================================================================
-- 4. RECURSIVE QUERIES
-- =====================================================================

-- 4.1 The role hierarchy, walked from each role up to its root. This is
-- the recursion that powers v_role_effective_permission.
WITH RECURSIVE role_path (role_id, role_name, ancestor_id, ancestor_name, depth) AS (
    SELECT r.role_id, r.role_name, r.role_id, r.role_name, 0
    FROM app_role r
    UNION ALL
    SELECT rp.role_id, rp.role_name, parent.role_id, parent.role_name, rp.depth + 1
    FROM role_path rp
    JOIN app_role child  ON child.role_id = rp.ancestor_id
    JOIN app_role parent ON parent.role_id = child.parent_role_id
    WHERE rp.depth < 16
)
SELECT role_name, depth, ancestor_name AS inherits_from
FROM role_path
WHERE depth > 0
ORDER BY role_name, depth;

-- 4.2 Leadership succession chain: follow leadership forward from the
-- first elected term.
WITH RECURSIVE succession (term_number, leader_id, hops) AS (
    SELECT MIN(term_number), NULL, 0 FROM term WHERE leader_id IS NOT NULL
    UNION ALL
    SELECT t.term_number, t.leader_id, s.hops + 1
    FROM succession s
    JOIN term t ON t.term_number = s.term_number + 1
    WHERE s.hops < 64
)
SELECT term_number, leader_id, hops FROM succession WHERE hops > 0;


-- =====================================================================
-- 5. SET OPERATIONS
-- =====================================================================

-- 5.1 UNION: one feed combining role changes and commit advances.
SELECT 'role'   AS kind, node_id, description, occurred_at
FROM node_event WHERE event_type = 'role_change'
UNION
SELECT 'commit' AS kind, node_id, description, occurred_at
FROM node_event WHERE event_type = 'commit_advance'
ORDER BY occurred_at DESC
LIMIT 20;

-- 5.2 Difference, expressed with NOT IN: log indexes present on node 0
-- but absent on node 3. MySQL has no EXCEPT, so this is the idiom.
SELECT log_index, term_number, key_name
FROM log_entry
WHERE node_id = 0
  AND log_index NOT IN (SELECT log_index FROM log_entry WHERE node_id = 3)
ORDER BY log_index;

-- 5.3 Intersection, expressed with IN: indexes both nodes agree exist.
SELECT log_index, key_name
FROM log_entry
WHERE node_id = 0
  AND log_index IN (SELECT log_index FROM log_entry WHERE node_id = 1)
ORDER BY log_index;


-- =====================================================================
-- 6. AGGREGATES, GROUPING AND WINDOW FUNCTIONS
-- =====================================================================

-- 6.1 Vote tally per term, with the winner flagged.
SELECT v.term_number,
       v.candidate_id,
       COUNT(*) AS votes_received,
       fn_majority_size() AS majority_needed,
       CASE WHEN COUNT(*) >= fn_majority_size()
            THEN 'elected' ELSE 'insufficient' END AS outcome
FROM vote v
WHERE v.granted = TRUE
GROUP BY v.term_number, v.candidate_id
ORDER BY v.term_number, votes_received DESC;

-- 6.2 Running total of committed entries over time, per node.
SELECT node_id, log_index, key_name,
       COUNT(*) OVER (PARTITION BY node_id ORDER BY log_index) AS running_entries,
       LAG(term_number) OVER (PARTITION BY node_id ORDER BY log_index) AS prev_term,
       term_number,
       CASE WHEN LAG(term_number) OVER (PARTITION BY node_id ORDER BY log_index)
                 <> term_number
            THEN 'term boundary' ELSE '' END AS note
FROM log_entry
ORDER BY node_id, log_index;

-- 6.3 Per-node commit progress against the cluster maximum.
SELECT * FROM v_cluster_status ORDER BY node_id;

-- 6.4 Which log indexes have reached a quorum.
SELECT * FROM v_commit_quorum ORDER BY log_index;


-- =====================================================================
-- 7. FULL-TEXT SEARCH
-- =====================================================================

-- 7.1 Natural-language search over the audit log.
SELECT node_id, event_type, description,
       MATCH(description) AGAINST('leader election term' IN NATURAL LANGUAGE MODE) AS relevance
FROM node_event
WHERE MATCH(description) AGAINST('leader election term' IN NATURAL LANGUAGE MODE)
ORDER BY relevance DESC
LIMIT 10;

-- 7.2 Boolean mode: crashes but not restarts.
SELECT node_id, description, occurred_at
FROM node_event
WHERE MATCH(description) AGAINST('+crashed -restarted' IN BOOLEAN MODE)
ORDER BY occurred_at DESC;

-- 7.3 Search the contents of the key-value store itself.
SELECT node_id, key_name, value_text
FROM kv_state
WHERE MATCH(value_text) AGAINST('raft consensus' IN NATURAL LANGUAGE MODE);


-- =====================================================================
-- 8. UPDATE AND DELETE
-- =====================================================================

-- 8.1 Revoke every expired login session.
UPDATE auth_session
   SET revoked = TRUE
 WHERE expires_at < NOW(6) AND revoked = FALSE;

-- 8.2 Retire clients that opened over a day ago and never sent a
-- request. The foreign keys cascade the deletion to every node's
-- client_session row for that client.
DELETE FROM client
 WHERE client_id NOT IN (SELECT DISTINCT client_id FROM client_request)
   AND opened_at < NOW(6) - INTERVAL 1 DAY;

-- 8.3 Mark every entry at or below the leader's commit index committed.
UPDATE log_entry le
  JOIN node n ON n.node_id = le.node_id
   SET le.committed = TRUE
 WHERE le.log_index <= n.commit_index
   AND le.committed = FALSE;


-- =====================================================================
-- 9. INSERT
--
-- Wrapped in a transaction and rolled back at the end, so running this
-- file shows every form of INSERT without changing the seeded roles.
-- The application's own writes use the same forms: see sp_append_entry
-- and sp_apply_committed in 04_procedures.sql, and raft/store.py.
-- =====================================================================

START TRANSACTION;

-- 9.1 Single-row INSERT: a new permission.
INSERT INTO app_permission (permission_code, description)
VALUES ('report.export', 'Export the audit log as a report');

-- 9.2 Multi-row INSERT.
INSERT INTO app_permission (permission_code, description) VALUES
    ('report.schedule', 'Schedule a recurring report'),
    ('report.share',    'Share a report with another user');

-- 9.3 INSERT ... SELECT: grant every report.* permission to admin.
INSERT INTO role_permission (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM app_role r
JOIN app_permission p ON p.permission_code LIKE 'report.%'
WHERE r.role_name = 'admin';

-- 9.4 INSERT IGNORE: re-running a grant is a no-op, not an error,
-- because (role_id, permission_id) is the primary key.
INSERT IGNORE INTO role_permission (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM app_role r
JOIN app_permission p ON p.permission_code = 'report.export'
WHERE r.role_name = 'admin';

-- 9.5 Upsert with INSERT ... ON DUPLICATE KEY UPDATE: permission_code is
-- unique, so this updates the existing row's description.
INSERT INTO app_permission (permission_code, description)
VALUES ('report.export', 'Export the audit log as CSV or PDF')
ON DUPLICATE KEY UPDATE description = VALUES(description);

-- Evidence: admin now resolves the new permissions through the view.
SELECT permission_code, description
FROM app_permission
WHERE permission_code LIKE 'report.%'
ORDER BY permission_code;

SELECT COUNT(*) AS admin_permissions_inside_transaction
FROM v_role_effective_permission v
JOIN app_role r ON r.role_id = v.role_id
WHERE r.role_name = 'admin';

ROLLBACK;
