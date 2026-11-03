-- =====================================================================
-- 02_views.sql - Views
--
-- Each view answers a question an operator genuinely asks about a Raft
-- cluster, and each one is built from a construct the course covers:
--
--   v_cluster_status            joins + window functions
--   v_commit_quorum             CTE implementing Raft's commit rule
--   v_replication_lag           window functions (RANK, lag arithmetic)
--   v_leader_timeline           window functions (LAG, LEAD)
--   v_log_divergence            correlated subquery
--   v_role_effective_permission RECURSIVE CTE over the role hierarchy
--   v_user_permission           the above, resolved per user
-- =====================================================================

USE raftkv;

DROP VIEW IF EXISTS v_user_permission;
DROP VIEW IF EXISTS v_role_effective_permission;
DROP VIEW IF EXISTS v_log_divergence;
DROP VIEW IF EXISTS v_leader_timeline;
DROP VIEW IF EXISTS v_replication_lag;
DROP VIEW IF EXISTS v_commit_quorum;
DROP VIEW IF EXISTS v_cluster_status;


-- ---------------------------------------------------------------------
-- Current state of every node, with its log length and how far behind
-- the cluster's furthest log it is.
--
-- MAX(...) OVER () is a window function over the whole result set: it
-- gives every row the cluster-wide maximum without collapsing the rows,
-- which a plain GROUP BY could not do.
-- ---------------------------------------------------------------------
CREATE VIEW v_cluster_status AS
SELECT
    n.node_id,
    n.host_address,
    n.port,
    n.role,
    n.current_term,
    n.voted_for,
    n.commit_index,
    n.last_applied,
    n.is_alive,
    COALESCE(l.entry_count, 0)                        AS log_length,
    COALESCE(l.last_index, 0)                         AS last_log_index,
    COALESCE(l.last_term, 0)                          AS last_log_term,
    MAX(COALESCE(l.last_index, 0)) OVER ()            AS cluster_max_index,
    MAX(COALESCE(l.last_index, 0)) OVER () - COALESCE(l.last_index, 0)
                                                      AS entries_behind,
    RANK() OVER (ORDER BY COALESCE(l.last_index, 0) DESC)
                                                      AS freshness_rank
FROM node n
LEFT JOIN (
    SELECT
        le.node_id,
        COUNT(*)            AS entry_count,
        MAX(le.log_index)   AS last_index,
        SUBSTRING_INDEX(
            GROUP_CONCAT(le.term_number ORDER BY le.log_index DESC), ',', 1
        )                   AS last_term
    FROM log_entry le
    GROUP BY le.node_id
) l ON l.node_id = n.node_id;


-- ---------------------------------------------------------------------
-- Raft's commit rule, expressed as a query.
--
-- An entry is committed once a majority of nodes have stored it. This
-- view computes, for every log index, how many nodes hold it and whether
-- that constitutes a majority. The leader's commitIndex is simply the
-- highest index where is_majority_replicated is true.
-- ---------------------------------------------------------------------
CREATE VIEW v_commit_quorum AS
WITH cluster AS (
    SELECT COUNT(*) AS cluster_size FROM node
),
replication AS (
    SELECT
        le.log_index,
        COUNT(DISTINCT le.node_id) AS replica_count,
        MIN(le.term_number)        AS term_number
    FROM log_entry le
    GROUP BY le.log_index
)
SELECT
    r.log_index,
    r.term_number,
    r.replica_count,
    c.cluster_size,
    FLOOR(c.cluster_size / 2) + 1                                AS majority_needed,
    (r.replica_count >= FLOOR(c.cluster_size / 2) + 1)           AS is_majority_replicated
FROM replication r
CROSS JOIN cluster c;


-- ---------------------------------------------------------------------
-- How far behind each follower is, from the leader's point of view.
-- ---------------------------------------------------------------------
CREATE VIEW v_replication_lag AS
SELECT
    rs.leader_id,
    rs.follower_id,
    rs.next_index,
    rs.match_index,
    leader_log.last_index                           AS leader_last_index,
    leader_log.last_index - rs.match_index          AS entries_behind,
    rs.last_contact,
    RANK() OVER (PARTITION BY rs.leader_id
                 ORDER BY rs.match_index DESC)      AS catch_up_rank,
    AVG(rs.match_index) OVER (PARTITION BY rs.leader_id)
                                                    AS avg_match_index
FROM replication_state rs
JOIN (
    SELECT node_id, COALESCE(MAX(log_index), 0) AS last_index
    FROM log_entry
    GROUP BY node_id
) leader_log ON leader_log.node_id = rs.leader_id;


-- ---------------------------------------------------------------------
-- Leadership history. LAG and LEAD let each term see its neighbours, so
-- we can show how long a leader held office and who replaced it. Terms
-- with no leader are split votes.
-- ---------------------------------------------------------------------
CREATE VIEW v_leader_timeline AS
SELECT
    t.term_number,
    t.leader_id,
    t.started_at,
    LAG(t.leader_id)  OVER (ORDER BY t.term_number) AS previous_leader,
    LEAD(t.leader_id) OVER (ORDER BY t.term_number) AS next_leader,
    TIMESTAMPDIFF(
        MICROSECOND,
        t.started_at,
        LEAD(t.started_at) OVER (ORDER BY t.term_number)
    ) / 1000.0                                      AS held_for_ms,
    CASE WHEN t.leader_id IS NULL THEN 'split vote - no leader elected'
         WHEN LAG(t.leader_id) OVER (ORDER BY t.term_number) = t.leader_id
              THEN 'leader retained office'
         ELSE 'leadership changed hands'
    END                                             AS transition
FROM term t;


-- ---------------------------------------------------------------------
-- Log divergence: entries that exist on one node but are missing, or
-- differ, on another. Uses a correlated subquery - the inner query
-- references the outer row's node and index.
--
-- Under the log-matching property, any divergence here must be confined
-- to uncommitted entries; a committed entry appearing in this view would
-- be a genuine safety violation.
-- ---------------------------------------------------------------------
CREATE VIEW v_log_divergence AS
SELECT
    a.node_id        AS node_a,
    b.node_id        AS node_b,
    a.log_index,
    a.term_number    AS term_a,
    b.term_number    AS term_b,
    a.committed      AS committed_a
FROM log_entry a
JOIN log_entry b
  ON b.log_index = a.log_index
 AND b.node_id   > a.node_id
WHERE b.term_number <> a.term_number
   OR EXISTS (
        SELECT 1
        FROM log_entry x
        WHERE x.node_id   = a.node_id
          AND x.log_index = a.log_index
          AND x.key_name IS NOT NULL
          AND x.key_name <> COALESCE(b.key_name, '')
      );


-- ---------------------------------------------------------------------
-- Effective permissions of each role, including everything inherited
-- from its ancestors.
--
-- RECURSIVE CTE. app_role.parent_role_id makes the roles a hierarchy -
-- admin inherits operator, operator inherits viewer - so a role's real
-- permission set is its own grants plus every ancestor's. The anchor
-- member seeds each role as its own ancestor at depth 0; the recursive
-- member climbs one parent per iteration until it reaches a root.
-- ---------------------------------------------------------------------
CREATE VIEW v_role_effective_permission AS
WITH RECURSIVE role_ancestry (base_role_id, ancestor_role_id, depth) AS (
    -- Anchor: every role is its own ancestor at depth zero.
    SELECT r.role_id, r.role_id, 0
    FROM app_role r

    UNION ALL

    -- Recursive step: climb to the parent of the current ancestor.
    SELECT ra.base_role_id, r.parent_role_id, ra.depth + 1
    FROM role_ancestry ra
    JOIN app_role r ON r.role_id = ra.ancestor_role_id
    WHERE r.parent_role_id IS NOT NULL
      AND ra.depth < 16              -- guards against a cyclic hierarchy
)
SELECT DISTINCT
    base.role_name                  AS role_name,
    ra.base_role_id                 AS role_id,
    p.permission_code,
    p.description                   AS permission_description,
    ra.depth                        AS inherited_from_depth,
    anc.role_name                   AS granted_by_role
FROM role_ancestry ra
JOIN app_role base      ON base.role_id = ra.base_role_id
JOIN app_role anc       ON anc.role_id  = ra.ancestor_role_id
JOIN role_permission rp ON rp.role_id   = ra.ancestor_role_id
JOIN app_permission p   ON p.permission_id = rp.permission_id;


-- ---------------------------------------------------------------------
-- The permission set of each user, resolved through their roles and the
-- role hierarchy above. This is the view the API consults on every
-- privileged request.
-- ---------------------------------------------------------------------
CREATE VIEW v_user_permission AS
SELECT DISTINCT
    u.user_id,
    u.username,
    rep.role_name,
    rep.permission_code
FROM app_user u
JOIN user_role ur               ON ur.user_id = u.user_id
JOIN app_role r                 ON r.role_id  = ur.role_id
JOIN v_role_effective_permission rep ON rep.role_id = r.role_id
WHERE u.is_active = TRUE;
