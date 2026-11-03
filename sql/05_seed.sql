-- =====================================================================
-- 05_seed.sql - Data Manipulation Language: reference data
--
-- Seeds the five cluster nodes and the access-control model.
--
-- The role hierarchy is deliberately three levels deep so that the
-- recursive CTE in v_role_effective_permission has something real to
-- climb:
--
--        admin          can do everything an operator can, plus
--          |            manage nodes and users
--       operator        can do everything a viewer can, plus write keys
--          |
--        viewer         read keys and inspect cluster state
--
-- Demo passwords are admin123 / operator123 / viewer123, stored as
-- bcrypt hashes. They are development credentials and must be changed
-- before any real deployment.
-- =====================================================================

USE raftkv;

SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE node_event;
TRUNCATE TABLE auth_session;
TRUNCATE TABLE role_permission;
TRUNCATE TABLE user_role;
TRUNCATE TABLE app_permission;
TRUNCATE TABLE app_role;
TRUNCATE TABLE app_user;
TRUNCATE TABLE client_request;
TRUNCATE TABLE client_session;
TRUNCATE TABLE client;
TRUNCATE TABLE kv_state;
TRUNCATE TABLE replication_state;
TRUNCATE TABLE snapshot;
TRUNCATE TABLE log_entry;
TRUNCATE TABLE vote;
TRUNCATE TABLE term;
TRUNCATE TABLE node;
SET FOREIGN_KEY_CHECKS = 1;

-- ---------------------------------------------------------------------
-- The cluster. Five nodes tolerate two simultaneous failures.
-- ---------------------------------------------------------------------
INSERT INTO node (node_id, host_address, port, role, current_term,
                  commit_index, last_applied, is_alive) VALUES
    (0, '127.0.0.1', 9000, 'follower', 0, 0, 0, TRUE),
    (1, '127.0.0.1', 9001, 'follower', 0, 0, 0, TRUE),
    (2, '127.0.0.1', 9002, 'follower', 0, 0, 0, TRUE),
    (3, '127.0.0.1', 9003, 'follower', 0, 0, 0, TRUE),
    (4, '127.0.0.1', 9004, 'follower', 0, 0, 0, TRUE);

-- Term 0 exists so that the sentinel log entry has a term to reference.
INSERT INTO term (term_number, leader_id) VALUES (0, NULL);

-- ---------------------------------------------------------------------
-- Permissions
-- ---------------------------------------------------------------------
INSERT INTO app_permission (permission_code, description) VALUES
    ('kv.read',       'Read keys from the replicated store'),
    ('kv.write',      'Create, update and delete keys'),
    ('cluster.view',  'View node roles, terms and replication status'),
    ('cluster.admin', 'Force elections and reconfigure the cluster'),
    ('node.kill',     'Crash a running node'),
    ('node.restart',  'Restart a crashed node from stable storage'),
    ('user.manage',   'Create users and assign roles'),
    ('event.read',    'Read the cluster event log');

-- ---------------------------------------------------------------------
-- Roles, as a hierarchy. parent_role_id is the self-referencing foreign
-- key the recursive CTE walks.
-- ---------------------------------------------------------------------
INSERT INTO app_role (role_name, description, parent_role_id) VALUES
    ('viewer', 'Read-only access to keys and cluster state', NULL);

INSERT INTO app_role (role_name, description, parent_role_id)
SELECT 'operator', 'Can write keys; inherits viewer', role_id
FROM app_role WHERE role_name = 'viewer';

INSERT INTO app_role (role_name, description, parent_role_id)
SELECT 'admin', 'Full cluster control; inherits operator', role_id
FROM app_role WHERE role_name = 'operator';

-- Direct grants only. Everything else is inherited through the
-- hierarchy, which is the point of the recursive view.
INSERT INTO role_permission (role_id, permission_id)
SELECT r.role_id, p.permission_id
FROM app_role r JOIN app_permission p
WHERE (r.role_name = 'viewer'
       AND p.permission_code IN ('kv.read', 'cluster.view', 'event.read'))
   OR (r.role_name = 'operator'
       AND p.permission_code IN ('kv.write'))
   OR (r.role_name = 'admin'
       AND p.permission_code IN ('cluster.admin', 'node.kill',
                                 'node.restart', 'user.manage'));

-- ---------------------------------------------------------------------
-- Users
-- ---------------------------------------------------------------------
INSERT INTO app_user (username, email, password_hash) VALUES
    ('admin',    'admin@raftkv.local',
     '$2b$10$Q.f.7mK.4cFhVdJuqy7ZNOmFd5wnjR54DCRFd5D4u6SkaDFvoXTRu'),
    ('operator', 'operator@raftkv.local',
     '$2b$10$jrKNTnwqcYHzAPyCdeSnt.oeuI98WGQOB0N4NXECrgFkda2TgEOuG'),
    ('viewer',   'viewer@raftkv.local',
     '$2b$10$OnggmsOzSdhFFoiO.jNTVuzVm727Ymj2SKbIKd3IF/AyDgjQVytNu');

INSERT INTO user_role (user_id, role_id)
SELECT u.user_id, r.role_id
FROM app_user u JOIN app_role r ON r.role_name = u.username;
