-- =====================================================================
-- Fault-Tolerant Distributed Key-Value Store using the Raft Consensus
-- Algorithm
--
-- 01_schema.sql - Data Definition Language
--
-- Creates every base relation, together with its primary keys, foreign
-- keys, unique constraints, check constraints and indexes.
--
-- The relations fall into four groups:
--   (a) Raft protocol state   - node, term, vote, log_entry,
--                               replication_state, snapshot
--   (b) Applied state machine - kv_state, client_session, client_request
--   (c) Access control        - app_user, app_role, app_permission and
--                               the two bridge relations
--   (d) Auditing              - node_event
--
-- In a production deployment each node would own a private database.
-- Here every node's state lives in one schema with node_id forming part
-- of each key, exactly as the ER model specifies. That keeps the design
-- faithful and lets us write analytical queries *across* the cluster,
-- which is what the views in 02_views.sql do.
--
-- Tested on MariaDB 10.11 and MySQL 8.0.
-- =====================================================================

DROP DATABASE IF EXISTS raftkv;
CREATE DATABASE raftkv
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;
USE raftkv;

-- ---------------------------------------------------------------------
-- (a) Raft protocol state
-- ---------------------------------------------------------------------

-- A server in the cluster.
--
-- voted_for is a self-referencing foreign key: a node votes for another
-- node. This is one of the two recursive relationships in the model.
CREATE TABLE node (
    node_id        INT UNSIGNED    NOT NULL,
    host_address   VARCHAR(45)     NOT NULL,
    port           SMALLINT UNSIGNED NOT NULL,
    role           ENUM('follower','candidate','leader')
                                   NOT NULL DEFAULT 'follower',
    current_term   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    voted_for      INT UNSIGNED    NULL,
    commit_index   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_applied   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    is_alive       BOOLEAN         NOT NULL DEFAULT TRUE,
    last_heartbeat TIMESTAMP(6)    NULL,
    updated_at     TIMESTAMP(6)    NOT NULL
                   DEFAULT CURRENT_TIMESTAMP(6)
                   ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_node PRIMARY KEY (node_id),
    CONSTRAINT uq_node_endpoint UNIQUE (host_address, port),
    CONSTRAINT fk_node_voted_for
        FOREIGN KEY (voted_for) REFERENCES node (node_id)
        ON DELETE SET NULL ON UPDATE CASCADE,
    -- A node can never have applied an entry it has not yet committed.
    CONSTRAINT chk_node_applied_le_commit
        CHECK (last_applied <= commit_index),
    CONSTRAINT chk_node_port CHECK (port BETWEEN 1 AND 65535)
) ENGINE = InnoDB;


-- A logical clock interval. Terms increase monotonically and each term
-- has at most one leader, which is what the vote rules guarantee.
CREATE TABLE term (
    term_number  BIGINT UNSIGNED NOT NULL,
    leader_id    INT UNSIGNED    NULL,
    started_at   TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ended_at     TIMESTAMP(6)    NULL,

    CONSTRAINT pk_term PRIMARY KEY (term_number),
    CONSTRAINT fk_term_leader
        FOREIGN KEY (leader_id) REFERENCES node (node_id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE = InnoDB;


-- A vote cast by one node, in one term, for one candidate.
--
-- The composite primary key (term_number, voter_id) is the single most
-- important constraint in this schema. It makes it structurally
-- impossible for a node to vote twice in the same term, which is what
-- guarantees Raft's election-safety property: at most one leader can be
-- elected per term. The rule is enforced by the database, not by an
-- if-statement in application code.
CREATE TABLE vote (
    term_number   BIGINT UNSIGNED NOT NULL,
    voter_id      INT UNSIGNED    NOT NULL,
    candidate_id  INT UNSIGNED    NOT NULL,
    granted       BOOLEAN         NOT NULL,
    -- What the candidate claimed about its own log when it asked. The
    -- vote trigger compares these against the voter's actual log to
    -- enforce Raft's election restriction (paper section 5.4.1).
    candidate_last_index BIGINT UNSIGNED NOT NULL DEFAULT 0,
    candidate_last_term  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    voted_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_vote PRIMARY KEY (term_number, voter_id),
    CONSTRAINT fk_vote_term
        FOREIGN KEY (term_number) REFERENCES term (term_number)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_vote_voter
        FOREIGN KEY (voter_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_vote_candidate
        FOREIGN KEY (candidate_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;

CREATE INDEX idx_vote_candidate ON vote (candidate_id, term_number);


-- One entry in a node's replicated log.
--
-- A weak entity: its identity depends on the owning node, because every
-- node holds its own independent copy of the log. The same log_index on
-- two different nodes may legitimately hold different entries until the
-- logs converge.
CREATE TABLE log_entry (
    node_id      INT UNSIGNED    NOT NULL,
    log_index    BIGINT UNSIGNED NOT NULL,
    term_number  BIGINT UNSIGNED NOT NULL,
    command_type ENUM('PUT','APPEND','DELETE','GET','NOOP') NOT NULL,
    key_name     VARCHAR(255)    NULL,
    value_text   TEXT            NULL,
    client_id    VARCHAR(64)     NULL,
    seq_no       BIGINT UNSIGNED NULL,
    committed    BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_log_entry PRIMARY KEY (node_id, log_index),
    CONSTRAINT fk_log_entry_node
        FOREIGN KEY (node_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_log_entry_term
        FOREIGN KEY (term_number) REFERENCES term (term_number)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    -- A write command must name the key it writes.
    CONSTRAINT chk_log_entry_key_present
        CHECK (command_type = 'NOOP' OR key_name IS NOT NULL)
) ENGINE = InnoDB;

CREATE INDEX idx_log_entry_client ON log_entry (client_id, seq_no);

-- A dedicated index for the term_number foreign key.
--
-- InnoDB requires an index on every foreign key column and will adopt
-- any index whose leading column fits. Without this one it adopts the
-- composite (term_number, committed) index that demos.py creates - and
-- then refuses to drop it, because a constraint now depends on it.
-- Giving the constraint its own index keeps the demo index free to come
-- and go.
CREATE INDEX idx_log_entry_term ON log_entry (term_number);

-- Deliberately NOT indexed on (term_number, committed). The query
-- optimization demo adds that index and shows the EXPLAIN plan change
-- from a full scan to a range scan.


-- Per-follower replication progress, tracked by the current leader.
--
-- This resolves the many-to-many "tracks" relationship whose two
-- participating entity sets are both NODE: leader_id and follower_id are
-- distinct foreign keys into the same relation. Rows are discarded and
-- rebuilt after every election, because a new leader knows nothing about
-- the previous leader's progress.
CREATE TABLE replication_state (
    leader_id    INT UNSIGNED    NOT NULL,
    follower_id  INT UNSIGNED    NOT NULL,
    next_index   BIGINT UNSIGNED NOT NULL DEFAULT 1,
    match_index  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_contact TIMESTAMP(6)    NULL,

    CONSTRAINT pk_replication_state PRIMARY KEY (leader_id, follower_id),
    CONSTRAINT fk_repl_leader
        FOREIGN KEY (leader_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_repl_follower
        FOREIGN KEY (follower_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    -- "A leader does not replicate to itself" would naturally be a CHECK
    -- here, but MariaDB (which XAMPP ships) rejects CHECK constraints on
    -- columns carrying a foreign key with ON UPDATE CASCADE. The rule is
    -- enforced by trg_replication_state_bi in 03_triggers.sql instead, so
    -- the schema loads identically on MySQL 8 and MariaDB 10.
    CONSTRAINT chk_repl_match_lt_next CHECK (match_index < next_index)
) ENGINE = InnoDB;


-- A compacted checkpoint of the state machine up to a given log index.
CREATE TABLE snapshot (
    snapshot_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    node_id             INT UNSIGNED    NOT NULL,
    last_included_index BIGINT UNSIGNED NOT NULL,
    last_included_term  BIGINT UNSIGNED NOT NULL,
    state_blob          LONGTEXT        NULL,
    created_at          TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_snapshot PRIMARY KEY (snapshot_id),
    CONSTRAINT uq_snapshot_node_index UNIQUE (node_id, last_included_index),
    CONSTRAINT fk_snapshot_node
        FOREIGN KEY (node_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;


-- ---------------------------------------------------------------------
-- (b) Applied state machine
-- ---------------------------------------------------------------------

-- The key-value pairs produced by applying committed log entries in
-- index order. Also a weak entity: each node applies its own copy.
CREATE TABLE kv_state (
    node_id            INT UNSIGNED    NOT NULL,
    key_name           VARCHAR(255)    NOT NULL,
    value_text         TEXT            NOT NULL,
    last_applied_index BIGINT UNSIGNED NOT NULL,
    owner_user_id      INT UNSIGNED    NULL,
    updated_at         TIMESTAMP(6)    NOT NULL
                       DEFAULT CURRENT_TIMESTAMP(6)
                       ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_kv_state PRIMARY KEY (node_id, key_name),
    CONSTRAINT fk_kv_state_node
        FOREIGN KEY (node_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;

-- Full-text index over stored values, used by the search feature in
-- 05_queries.sql.
CREATE FULLTEXT INDEX ftx_kv_state_value ON kv_state (value_text);


-- Client identity. Cluster-wide: a client is one logical caller,
-- independent of which replica happens to serve it.
CREATE TABLE client (
    client_id  VARCHAR(64)  NOT NULL,
    user_id    INT UNSIGNED NULL,
    opened_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_client PRIMARY KEY (client_id)
) ENGINE = InnoDB;


-- Per-replica deduplication state: the highest sequence number this
-- node has applied for a client, and the result it produced.
--
-- A weak entity owned by NODE, exactly like kv_state, and for the same
-- reason: the session table is part of the replicated state machine, so
-- every node maintains its own copy by applying the same log. Keying it
-- on client_id alone would be a bug - the first node to apply a command
-- would mark the sequence number seen, and every other node would then
-- treat that command as a duplicate and skip it, so the replicas would
-- silently diverge.
CREATE TABLE client_session (
    node_id       INT UNSIGNED    NOT NULL,
    client_id     VARCHAR(64)     NOT NULL,
    last_seq_seen BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_result   TEXT            NULL,
    updated_at    TIMESTAMP(6)    NOT NULL
                  DEFAULT CURRENT_TIMESTAMP(6)
                  ON UPDATE CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_client_session PRIMARY KEY (node_id, client_id),
    CONSTRAINT fk_client_session_node
        FOREIGN KEY (node_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_client_session_client
        FOREIGN KEY (client_id) REFERENCES client (client_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;


-- One client command, its assigned log index once appended, and its
-- outcome.
CREATE TABLE client_request (
    request_id     VARCHAR(64)     NOT NULL,
    client_id      VARCHAR(64)     NOT NULL,
    seq_no         BIGINT UNSIGNED NOT NULL,
    command_type   ENUM('PUT','APPEND','DELETE','GET','NOOP') NOT NULL,
    key_name       VARCHAR(255)    NULL,
    value_text     TEXT            NULL,
    assigned_index BIGINT UNSIGNED NULL,
    status         ENUM('pending','committed','applied','failed','duplicate')
                                   NOT NULL DEFAULT 'pending',
    submitted_at   TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at   TIMESTAMP(6)    NULL,

    CONSTRAINT pk_client_request PRIMARY KEY (request_id),
    CONSTRAINT uq_client_request_seq UNIQUE (client_id, seq_no),
    CONSTRAINT fk_client_request_client
        FOREIGN KEY (client_id) REFERENCES client (client_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;


-- ---------------------------------------------------------------------
-- (c) Authentication and role-based access control
-- ---------------------------------------------------------------------

-- app_role carries a self-referencing parent_role_id, which makes the
-- role set a hierarchy: admin inherits operator, operator inherits
-- viewer. Computing the effective permissions of a role therefore means
-- walking that hierarchy, which is what the recursive CTE in
-- 02_views.sql does.
CREATE TABLE app_role (
    role_id        INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    role_name      VARCHAR(32)     NOT NULL,
    description    VARCHAR(255)    NULL,
    parent_role_id INT UNSIGNED    NULL,

    CONSTRAINT pk_app_role PRIMARY KEY (role_id),
    CONSTRAINT uq_app_role_name UNIQUE (role_name),
    CONSTRAINT fk_app_role_parent
        FOREIGN KEY (parent_role_id) REFERENCES app_role (role_id)
        ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE = InnoDB;


CREATE TABLE app_permission (
    permission_id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
    permission_code VARCHAR(64)  NOT NULL,
    description     VARCHAR(255) NULL,

    CONSTRAINT pk_app_permission PRIMARY KEY (permission_id),
    CONSTRAINT uq_app_permission_code UNIQUE (permission_code)
) ENGINE = InnoDB;


CREATE TABLE app_user (
    user_id       INT UNSIGNED NOT NULL AUTO_INCREMENT,
    username      VARCHAR(64)  NOT NULL,
    email         VARCHAR(255) NOT NULL,
    password_hash CHAR(60)     NOT NULL,   -- bcrypt
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_login_at TIMESTAMP(6) NULL,

    CONSTRAINT pk_app_user PRIMARY KEY (user_id),
    CONSTRAINT uq_app_user_username UNIQUE (username),
    CONSTRAINT uq_app_user_email UNIQUE (email),
    CONSTRAINT chk_app_user_email CHECK (email LIKE '%@%.%')
) ENGINE = InnoDB;


-- Bridge relation: a user may hold several roles.
CREATE TABLE user_role (
    user_id    INT UNSIGNED NOT NULL,
    role_id    INT UNSIGNED NOT NULL,
    granted_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_user_role PRIMARY KEY (user_id, role_id),
    CONSTRAINT fk_user_role_user
        FOREIGN KEY (user_id) REFERENCES app_user (user_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_user_role_role
        FOREIGN KEY (role_id) REFERENCES app_role (role_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;


-- Bridge relation: a role grants several permissions.
CREATE TABLE role_permission (
    role_id       INT UNSIGNED NOT NULL,
    permission_id INT UNSIGNED NOT NULL,

    CONSTRAINT pk_role_permission PRIMARY KEY (role_id, permission_id),
    CONSTRAINT fk_role_permission_role
        FOREIGN KEY (role_id) REFERENCES app_role (role_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_role_permission_permission
        FOREIGN KEY (permission_id) REFERENCES app_permission (permission_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;


-- Issued login sessions. Named auth_session to keep it distinct from
-- client_session, which is Raft's request-deduplication table.
CREATE TABLE auth_session (
    token_hash CHAR(64)     NOT NULL,       -- sha256 of the bearer token
    user_id    INT UNSIGNED NOT NULL,
    issued_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at TIMESTAMP(6) NOT NULL,
    revoked    BOOLEAN      NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_auth_session PRIMARY KEY (token_hash),
    CONSTRAINT fk_auth_session_user
        FOREIGN KEY (user_id) REFERENCES app_user (user_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT chk_auth_session_window CHECK (expires_at > issued_at)
) ENGINE = InnoDB;

CREATE INDEX idx_auth_session_user ON auth_session (user_id, expires_at);


-- kv_state.owner_user_id is declared above but its foreign key is added
-- here, because app_user did not exist at that point.
ALTER TABLE kv_state
    ADD CONSTRAINT fk_kv_state_owner
    FOREIGN KEY (owner_user_id) REFERENCES app_user (user_id)
    ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE client
    ADD CONSTRAINT fk_client_user
    FOREIGN KEY (user_id) REFERENCES app_user (user_id)
    ON DELETE SET NULL ON UPDATE CASCADE;


-- ---------------------------------------------------------------------
-- (d) Auditing
-- ---------------------------------------------------------------------

-- Written by triggers, never by the application. Every role or term
-- change a node undergoes lands here, which gives the cluster an
-- authoritative history that no application bug can skip.
CREATE TABLE node_event (
    event_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    node_id     INT UNSIGNED    NOT NULL,
    event_type  ENUM('role_change','term_change','vote_cast',
                     'commit_advance','crash','restart') NOT NULL,
    old_value   VARCHAR(64)     NULL,
    new_value   VARCHAR(64)     NULL,
    term_number BIGINT UNSIGNED NULL,
    description VARCHAR(512)    NULL,
    occurred_at TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),

    CONSTRAINT pk_node_event PRIMARY KEY (event_id),
    CONSTRAINT fk_node_event_node
        FOREIGN KEY (node_id) REFERENCES node (node_id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB;

CREATE INDEX idx_node_event_node_time ON node_event (node_id, occurred_at);
CREATE FULLTEXT INDEX ftx_node_event_description ON node_event (description);
