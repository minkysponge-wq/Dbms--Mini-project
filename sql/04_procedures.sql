-- =====================================================================
-- 04_procedures.sql - Stored functions and procedures
--
-- The Raft protocol operations, implemented in the database rather than
-- in application code. Each one that mutates state does so inside an
-- explicit transaction with an exit handler that rolls back, so a
-- partially applied operation can never be observed.
--
-- Functions
--   fn_majority_size         nodes needed for a quorum
--   fn_last_log_term         term of a node's final log entry
--   fn_user_has_permission   RBAC check, used by the API on every request
--
-- Procedures
--   sp_start_election        advance term, create it, self-vote
--   sp_cast_vote             record a vote under a row lock
--   sp_append_entry          append one entry to a node's log
--   sp_advance_commit_index  apply Raft's majority commit rule
--   sp_apply_committed       apply committed entries to the state machine
--   sp_kill_node             crash a node (admin only)
--   sp_restart_node          restart a node from stable storage
-- =====================================================================

USE raftkv;

DROP FUNCTION IF EXISTS fn_majority_size;
DROP FUNCTION IF EXISTS fn_last_log_term;
DROP FUNCTION IF EXISTS fn_user_has_permission;
DROP PROCEDURE IF EXISTS sp_start_election;
DROP PROCEDURE IF EXISTS sp_cast_vote;
DROP PROCEDURE IF EXISTS sp_append_entry;
DROP PROCEDURE IF EXISTS sp_advance_commit_index;
DROP PROCEDURE IF EXISTS sp_apply_committed;
DROP PROCEDURE IF EXISTS sp_kill_node;
DROP PROCEDURE IF EXISTS sp_restart_node;

DELIMITER $$

-- ---------------------------------------------------------------------
-- How many nodes constitute a majority. For five nodes this is three,
-- which is why the cluster tolerates two failures.
-- ---------------------------------------------------------------------
CREATE FUNCTION fn_majority_size()
RETURNS INT UNSIGNED
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_size INT UNSIGNED;
    SELECT COUNT(*) INTO v_size FROM node;
    RETURN FLOOR(v_size / 2) + 1;
END$$


-- ---------------------------------------------------------------------
-- Term of a node's last log entry; 0 for an empty log. Used by the
-- election restriction.
-- ---------------------------------------------------------------------
CREATE FUNCTION fn_last_log_term(p_node_id INT UNSIGNED)
RETURNS BIGINT UNSIGNED
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_term BIGINT UNSIGNED;
    SELECT term_number INTO v_term
      FROM log_entry
     WHERE node_id = p_node_id
     ORDER BY log_index DESC
     LIMIT 1;
    RETURN COALESCE(v_term, 0);
END$$


-- ---------------------------------------------------------------------
-- Role-based access check. Resolves through the role hierarchy, so a
-- user holding only 'admin' still passes a check for 'kv.read', which
-- admin inherits from viewer two levels down.
-- ---------------------------------------------------------------------
CREATE FUNCTION fn_user_has_permission(
    p_user_id INT UNSIGNED,
    p_code    VARCHAR(64)
)
RETURNS BOOLEAN
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_found INT DEFAULT 0;
    SELECT COUNT(*) INTO v_found
      FROM v_user_permission
     WHERE user_id = p_user_id
       AND permission_code = p_code;
    RETURN v_found > 0;
END$$


-- ---------------------------------------------------------------------
-- A follower times out and becomes a candidate: increment the term,
-- create the term row if this is the first node to reach it, and vote
-- for itself.
--
-- The whole thing is one transaction. A candidate that advanced its term
-- but failed to record its self-vote would be able to vote for someone
-- else in the same term, which breaks election safety.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_start_election(
    IN  p_node_id  INT UNSIGNED,
    OUT p_new_term BIGINT UNSIGNED
)
BEGIN
    DECLARE v_term      BIGINT UNSIGNED;
    DECLARE v_last_idx  BIGINT UNSIGNED;
    DECLARE v_last_term BIGINT UNSIGNED;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    -- Lock our own row for the duration, so two threads cannot both
    -- advance this node's term.
    SELECT current_term INTO v_term
      FROM node WHERE node_id = p_node_id FOR UPDATE;

    SET v_term = v_term + 1;

    INSERT IGNORE INTO term (term_number, leader_id) VALUES (v_term, NULL);

    UPDATE node
       SET current_term = v_term,
           role         = 'candidate',
           voted_for    = p_node_id
     WHERE node_id = p_node_id;

    SELECT COALESCE(MAX(log_index), 0) INTO v_last_idx
      FROM log_entry WHERE node_id = p_node_id;
    SET v_last_term = fn_last_log_term(p_node_id);

    INSERT INTO vote (term_number, voter_id, candidate_id, granted,
                      candidate_last_index, candidate_last_term)
    VALUES (v_term, p_node_id, p_node_id, TRUE, v_last_idx, v_last_term);

    COMMIT;
    SET p_new_term = v_term;
END$$


-- ---------------------------------------------------------------------
-- Record a vote.
--
-- This is the concurrency-control centrepiece. SELECT ... FOR UPDATE
-- takes an exclusive row lock on the voter's node row before any
-- decision is made, so if two candidates solicit the same voter at the
-- same instant, the second blocks until the first commits and then sees
-- the updated term. Without the lock both could read the old term, both
-- could be granted a vote, and two leaders could be elected in one term.
--
-- p_granted returns 0 or 1 rather than raising, because a refused vote
-- is a normal protocol outcome, not an error.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_cast_vote(
    IN  p_term            BIGINT UNSIGNED,
    IN  p_voter_id        INT UNSIGNED,
    IN  p_candidate_id    INT UNSIGNED,
    IN  p_cand_last_index BIGINT UNSIGNED,
    IN  p_cand_last_term  BIGINT UNSIGNED,
    OUT p_granted         BOOLEAN
)
BEGIN
    DECLARE v_current_term BIGINT UNSIGNED;
    DECLARE v_voted_for    INT UNSIGNED;
    DECLARE v_last_index   BIGINT UNSIGNED;
    DECLARE v_last_term    BIGINT UNSIGNED;
    DECLARE v_existing     INT DEFAULT 0;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SET p_granted = FALSE;
    END;

    SET p_granted = FALSE;
    START TRANSACTION;

    -- Exclusive lock on the voter for the rest of the transaction.
    SELECT current_term, voted_for
      INTO v_current_term, v_voted_for
      FROM node
     WHERE node_id = p_voter_id
       FOR UPDATE;

    -- Stale request from an old term: reject without changing anything.
    IF p_term < v_current_term THEN
        COMMIT;
    ELSE
        -- A higher term means we step down and our previous vote lapses.
        IF p_term > v_current_term THEN
            INSERT IGNORE INTO term (term_number, leader_id)
                VALUES (p_term, NULL);
            UPDATE node
               SET current_term = p_term,
                   role         = 'follower',
                   voted_for    = NULL
             WHERE node_id = p_voter_id;
            SET v_voted_for = NULL;
        END IF;

        SELECT COUNT(*) INTO v_existing
          FROM vote
         WHERE term_number = p_term AND voter_id = p_voter_id;

        SELECT COALESCE(MAX(log_index), 0) INTO v_last_index
          FROM log_entry WHERE node_id = p_voter_id;
        SET v_last_term = fn_last_log_term(p_voter_id);

        -- Grant only if we have not already voted this term and the
        -- candidate's log is at least as up to date as ours.
        IF v_existing = 0
           AND (p_cand_last_term > v_last_term
                OR (p_cand_last_term = v_last_term
                    AND p_cand_last_index >= v_last_index)) THEN

            INSERT INTO vote (term_number, voter_id, candidate_id, granted,
                              candidate_last_index, candidate_last_term)
            VALUES (p_term, p_voter_id, p_candidate_id, TRUE,
                    p_cand_last_index, p_cand_last_term);

            UPDATE node SET voted_for = p_candidate_id
             WHERE node_id = p_voter_id;

            SET p_granted = TRUE;
        END IF;

        COMMIT;
    END IF;
END$$


-- ---------------------------------------------------------------------
-- Append one entry to a node's log. The log-continuity trigger rejects
-- any index that would leave a gap.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_append_entry(
    IN  p_node_id   INT UNSIGNED,
    IN  p_term      BIGINT UNSIGNED,
    IN  p_cmd       VARCHAR(16),
    IN  p_key       VARCHAR(255),
    IN  p_value     TEXT,
    IN  p_client_id VARCHAR(64),
    IN  p_seq_no    BIGINT UNSIGNED,
    OUT p_index     BIGINT UNSIGNED
)
BEGIN
    DECLARE v_next BIGINT UNSIGNED;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    SELECT COALESCE(MAX(log_index), 0) + 1 INTO v_next
      FROM log_entry WHERE node_id = p_node_id FOR UPDATE;

    INSERT IGNORE INTO term (term_number, leader_id) VALUES (p_term, NULL);

    INSERT INTO log_entry (node_id, log_index, term_number, command_type,
                           key_name, value_text, client_id, seq_no, committed)
    VALUES (p_node_id, v_next, p_term, p_cmd,
            p_key, p_value, p_client_id, p_seq_no, FALSE);

    COMMIT;
    SET p_index = v_next;
END$$


-- ---------------------------------------------------------------------
-- Raft's commit rule.
--
-- Walk back from the leader's last index and commit the highest entry
-- that a majority of nodes hold. The term check is the subtle part: a
-- leader may only commit an entry from its OWN term by counting
-- replicas. Figure 8 of the Raft paper shows the case where counting
-- replicas on an older entry loses committed data. Earlier entries are
-- committed indirectly, once a same-term entry commits.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_advance_commit_index(
    IN  p_leader_id  INT UNSIGNED,
    OUT p_new_commit BIGINT UNSIGNED
)
BEGIN
    DECLARE v_term     BIGINT UNSIGNED;
    DECLARE v_commit   BIGINT UNSIGNED;
    DECLARE v_majority INT UNSIGNED;
    DECLARE v_best     BIGINT UNSIGNED;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    SELECT current_term, commit_index
      INTO v_term, v_commit
      FROM node WHERE node_id = p_leader_id FOR UPDATE;

    SET v_majority = fn_majority_size();

    -- Highest index from the leader's current term that a majority of
    -- nodes have stored.
    SELECT COALESCE(MAX(le.log_index), v_commit) INTO v_best
      FROM log_entry le
     WHERE le.node_id     = p_leader_id
       AND le.term_number = v_term
       AND le.log_index   > v_commit
       AND (
             SELECT COUNT(DISTINCT r.node_id)
               FROM log_entry r
              WHERE r.log_index   = le.log_index
                AND r.term_number = le.term_number
           ) >= v_majority;

    IF v_best > v_commit THEN
        UPDATE node SET commit_index = v_best WHERE node_id = p_leader_id;
        UPDATE log_entry
           SET committed = TRUE
         WHERE node_id = p_leader_id AND log_index <= v_best;
    END IF;

    COMMIT;
    SET p_new_commit = GREATEST(v_best, v_commit);
END$$


-- ---------------------------------------------------------------------
-- Apply committed-but-not-yet-applied entries to the state machine.
--
-- This is the transaction that most needs to be atomic. Applying one
-- entry touches three relations: kv_state gets the new value, node
-- advances last_applied, and client_session records the sequence number
-- for deduplication. If the process died between the first and third,
-- the node would re-apply the command on restart and a retried APPEND
-- would be applied twice. All three commit together or none do.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_apply_committed(
    IN  p_node_id INT UNSIGNED,
    OUT p_applied INT UNSIGNED
)
BEGIN
    DECLARE v_done       INT DEFAULT 0;
    DECLARE v_index      BIGINT UNSIGNED;
    DECLARE v_cmd        VARCHAR(16);
    DECLARE v_key        VARCHAR(255);
    DECLARE v_value      TEXT;
    DECLARE v_client     VARCHAR(64);
    DECLARE v_seq        BIGINT UNSIGNED;
    DECLARE v_last_seen  BIGINT UNSIGNED;
    DECLARE v_count      INT UNSIGNED DEFAULT 0;

    DECLARE cur CURSOR FOR
        SELECT le.log_index, le.command_type, le.key_name, le.value_text,
               le.client_id, le.seq_no
          FROM log_entry le
          JOIN node n ON n.node_id = le.node_id
         WHERE le.node_id   = p_node_id
           AND le.log_index > n.last_applied
           AND le.log_index <= n.commit_index
         ORDER BY le.log_index;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;
    END;

    START TRANSACTION;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO v_index, v_cmd, v_key, v_value, v_client, v_seq;
        IF v_done = 1 THEN
            LEAVE read_loop;
        END IF;

        -- Duplicate detection. A retried request carries the same
        -- (client_id, seq_no); applying it twice would corrupt an APPEND.
        --
        -- Written as a scalar subquery, NOT as SELECT ... INTO. A
        -- SELECT ... INTO that matches no rows raises NOT FOUND, which is
        -- the *same* condition the cursor uses to signal exhaustion. With
        -- a CONTINUE HANDLER FOR NOT FOUND in scope, the first lookup for
        -- an unknown client would set v_done = 1 and silently end the loop
        -- after one entry. A scalar subquery yields NULL instead and
        -- raises nothing.
        SET v_last_seen = NULL;
        IF v_client IS NOT NULL THEN
            SET v_last_seen = (
                SELECT last_seq_seen FROM client_session
                 WHERE node_id = p_node_id AND client_id = v_client
            );
        END IF;

        IF v_client IS NULL OR v_last_seen IS NULL OR v_seq > v_last_seen THEN
            IF v_cmd = 'PUT' THEN
                INSERT INTO kv_state (node_id, key_name, value_text,
                                      last_applied_index)
                VALUES (p_node_id, v_key, v_value, v_index)
                ON DUPLICATE KEY UPDATE
                    value_text = VALUES(value_text),
                    last_applied_index = VALUES(last_applied_index);

            ELSEIF v_cmd = 'APPEND' THEN
                INSERT INTO kv_state (node_id, key_name, value_text,
                                      last_applied_index)
                VALUES (p_node_id, v_key, v_value, v_index)
                ON DUPLICATE KEY UPDATE
                    value_text = CONCAT(kv_state.value_text, VALUES(value_text)),
                    last_applied_index = VALUES(last_applied_index);

            ELSEIF v_cmd = 'DELETE' THEN
                DELETE FROM kv_state
                 WHERE node_id = p_node_id AND key_name = v_key;
            END IF;

            IF v_client IS NOT NULL THEN
                INSERT IGNORE INTO client (client_id) VALUES (v_client);
                INSERT INTO client_session (node_id, client_id, last_seq_seen)
                VALUES (p_node_id, v_client, v_seq)
                ON DUPLICATE KEY UPDATE last_seq_seen = VALUES(last_seq_seen);
            END IF;

            SET v_count = v_count + 1;
        END IF;

        UPDATE node SET last_applied = v_index WHERE node_id = p_node_id;
    END LOOP;
    CLOSE cur;

    COMMIT;
    SET p_applied = v_count;
END$$


-- ---------------------------------------------------------------------
-- Crash a node. Its persistent state is untouched, which is the whole
-- point: sp_restart_node brings it back from exactly what it had.
-- Requires the node.kill permission.
-- ---------------------------------------------------------------------
CREATE PROCEDURE sp_kill_node(
    IN p_node_id INT UNSIGNED,
    IN p_user_id INT UNSIGNED
)
BEGIN
    IF NOT fn_user_has_permission(p_user_id, 'node.kill') THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'access denied: node.kill permission required';
    END IF;

    UPDATE node
       SET is_alive = FALSE, role = 'follower'
     WHERE node_id = p_node_id;
END$$


CREATE PROCEDURE sp_restart_node(
    IN p_node_id INT UNSIGNED,
    IN p_user_id INT UNSIGNED
)
BEGIN
    IF NOT fn_user_has_permission(p_user_id, 'node.restart') THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'access denied: node.restart permission required';
    END IF;

    -- Volatile state resets; current_term, voted_for and the log survive,
    -- which is what makes recovery safe.
    UPDATE node
       SET is_alive = TRUE, role = 'follower'
     WHERE node_id = p_node_id;
END$$

DELIMITER ;
