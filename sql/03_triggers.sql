-- =====================================================================
-- 03_triggers.sql - Triggers
--
-- These are not decorative. Each one enforces a rule that the Raft
-- correctness argument depends on, at the database layer, so that an
-- application bug cannot violate it:
--
--   trg_vote_bi    election safety and the election restriction
--   trg_vote_bu    a cast vote is immutable
--   trg_log_bi     log continuity (no gaps in a node's log)
--   trg_node_au    automatic audit of every role and term change
--   trg_repl_bi/bu a leader never replicates to itself
--
-- Errors are raised with SIGNAL SQLSTATE '45000', which surfaces in the
-- application as an ordinary database error.
-- =====================================================================

USE raftkv;

DROP TRIGGER IF EXISTS trg_vote_bi;
DROP TRIGGER IF EXISTS trg_vote_bu;
DROP TRIGGER IF EXISTS trg_log_bi;
DROP TRIGGER IF EXISTS trg_node_au;
DROP TRIGGER IF EXISTS trg_repl_bi;
DROP TRIGGER IF EXISTS trg_repl_bu;

DELIMITER $$

-- ---------------------------------------------------------------------
-- Election safety and the election restriction.
--
-- The composite primary key (term_number, voter_id) already makes a
-- second vote in the same term impossible. This trigger adds the two
-- rules a key cannot express:
--
--   1. A node may not cast a vote in a term below the one it has
--      already reached. A stale RequestVote arriving late must not be
--      recorded.
--   2. A granted vote requires the candidate's log to be at least as up
--      to date as the voter's - last term first, then last index. Without
--      this a candidate missing committed entries could win an election
--      and overwrite them.
-- ---------------------------------------------------------------------
CREATE TRIGGER trg_vote_bi
BEFORE INSERT ON vote
FOR EACH ROW
BEGIN
    DECLARE v_current_term BIGINT UNSIGNED;
    DECLARE v_last_index   BIGINT UNSIGNED;
    DECLARE v_last_term    BIGINT UNSIGNED;

    SELECT current_term INTO v_current_term
      FROM node WHERE node_id = NEW.voter_id;

    IF NEW.term_number < v_current_term THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: refusing to vote in a term below the voter current term';
    END IF;

    IF NEW.granted THEN
        SELECT COALESCE(MAX(log_index), 0) INTO v_last_index
          FROM log_entry WHERE node_id = NEW.voter_id;

        SELECT COALESCE(term_number, 0) INTO v_last_term
          FROM log_entry
         WHERE node_id = NEW.voter_id AND log_index = v_last_index;

        IF NEW.candidate_last_term < v_last_term
           OR (NEW.candidate_last_term = v_last_term
               AND NEW.candidate_last_index < v_last_index) THEN
            SIGNAL SQLSTATE '45000'
              SET MESSAGE_TEXT = 'raft: election restriction - candidate log is not up to date';
        END IF;
    END IF;
END$$


-- ---------------------------------------------------------------------
-- A vote, once cast, can never be changed. Raft's safety argument
-- assumes a voter's decision for a term is final; allowing an UPDATE
-- would let a node effectively vote twice.
-- ---------------------------------------------------------------------
CREATE TRIGGER trg_vote_bu
BEFORE UPDATE ON vote
FOR EACH ROW
BEGIN
    IF OLD.candidate_id <> NEW.candidate_id
       OR OLD.granted <> NEW.granted THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: a cast vote is immutable and cannot be changed';
    END IF;
END$$


-- ---------------------------------------------------------------------
-- Log continuity. A node's log must have no gaps: the next entry is
-- always exactly one past its current highest index. This is what makes
-- the log-matching property checkable.
-- ---------------------------------------------------------------------
CREATE TRIGGER trg_log_bi
BEFORE INSERT ON log_entry
FOR EACH ROW
BEGIN
    DECLARE v_max BIGINT UNSIGNED;

    SELECT COALESCE(MAX(log_index), 0) INTO v_max
      FROM log_entry WHERE node_id = NEW.node_id;

    IF NEW.log_index <> v_max + 1 THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: log gap - entries must be appended contiguously';
    END IF;
END$$


-- ---------------------------------------------------------------------
-- Automatic audit. Every role change, term change and commit advance a
-- node undergoes is recorded in node_event without the application
-- having to remember to log it.
-- ---------------------------------------------------------------------
CREATE TRIGGER trg_node_au
AFTER UPDATE ON node
FOR EACH ROW
BEGIN
    IF OLD.role <> NEW.role THEN
        INSERT INTO node_event
            (node_id, event_type, old_value, new_value, term_number, description)
        VALUES
            (NEW.node_id, 'role_change', OLD.role, NEW.role, NEW.current_term,
             CONCAT('node ', NEW.node_id, ' changed role from ', OLD.role,
                    ' to ', NEW.role, ' in term ', NEW.current_term));
    END IF;

    IF OLD.current_term <> NEW.current_term THEN
        INSERT INTO node_event
            (node_id, event_type, old_value, new_value, term_number, description)
        VALUES
            (NEW.node_id, 'term_change', OLD.current_term, NEW.current_term,
             NEW.current_term,
             CONCAT('node ', NEW.node_id, ' advanced from term ', OLD.current_term,
                    ' to term ', NEW.current_term));
    END IF;

    IF OLD.commit_index <> NEW.commit_index THEN
        INSERT INTO node_event
            (node_id, event_type, old_value, new_value, term_number, description)
        VALUES
            (NEW.node_id, 'commit_advance', OLD.commit_index, NEW.commit_index,
             NEW.current_term,
             CONCAT('node ', NEW.node_id, ' advanced commit index from ',
                    OLD.commit_index, ' to ', NEW.commit_index));
    END IF;

    IF OLD.is_alive <> NEW.is_alive THEN
        INSERT INTO node_event
            (node_id, event_type, old_value, new_value, term_number, description)
        VALUES
            (NEW.node_id,
             CASE WHEN NEW.is_alive THEN 'restart' ELSE 'crash' END,
             OLD.is_alive, NEW.is_alive, NEW.current_term,
             CONCAT('node ', NEW.node_id,
                    CASE WHEN NEW.is_alive
                         THEN ' restarted and recovered state from stable storage'
                         ELSE ' crashed and left the cluster' END));
    END IF;
END$$


-- ---------------------------------------------------------------------
-- A leader never replicates to itself. This would be a CHECK constraint,
-- but MariaDB rejects CHECK on foreign-key columns that cascade, so it
-- lives here to keep the schema portable between MySQL and MariaDB.
-- ---------------------------------------------------------------------
CREATE TRIGGER trg_repl_bi
BEFORE INSERT ON replication_state
FOR EACH ROW
BEGIN
    IF NEW.leader_id = NEW.follower_id THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: a leader cannot replicate to itself';
    END IF;
END$$


-- match_index records the highest entry known to be stored on a
-- follower. That knowledge can never be withdrawn, so the value must
-- increase monotonically.
CREATE TRIGGER trg_repl_bu
BEFORE UPDATE ON replication_state
FOR EACH ROW
BEGIN
    IF NEW.leader_id = NEW.follower_id THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: a leader cannot replicate to itself';
    END IF;
    IF NEW.match_index < OLD.match_index THEN
        SIGNAL SQLSTATE '45000'
          SET MESSAGE_TEXT = 'raft: match_index must increase monotonically';
    END IF;
END$$

DELIMITER ;
