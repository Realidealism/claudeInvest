-- Make "this Core has to be judged again" a row, not something someone remembers.
--
-- R023 says a taxonomy action that changes a company's candidate set obliges a
-- fresh Core adjudication: adding a node, or widening an existing node's
-- definition so that a business which had nowhere to go becomes a legal
-- candidate. On 2026-09-03 both happened -- four nodes added, J5 and J4-01
-- widened -- and eight Cores were re-run. Every one of them was re-run because
-- the same person who changed the definition also remembered to, and the only
-- trace is a sentence inside each row's evidence.
--
-- That is a record, not a queue. Nothing lights up. The next round, done by
-- someone who was not here, has no way to learn that widening J4-01 owes a
-- rescan of every company whose products the new clause now reaches.
--
-- Two kinds of row, because two different things are knowable at two different
-- times:
--
--   stock_id IS NULL   the taxonomy action itself. Knowable automatically the
--                      moment the tree changes, and it means "the whole table
--                      still owes a scan against this node". sync_tree raises
--                      these on its own.
--   stock_id IS NOT NULL  a company known to be affected. Only knowable by
--                      whoever does the mapping, so these are written by hand
--                      as the work proceeds.
--
-- Resolving is deliberate: resolved_on is set by the person who re-ran the
-- adjudication, and db.check_industry's [core_pending] lists whatever is still
-- open. An empty queue is the claim that no Core is owed a re-run, and that
-- claim is now checkable.

CREATE TABLE IF NOT EXISTS tw.core_reassessment (
    id           BIGSERIAL PRIMARY KEY,
    stock_id     TEXT REFERENCES tw.stocks(stock_id),
    trigger_kind TEXT NOT NULL
                 CHECK (trigger_kind IN ('node_created', 'definition_changed')),
    trigger_node TEXT NOT NULL REFERENCES tw.industry_tree(node_code),
    raised_on    DATE NOT NULL DEFAULT current_date,
    resolved_on  DATE,
    note         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT core_reassessment_resolved_after_raised
        CHECK (resolved_on IS NULL OR resolved_on >= raised_on)
);

-- One open item per (company, node, cause). A second widening of the same node
-- after the first was resolved is a new obligation, so the uniqueness only
-- binds while the row is open -- which is what a partial index expresses and a
-- plain UNIQUE cannot.
CREATE UNIQUE INDEX IF NOT EXISTS core_reassessment_open_uniq
    ON tw.core_reassessment (COALESCE(stock_id, ''), trigger_node, trigger_kind)
    WHERE resolved_on IS NULL;

CREATE INDEX IF NOT EXISTS core_reassessment_open_idx
    ON tw.core_reassessment (resolved_on) WHERE resolved_on IS NULL;

COMMENT ON TABLE tw.core_reassessment IS
    'Cores owed a fresh R023 adjudication because a taxonomy action changed the '
    'candidate set. stock_id NULL = the whole table owes a scan against that node.';
