-- The reassessment trail must be able to outlive the node it names.
--
-- tw.core_reassessment records that on a given date a node's creation or
-- redefinition triggered a full rescan. That statement stays true after the
-- node is removed -- but the foreign key on trigger_node asserted the opposite,
-- so removing a node required either deleting its history or keeping a cell the
-- taxonomy had already ruled against.
--
-- It surfaced on 2026-09-11 removing A5-02 先進封裝: the tree sync could not
-- drop the node because one audit row from the 2026-09-04 rescan still cited
-- it. Deleting that row to make the tree edit possible would have destroyed the
-- record of a rescan that did happen, in order to tidy a tree that changed
-- afterwards. The column stays text and keeps its value; what goes is the claim
-- that the value must still resolve.
--
-- The same is not true of tw.company_industry_exposure: a membership pointing
-- at a node that no longer exists IS broken, and that FK stays -- which is why
-- the members had to be moved to A5-05 before the node could be dropped.

ALTER TABLE tw.core_reassessment
    DROP CONSTRAINT IF EXISTS core_reassessment_trigger_node_fkey;

COMMENT ON COLUMN tw.core_reassessment.trigger_node IS
    'Node code as it stood when the rescan was raised. Deliberately not a '
    'foreign key: the node may since have been removed or renamed, and the '
    'audit row still records what happened (migration 112).';
