import C1z

/-!
Axiom audit. `scripts/check.sh` runs this file and diffs its output
against `AXIOMS.golden`. Any theorem whose dependency set grows beyond
`propext`, `Classical.choice`, and `Quot.sound` (a `sorry`, a
`native_decide`, a custom axiom) changes the output and fails the gate.
Add every exported theorem here; the golden file is reviewed by a human.
-/

-- Codec
#print axioms C1z.Codec.escape_injective
#print axioms C1z.Codec.encodeTuple_injective
#print axioms C1z.Codec.isPrefix_of_scanPrefix_isPrefix
#print axioms C1z.Codec.encodeKey_injective
#print axioms C1z.Codec.encodeScanPrefix_isPrefix_iff

-- Order
#print axioms C1z.lexLt.trans
#print axioms C1z.lexLt.total
#print axioms C1z.lexLt.lt_of_prefix_of_lt_length

-- Identity
#print axioms C1z.expandEnt_compressEnt
#print axioms C1z.compressEnt_stripped_iff
#print axioms C1z.ResourceTypeId.key_injective
#print axioms C1z.ResourceId.key_injective
#print axioms C1z.EntitlementId.key_injective
#print axioms C1z.GrantId.key_injective
#print axioms C1z.key_kind_disjoint
#print axioms C1z.ResourceId.key_ne_of_rt_ne
#print axioms C1z.GrantId.key_under_entitlement_prefix
#print axioms C1z.GrantId.ent_eq_of_key_under_prefix
#print axioms C1z.GrantRecord.key_eq_iff
#print axioms C1z.publicId_not_injective

-- Store
#print axioms C1z.Store.get_put_self
#print axioms C1z.Store.get_put_of_ne
#print axioms C1z.Store.put_put_last
#print axioms C1z.Store.get_erase_self
#print axioms C1z.Store.get_putBatch
#print axioms C1z.Store.putBatch_dedupLast
#print axioms C1z.Store.keys_nodup
#print axioms C1z.Store.mem_keys_iff
#print axioms C1z.Store.ext_of_get_eq

-- Paginate
#print axioms C1z.Paginate.clampPageSize_pos
#print axioms C1z.Paginate.clampPageSize_le
#print axioms C1z.Paginate.page_items_mem
#print axioms C1z.Paginate.page_next_isSome_imp_full
#print axioms C1z.Paginate.page_next_isSome_imp_nonempty
#print axioms C1z.Paginate.page_next_none_imp_exhausted
#print axioms C1z.Paginate.checkCursor_invalid_of_not_prefix
#print axioms C1z.Paginate.checkCursor_ok_of_page
#print axioms C1z.Paginate.traverse_complete
#print axioms C1z.Paginate.traverse_flatten_eq_of_pos
#print axioms C1z.Paginate.traverse_terminates

-- Sync
#print axioms C1z.Sync.writeGate_opened
#print axioms C1z.Sync.writeGate_endSync
#print axioms C1z.Sync.writeGate_engineSealed_iff
#print axioms C1z.Sync.finished_endSync
#print axioms C1z.Sync.writeGate_resumeSync_finished
#print axioms C1z.Sync.resumeSync_notFound
#print axioms C1z.Sync.hasRecords_startNewSync
#print axioms C1z.Sync.startNewSync_refused_of_fresh
#print axioms C1z.Sync.startNewSync_startNewSync
#print axioms C1z.Sync.startNewSync_endSync
#print axioms C1z.Sync.startNewSync_resumeSync
#print axioms C1z.Sync.latestFinished_none_of_unfinished
#print axioms C1z.Sync.latestFinished_type
#print axioms C1z.Sync.latestFinished_spec
#print axioms C1z.Sync.resolveActiveSync_source
#print axioms C1z.Sync.resolveActiveSync_none_of_stale
#print axioms C1z.Sync.resolveActiveSync_annotation

-- Result
#print axioms C1z.Result.complete_iff
#print axioms C1z.Result.not_complete_failed
#print axioms C1z.Result.not_complete_abandoned
#print axioms C1z.Result.streamEnd_failed_of_error
#print axioms C1z.Result.streamEnd_exhausted_of_no_error
#print axioms C1z.Result.resolveBare_found_iff
#print axioms C1z.Result.resolveBare_ambiguous_iff
#print axioms C1z.Result.resolveBare_found_imp_unique
