# Separate CPU profiles before storage tuning

Matched 10,000 pages × 1,000 resources, one worker. Fresh ledger runs first,
then the token baseline; these profiled runs are separate from the unprofiled
cost table. Source: 65214ade plus the optional pre-seal metrics hook in this
commit; token source: eb63f1b5 plus the same baseline test driver. Binary and
raw-profile hashes are recorded in manifest.json. Both count checks pass.

Sampled CPU totals are 122.46 s ledger and 88.98 s token. Pebble getInternal
accounts for 60.26 s versus 32.22 s cumulatively. The resource handler's
GetResource is 33.58 s versus 19.61 s; stageResourceRecords is 35.77 s versus
21.74 s. These are nested cumulative samples and must not be summed. Final
fixture ListResources verification costs roughly 18 s in both profiles and is
not part of the sync's measured wall time. OS CPU totals in usage.json include
all process work and differ from sampled totals.

Before sealing, ledger collection has performed four compactions versus zero
for token. Reported read amplification is 3 versus 2; ledger has tables in L0
and L6, token only in L0. Both have 99.9% block-cache hit rates and no filters.
The production options configure no Bloom filter. These observations locate
most extra CPU in existing point lookups and show a changed table layout.
They support, but do not independently prove, the hypothesis that frequent
ledger metadata changes table overlap and increases negative-lookup work.

The optional hook writes the Pebble metrics at the existing invariants-complete
boundary. It is inactive unless BATON_LEDGER_COST_PRESEAL_METRICS is set, and
introduces no production hook or store behavior. Profiled sync wall times are
108.836 s ledger and 77.078 s token; do not merge them into unprofiled medians.
