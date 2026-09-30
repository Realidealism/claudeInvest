# Migrations that ran and whose files no longer exist

Three filenames are recorded in `public.schema_migrations` with no file anywhere.
`init_db()` inserts that row only after `cursor.execute(sql)` returns, so each of
them **ran successfully against the production database**. What they said is gone.
The live schema is the only remaining trace.

They are listed in `ACKNOWLEDGED_LOST` in `db/check_migrations.py` so that a
settled loss does not fail that check forever. A fourth missing file is a finding,
not a new entry here — an entry is added only after the loss has been adjudicated.

| filename | registration timestamp |
|---|---|
| `050_add_social_subscriptions.sql` | 2026-06-11 17:33:30.899969+00 |
| `051_intraday_push_state.sql` | 2026-06-11 17:33:30.899969+00 |
| `057_add_social_pushed.sql` | 2026-06-11 17:33:30.899969+00 |

## The timestamp is a registration time, not a first-application time

All three share `2026-06-11 17:33:30`, and so do 56 others: **001–057, 59 rows in
total, carry that one value**. That is the baseline run `init_db()` describes in
its own docstring — the first run against a pre-existing database re-executes
every file and records them as the baseline.

Two things follow, and they are different in strength:

- **Hard**: all three still existed on disk on 2026-06-11, and had certainly run
  by 17:33:30, because that run executed each file before registering it.
- **Unknown**: when each first ran. Before 2026-06-11 there was no
  `schema_migrations` table, so no registration trace exists from earlier. The
  timestamp above is an upper bound on first application, not the value itself.

## When each file was created

Derived from the git-add times of the neighbours, which are all tracked:

| neighbour | added to git | commit |
|---|---|---|
| `049_open_positions_exits.sql` | 2026-05-17 23:45 | `d2b1e997` |
| `052_add_cnn_fear_greed.sql` | 2026-06-10 09:39 | `7b08bcd6` |
| `056_add_yield_curve.sql` | 2026-06-11 17:00 | `08260089` |
| `058_add_taifex_futopt.sql` | 2026-06-24 23:35 | `e1e1b4a8` |

- `050`, `051`: created between **2026-05-17 and 2026-06-10**.
- `057`: created on **2026-06-11, between 17:00 and 17:33:30** — about half an hour.

Both windows rest on the convention that migration numbers are issued in creation
order. That is a convention, not a proof, so these bounds are weaker than the
17:33:30 upper bound above, which follows from the registry itself.

## Why they are unrecoverable

Every source that could hold a copy was checked:

| source | result |
|---|---|
| working tree | absent |
| git history (`--all --diff-filter=A`) | never present, in any commit or branch |
| 5 surviving exes (`db/migrations` is bundled per `daily_update.spec:8`) | absent |
| 5 PyInstaller `build/` directories | no `.sql` copies at all |

The exe check carries extra weight in two ways. First, the detector was validated
before its zeroes were believed: the TOC stores `db\migrations\NNN_name.sql` with
backslashes, and known-present filenames (`113`, `101`, `084`) each matched exactly
once in `dist/daily_update.exe`. Second, the bundled sequences run **001–049,
052–056, 058–113 unbroken** — the three gaps sit exactly where these files belong,
which is independent evidence that the files once existed rather than the numbers
having been skipped.

The two April 2026 exes only reach 015 and 022 and come from an earlier numbering
lineage (their `020` is `add_market_breadth_total`, where the later builds have
`add_etf_holdings`), so they predate these files and cannot help.

## Cause, and what merely triggered it

**Cause — the commit habit.** The gap was created when a migration was added and
committed by itself. `7b08bcd6` added `052` on 2026-06-10 while `050` and `051`
were already on disk and untracked. `085` and `102`–`113` repeated the same
pattern for the next three months, and `d9a80a13` closed that exposure by
tracking all fourteen remaining files.

**Trigger — a later deletion.** Something removed the files between 2026-06-11
(present, re-executed by the baseline run) and 2026-08-10 (absent from that
build). The `git clean -xdf` incident of 2026-07-02 falls inside that window, but
the record of it describes untracked files created after roughly 2026-07-01, while
these three existed well before. **It is a possible cause and is not established
as the cause.** A deletion turned a recoverable gap into a permanent one; it did
not create the gap.
