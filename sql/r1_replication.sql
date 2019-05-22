--Replication [WIP]

/*
In Postgres 9.6 and earlier use:

\set postgres_dba_wal_lsn_diff pg_xlog_location_diff
\set postgres_dba_wal_current_lsn pg_current_xlog_location
\set postgres_dba_col_sent_lsn sent_location
\set postgres_dba_col_write_lsn write_location
\set postgres_dba_col_flush_lsn flush_location
\set postgres_dba_col_replay_lsn replay_location
*/


select
  client_addr, usename, application_name, state, sync_state,
  pg_size_pretty(:postgres_dba_wal_lsn_diff(:postgres_dba_wal_current_lsn(), :postgres_dba_col_sent_lsn)) as pending_lag,
  pg_size_pretty(:postgres_dba_wal_lsn_diff(:postgres_dba_col_sent_lsn, :postgres_dba_col_write_lsn)) as write_lag,
  pg_size_pretty(:postgres_dba_wal_lsn_diff(:postgres_dba_col_write_lsn, :postgres_dba_col_flush_lsn)) as flush_lag,
  pg_size_pretty(:postgres_dba_wal_lsn_diff(:postgres_dba_col_flush_lsn, :postgres_dba_col_replay_lsn)) as replay_lag,
  pg_size_pretty(:postgres_dba_wal_lsn_diff(:postgres_dba_wal_current_lsn(), coalesce(:postgres_dba_col_replay_lsn, :postgres_dba_col_flush_lsn))) as total_lag,
  txid_current() % (2^32)::int8 - xmin::text::int8 as xid_delayed,
  slots.*
from pg_stat_replication
left join pg_replication_slots slots on pid = active_pid;
