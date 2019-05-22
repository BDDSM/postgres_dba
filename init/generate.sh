#!/bin/bash
# Generate start.psql based on the contents of "sql" directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

WARMUP="warmup.psql"
OUT="start.psql"

> "$WARMUP"
> "$OUT"

cd "$DIR/.."
cat > "$WARMUP" <<- VersCheck
-- check if "\if" is supported (psql 10+)
\if false
  \echo cannot work, you need psql version 10+ (Postgres server can be older)
  select 1/0;
\endif

select regexp_replace(version(), '^PostgreSQL (\d+\.\d+).*$', e'\\\\1')::numeric >= 10 as postgres_dba_pgvers_10plus \gset
\if :postgres_dba_pgvers_10plus
  \set postgres_dba_last_wal_receive_lsn pg_last_wal_receive_lsn
  \set postgres_dba_last_wal_replay_lsn pg_last_wal_replay_lsn
  \set postgres_dba_is_wal_replay_paused pg_is_wal_replay_paused

  \set postgres_dba_wal_lsn_diff pg_wal_lsn_diff
  \set postgres_dba_wal_current_lsn pg_current_wal_lsn
  \set postgres_dba_col_sent_lsn sent_lsn
  \set postgres_dba_col_write_lsn write_lsn
  \set postgres_dba_col_flush_lsn flush_lsn
  \set postgres_dba_col_replay_lsn replay_lsn
\else
  \set postgres_dba_last_wal_receive_lsn pg_last_xlog_receive_location
  \set postgres_dba_last_wal_replay_lsn pg_last_xlog_replay_location
  \set postgres_dba_is_wal_replay_paused pg_is_xlog_replay_paused

  \set postgres_dba_wal_lsn_diff pg_xlog_location_diff
  \set postgres_dba_wal_current_lsn pg_current_xlog_location
  \set postgres_dba_col_sent_lsn sent_location
  \set postgres_dba_col_write_lsn write_location
  \set postgres_dba_col_flush_lsn flush_location
  \set postgres_dba_col_replay_lsn replay_location
\endif

-- TODO: improve work with custom GUCs for Postgres 9.5 and older
select regexp_replace(version(), '^PostgreSQL (\d+\.\d+).*$', e'\\\\1')::numeric >= 9.6 as postgres_dba_pgvers_96plus \gset
\if :postgres_dba_pgvers_96plus
  select coalesce(current_setting('postgres_dba.wide', true), 'off') = 'on' as postgres_dba_wide \gset
\else
  set client_min_messages to 'fatal';
  select :postgres_dba_wide as postgres_dba_wide \gset
  reset client_min_messages;
\endif
VersCheck

echo "\\ir $WARMUP" >> "$OUT"

echo "\\echo '\\033[1;35mMenu:\\033[0m'" >> "$OUT"
for f in ./sql/*.sql
do
  prefix=$(echo $f | sed -e 's/_.*$//g' -e 's/^.*\///g')
  desc=$(head -n1 $f | sed -e 's/^--//g')
  printf "%s '%4s – %s'\n" "\\echo" "$prefix" "$desc" >> "$OUT"
done
printf "%s '%4s – %s'\n" "\\echo" "q" "Quit" >> "$OUT"
echo "\\echo" >> "$OUT"
echo "\\echo Type your choice and press <Enter>:" >> "$OUT"
echo "\\prompt d_step_unq" >> "$OUT"
echo "\\set d_stp '\\'' :d_step_unq '\\''" >> "$OUT"
echo "select" >> "$OUT"

for f in ./sql/*.sql
do
  prefix=$(echo $f | sed -e 's/_.*$//g' -e 's/^.*\///g')
  echo ":d_stp::text = '$prefix' as d_step_is_$prefix," >> "$OUT"
done
echo ":d_stp::text = 'q' as d_step_is_q \\gset" >> "$OUT"

echo "\\if :d_step_is_q" >> "$OUT"
echo "  \\echo 'Bye!'" >> "$OUT"
echo "  \\echo" >> "$OUT"
for f in ./sql/*.sql
do
  prefix=$(echo $f | sed -e 's/_.*$//g' -e 's/^.*\///g')
  echo "\\elif :d_step_is_$prefix" >> "$OUT"
  echo "  \\ir $f" >> "$OUT"
  echo "  \\prompt 'Press <Enter> to continue…' d_dummy" >> "$OUT"
  echo "  \\ir ./$OUT" >> "$OUT"
done
echo "\\else" >> "$OUT"
echo "  \\echo" >> "$OUT"
echo "  \\echo '\\033[1;31mError:\\033[0m Unknown option! Try again.'" >> "$OUT"
echo "  \\echo" >> "$OUT"
echo "  \\ir ./$OUT" >> "$OUT"
echo "\\endif" >> "$OUT"

echo "Done."
cd ->/dev/null
exit 0
