--Tables Bloat, rough estimation

--This SQL is derived from https://github.com/ioguix/pgsql-bloat-estimation/blob/master/table/table_bloat.sql

/*
* WARNING: executed with a non-superuser role, the query inspect only tables you are granted to read.
* This query is compatible with PostgreSQL 9.0 and more
*/


with constants as (
  select case when version() ~ 'mingw32|64-bit|x86_64|ppc64|ia64|amd64' then 8 else 4 end as chunk_size
), step1 as (
  select
    tbl.oid tblid,
    ns.nspname as schema_name,
    tbl.relname as table_name,
    tbl.reltuples,
    tbl.relpages as heappages,
    coalesce(toast.relpages, 0) as toastpages,
    coalesce(toast.reltuples, 0) as toasttuples,
    coalesce(substring(array_to_string(tbl.reloptions, ' ') from '%fillfactor=#"__#"%' for '#')::int2, 100) as fillfactor,
    current_setting('block_size')::numeric as bs,
    chunk_size,
    24 as page_hdr,
    23 + case when max(coalesce(null_frac, 0)) > 0 then (7 + count(*)) / 8 else 0::int end
      + case when tbl.relhasoids then 4 else 0 end as tpl_hdr_size,
    sum((1 - coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) as tpl_data_size,
    bool_or(att.atttypid = 'pg_catalog.name'::regtype) or count(att.attname) <> count(s.attname) as is_na
  from pg_attribute as att
  join constants on true
  join pg_class as tbl on att.attrelid = tbl.oid and tbl.relkind = 'r'
  join pg_namespace as ns on ns.oid = tbl.relnamespace
  join pg_stats as s on s.schemaname = ns.nspname and s.tablename = tbl.relname and not s.inherited and s.attname = att.attname
  left join pg_class as toast on tbl.reltoastrelid = toast.oid
  where att.attnum > 0 and not att.attisdropped and s.schemaname not in ('pg_catalog', 'information_schema')
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, tbl.relhasoids
  order by 2, 3
), padding as (
  with recursive columns as (
    select
      table_schema,
      table_name,
      ordinal_position,
      column_name,
      udt_name,
      typalign,
      typlen,
      case typalign -- see https://www.postgresql.org/docs/current/static/catalog-pg-type.html
        when 'c' then
          case when typlen > 0 then typlen % chunk_size else 0 end
        when 's' then 2
        when 'i' then 4
        when 'd' then 8
        else null
      end as _shift,
      case typalign
        when 's' then 1
        when 'i' then 2
        when 'd' then 3
        when 'c' then
          case when typlen > 0 then typlen % chunk_size else 9 end
        else 9
      end as alt_order_group,
      character_maximum_length
    from information_schema.columns
    join constants on true
    join pg_type on udt_name = typname
    where table_schema not in ('information_schema', 'pg_catalog')
  ), combined_columns as (
    select *, coalesce(character_maximum_length, _shift) as shift
    from columns
  ), analyze_alignment as (
    select
      table_schema,
      table_name,
      0 as analyzed,
      (select chunk_size from constants) as left_in_chunk,
      '{}'::text[] as padded_columns,
      '{}'::int[] as pads,
      (select max(ordinal_position) from columns c where c.table_name = _.table_name and c.table_schema = _.table_schema) as col_cnt,
      array_agg(_.column_name::text order by ordinal_position) as cols,
      array_agg(_.udt_name::text order by ordinal_position) as types,
      array_agg(shift order by ordinal_position) as shifts,
      null::int as curleft,
      null::text as prev_column_name,
      false as has_varlena
    from
      combined_columns _
    group by table_schema, table_name
    union all
    select
      table_schema,
      table_name,
      analyzed + 1,
      cur_left_in_chunk,
      case when padding_occured > 0 then padded_columns || array[prev_column_name] else padded_columns end,
      case when padding_occured > 0 then pads || array[padding_occured] else pads end,
      col_cnt,
      cols,
      types,
      shifts,
      cur_left_in_chunk,
      ext.column_name as prev_column_name,
      a.has_varlena or (ext.typlen = -1) -- see https://www.postgresql.org/docs/current/static/catalog-pg-type.html
    from analyze_alignment a, constants, lateral (
      select
        shift,
        case when left_in_chunk < shift then left_in_chunk else 0 end as padding_occured,
        case when left_in_chunk < shift then chunk_size - shift % chunk_size else left_in_chunk - shift end as cur_left_in_chunk,
        column_name,
        typlen
      from combined_columns c, constants
      where
        ordinal_position = a.analyzed + 1
        and c.table_name = a.table_name
        and c.table_schema = a.table_schema
    ) as ext
    where
      analyzed < col_cnt and analyzed < 1000/*sanity*/
  )
  select distinct on (table_schema, table_name)
    table_schema,
    table_name,
    padded_columns,
    case when curleft % chunk_size > 0 then pads || array[curleft] else pads end as pads,
    curleft,
    coalesce((select sum(p) from unnest(pads) _(p)), 0) + (chunk_size + a1.curleft) % chunk_size as padding_sum,
    shifts,
    analyzed,
    a1.has_varlena
  from analyze_alignment a1
  join pg_namespace n on n.nspname = table_schema
  join pg_class c on n.oid = c.relnamespace and c.relname = table_name
  join constants on true
  order by 1, 2, analyzed desc
), step2 as (
  select
    step1.*,
    coldata.padding_amendment,
    (
      4 + tpl_hdr_size + tpl_data_size + (2 * chunk_size)
      - case when tpl_hdr_size % chunk_size = 0 then chunk_size else tpl_hdr_size % chunk_size end
      - case when ceil(tpl_data_size)::int % chunk_size = 0 then chunk_size else ceil(tpl_data_size)::int % chunk_size end
      + coalesce(padding_amendment, 0) -- add calculated total padding for fixed-size columns (varlena is not considered now)
    ) as tpl_size,
    bs - page_hdr as size_per_block,
    (heappages + toastpages) as tblpages
  from step1
  join lateral (
    select sum(padding_sum) as padding_amendment
    from padding p
    where p.table_schema = step1.schema_name and p.table_name = step1.table_name
  ) coldata on true
), step3 as (
  select
    *,
    reltuples * padding_amendment as padding_total,
    ceil(reltuples / ((bs - page_hdr) / tpl_size)) + ceil(toasttuples / 4) as est_tblpages,
    ceil(reltuples / ((bs - page_hdr) * fillfactor / (tpl_size * 100))) + ceil(toasttuples / 4) as est_tblpages_ff
    -- , stattuple.pgstattuple(tblid) as pst
  from step2
), step4 as (
  select
    step3.*,
    tblpages * bs as real_size,
    (tblpages - est_tblpages) * bs as extra_size,
    case when tblpages - est_tblpages > 0 then 100 * (tblpages - est_tblpages) / tblpages::float else 0 end as extra_ratio,
    (tblpages - est_tblpages_ff) * bs as bloat_size,
    case when tblpages - est_tblpages_ff > 0 then 100 * (tblpages - est_tblpages_ff) / tblpages::float else 0 end as bloat_ratio
    -- , (pst).free_percent + (pst).dead_tuple_percent as real_frag
  from step3
  -- WHERE NOT is_na
  --   AND tblpages*((pst).free_percent + (pst).dead_tuple_percent)::float4/100 >= 1
)
select
  padding_amendment,
  pg_size_pretty(padding_total::numeric),
  case is_na when true then 'TRUE' else '' end as "Is N/A",
  coalesce(nullif(schema_name, 'public') || '.', '') || table_name as "Table",
  pg_size_pretty(real_size::numeric) as "Size",
  '~' || pg_size_pretty(extra_size::numeric)::text || ' (' || round(extra_ratio::numeric, 2)::text || '%)' as "Extra",
  '~' || pg_size_pretty(bloat_size::numeric)::text || ' (' || round(bloat_ratio::numeric, 2)::text || '%)' as "Bloat",
  '~' || pg_size_pretty((real_size - bloat_size)::numeric) as "Live",
  fillfactor
\if :postgres_dba_wide
  ,
  real_size as real_size_raw,
  extra_size as extra_size_raw,
  bloat_size as bloat_size_raw,
  real_size - bloat_size as live_data_size_raw,
  *
\endif
from step4
order by real_size desc nulls last
;

/*
Author of the original version:
  2015, Jehan-Guillaume (ioguix) de Rorthais

License of the original version:

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
