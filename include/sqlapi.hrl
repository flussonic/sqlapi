

-record(sql_filter, {
  conditions,
  columns = [], % [] = no restrictions, use all
  table_columns,
  order,
  group,
  limit,
  offset
}).
