-- pre reqs
  -- query first 400 from raw data
  -- make column data with accurate typing and persist to s3 (should this live in s3 anymore? Probs not)

CREATE TABLE spectrum.asdfadsf_base (
  ${mapColumnsToTable()} -- this should allow us to dynamically query this piece
)

INSERT INTO asdfads_working (mapColumnIds) 
  SELECT 
    _id uuid(),
    case when isna then null,
    x as mapColValueToColId 
  FROM spectrum.asdfadsf_base

FROM importedcsv

