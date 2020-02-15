CREATE TABLE mag.data
(
  id bigserial NOT NULL,
  observatory_id bigint,
  obs_date date,
  "values" jsonb
);
ALTER TABLE mag.data
  OWNER TO steve;