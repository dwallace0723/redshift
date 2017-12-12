select
  md5(query_id::varchar + '-' + started_at::varchar) as unique_key,
  *
from {{ ref('redshift_query_mode') }}
{% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
  where started_at > (select max(started_at) from {{ this }})
{% endif %}
