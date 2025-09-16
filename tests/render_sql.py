from resolver.defs.sql_utils import render_sql_to_file

path = render_sql_to_file(
  "pair_features_shard.sql.j2",
  "artifacts/feature_shard.sql",
  shard_index=11,
  shard_modulus=64,
  pairs_table="er.blocking_pairs",
  companies_table="silver.companies",
)

# path = render_sql_to_file(
#   "create_blocks_adaptive.sql.j2",
#   "artifacts/create_blocks_adaptive.sql",
#   source_table="silver.companies",
#   target_schema="er",
#   id_col="company_id",
#   name_col="company_name",
#   domain_col="domain_name",
#   city_col="city",
#   country_col="final_country",
#   max_block_size=200,
#   use_domain=True,
#   use_name3_country=True,
#   use_compact5_city=True,
#   refine_domain_with_country=True,
#   enable_stage3_token=True,
#   stage3_min_token_len=3,
#   stage3_stopwords=[  # or read from env as you planned
#     "inc", "llc", "ltd", "gmbh", "corp", "co", "company", "plc", "pty", "ag", "nv",
#     "sa", "spa", "group", "global", "international", "national", "community", "north",
#     "south", "east", "west", "central", "american", "solutions", "systems", "services",
#     "the", "and", "of", "blue", "green", "red", "white", "black"
#   ],
#   enable_stage3_name6=True,
# )
print(f"Rendered to: {path}")
