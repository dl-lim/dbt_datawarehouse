# dbt_datawarehouse
*August 2021*

This repo is a showcase of dbt being used in a real-world setting, when paired with Airbyte.

The objective of these transformations are to combine multiple data sources into a central, single source of truth.

The final outputs are consumed by analysts from Marketing, Customer Support and Finance via an instance of Apache Superset which I managed and self-hosted.

Some of these transforms are materialised as Views, while some are materialised as Tables. Views are more cost-effective but may take a while to display on Superset, since they are computed on demand. The tables materialised as Tables are some core tables that are used to calculate second layer tables.

While there's a type of materialisation called "Materialised View", which would be the best option in a scenario like this, it wasn't supported by dbt for Postgres at the time of this writing. Storage was relatively cheap and for the size of the data, it was a negligible cost when compared against the speed of queries.

The layering of tables/views are done with `tags` in `dbt_project.yml`. This ensures that all lower-layer tables/views are materialised before calculating the upper layers.

It is worth noting that with some databases like Snowflake, the views remain even when the underlying tables are deleted. With how Postgres and Airbyte work, each time an ELT is called (every hour in this case), the table names get shifted and thus "deleted" in Postgres's eyes. So, each view has to be recomputed each run, which is not an issue with automated scheduling. This may not be a feasible option if the data is massive and compute costs are high.




# Developer Instructions (for deploying on Airbyte)

- airbyte dbt entrypoint: `run --profiles-dir ./.dbt/ --models tag:xxx`
- Docker image: `fishtownanalytics/dbt:0.19.1`
- Git repository URL: `https://user:password@gitrepourl`
- Configurable files:
  - .dbt/profiles.yml
  - dbt_project.yml
- dbt model files:
  - models/*



# Manual Development Setup

Get python3, pip and create a venv
```
apt install python3-pip
mkdir ~/venv/
python3 -m venv ~/venv/dbt/             # create the environment
source ~/venv/dbt/bin/activate         # activate the environment
```

`export DBT_PROFILES_DIR=./.dbt/`
OR
`dbt run --profiles-dir ./.dbt/`

Once DBT_PROFILES_DIR is exported, run:
`dbt run -m tag:store`
or whichever desired tag



# Other Notes

`/archive/` is where superseded SQL files go for reference.

This repo is uploaded with full permission from the owner and is free for use and modification, please notify me.