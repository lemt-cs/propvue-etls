# This is an ETL workflow to run Domain search outside of local machine to optimize dashboards development by isolating ETL workflows.

## At the start of the day, run this to cleanup the Atlas database:
1. python L_atlas_to_local.py

## Modify these and push to remote Git to run as Github actions to store data to Atlas.
1. python EL_search_smaps_mapped.py
2. Optional python EL_search_smaps_mapped_local.py for local and github running

