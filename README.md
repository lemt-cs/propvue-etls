# This is an ETL workflow to run Domain search outside of local machine to optimize dashboards development by isolating ETL workflows.

1. python EL_search_smaps_mapped.py
2. Optional python EL_search_smaps_mapped_local.py for local and github running
3. python L_atlas_to_local.py
4. python L_cosmos_to_local.py = Always respond no as this runs 24/7 in Azure VM.