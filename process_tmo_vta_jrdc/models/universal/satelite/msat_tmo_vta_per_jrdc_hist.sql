{{ config ( materialized='incremental',
            incremental_strategy='insert_overwrite',
            alias = 'msat_tmo_vta_per_jrdc_hist',
            schema= var('dominio'),
            partition_by={"field": 'periodo_anu',
                        "data_type": 'date',
                        "granularity":'day' 
                         }
            )
 }}


SELECT
    {{ attr_msat_tmo_vta_per_jrdc_hist() }}
FROM
    {{ source('production_stage', 'tbl_feesii_tmo_vta_per_jrdc_anu') }}
WHERE
    periodo_anu='{{var("periodo")}}'