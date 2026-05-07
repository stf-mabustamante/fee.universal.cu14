{{ config ( materialized='incremental',
            incremental_strategy='merge',
            alias = 'hub_persona_sii',
            schema= var('dominio'),
            unique_key = ['hk_num_rut']
            )
 }}

SELECT
    {{ attr_hub_persona_sii() }}
FROM
    {{ source('production_stage', 'tbl_feesii_tmo_vta_per_jrdc_anu') }}
WHERE
    periodo_anu='{{var("periodo")}}'