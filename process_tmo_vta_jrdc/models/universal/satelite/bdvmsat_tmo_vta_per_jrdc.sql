 {{ config ( materialized='table',
            schema= var('dominio'),
            alias='bdvmsat_tmo_vta_per_jrdc'
 )}}


    WITH sat as (
        SELECT {{attr_bdvmsat_tmo_vta_per_jrdc()}} 
        FROM {{ source('production_stage', 'tbl_feesii_tmo_vta_per_jrdc_anu') }}
        LEFT JOIN (SELECT MAX(periodo_anu) as max_periodo FROM {{ source('production_stage', 'tbl_feesii_tmo_vta_per_jrdc_anu') }}) b

        ON periodo_anu = b.max_periodo
        WHERE max_periodo IS NOT NULL
    )
    SELECT {{attr_tramo_venta()}}
    FROM sat