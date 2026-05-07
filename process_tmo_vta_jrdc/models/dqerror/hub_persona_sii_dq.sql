{{ config(  materialized='incremental',
            alias='hub_persona_sii_dq',
            partition_by={ "field": 'data_date',
                           "data_type": 'date',
                           "granularity":'day'
                         }  )}} 

{%- set data_date = var("periodo") -%}

select * from (
  {{dq_rule (dataset='production_stage',
            stg_table='tbl_feesii_tmo_vta_per_jrdc_anu' ,
            stg_column='rut',
            notif='WARNING',
            rule='C1',
            dq_message='El concepto debe estar informado',
            dq_query=dq_is_null("rut"),
            fecha=data_date
            )}}

    UNION ALL

    {{dq_rule (dataset='production_stage',
            stg_table='tbl_feesii_tmo_vta_per_jrdc_anu' ,
            stg_column='dv',
            notif='WARNING',
            rule='C1',
            dq_message='El concepto debe estar informado',
            dq_query=dq_is_null("dv"),
            fecha=data_date
            )}}

)
where error_count > 0