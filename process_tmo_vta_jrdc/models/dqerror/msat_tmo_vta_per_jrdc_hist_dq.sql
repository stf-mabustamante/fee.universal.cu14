{{ config(  materialized='incremental',
            alias='msat_tmo_vta_per_jrdc_hist_dq',
            partition_by={ "field": 'data_date',
                           "data_type": 'date',
                           "granularity":'day'
                         }  )}} 

{%- set data_date = var("periodo") -%}

select * from (

  {{dq_rule (dataset='production_stage',
            stg_table='tbl_feesii_tmo_vta_per_jrdc_anu' ,
            stg_column='fecha_inicio_actividad_vige',
            notif='WARNING',
            rule='C5',
            dq_message='La fecha debe venir en formato AAAA-MM-DD y el año (AAAA) debe ser mayor o igual 1800. ',
            dq_query=dq_validez_fecha("fecha_inicio_actividad_vige"),
            fecha=data_date
            )}}

  UNION ALL

  {{dq_rule (dataset='production_stage',
            stg_table='tbl_feesii_tmo_vta_per_jrdc_anu' ,
            stg_column='fecha_termino_giro',
            notif='WARNING',
            rule='C5',
            dq_message='La fecha debe venir en formato AAAA-MM-DD y el año (AAAA) debe ser mayor o igual 1800. ',
            dq_query=dq_validez_fecha("fecha_termino_giro"),
            fecha=data_date
            )}}
  
  UNION ALL

  {{dq_rule (dataset='production_stage',
            stg_table='tbl_feesii_tmo_vta_per_jrdc_anu' ,
            stg_column='fecha_primera_inscripcion_ac',
            notif='WARNING',
            rule='C5',
            dq_message='La fecha debe venir en formato AAAA-MM-DD y el año (AAAA) debe ser mayor o igual 1800. ',
            dq_query=dq_validez_fecha("fecha_primera_inscripcion_ac"),
            fecha=data_date
            )}}


)
where error_count > 0