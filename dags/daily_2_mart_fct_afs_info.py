from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
import pendulum
import logging

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'chansu',
    'depends_on_past': True,  # 선행작업의존여부
    'start_date': pendulum.datetime(2024, 7, 29, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def mart_fct_afs_info():
    logging.info("redshift 적재 시작")
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
        
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS temp_fct_afs_info (
        reg_name VARCHAR(60),
        tm_fc TIMESTAMP,
        tm_ef TIMESTAMP,
        min_ta NUMERIC(9,4),
        max_ta NUMERIC(9,4),
        wf_sky_cd VARCHAR(60),
        Wf_pre_cd VARCHAR(60),
        wf_info VARCHAR(60),
        rn_st INT,
        wc_reg_id VARCHAR(60),
        wl_reg_id VARCHAR(60),
        data_key TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    cursor.execute("TRUNCATE TABLE temp_fct_afs_info;")
        
    insert_temp_query = """
    INSERT INTO temp_fct_afs_info (
    reg_name,
    tm_fc,
    tm_ef,
    min_ta,
    max_ta,
    wf_sky_cd,
    wf_pre_cd,
    wf_info,
    rn_st,
    wc_reg_id,
    wl_reg_id,
    data_key,
    created_at,
    updated_at
    )
    WITH MAX_TM_FC AS (
        SELECT reg_id, MAX(tm_fc) AS max_tm_fc
        FROM raw_data.fct_afs_wc_info
        GROUP BY reg_id
    )
    SELECT DISTINCT
        T3.reg_name,
        T2.tm_fc,
        T2.tm_ef,
        T2.min_ta,
        T2.max_ta,
        CASE 
            WHEN T4.wf_sky_cd='WB01' THEN '맑음'
            WHEN T4.wf_sky_cd='WB02' THEN '구름조금'
            WHEN T4.wf_sky_cd='WB03' THEN '구름많음'
            WHEN T4.wf_sky_cd='WB04' THEN '흐림'
            ELSE '정보없음'
        END AS wf_sky_cd,
        CASE 
            WHEN T4.wf_pre_cd='WB00' THEN '강수없음'
            WHEN T4.wf_pre_cd='WB09' THEN '비'
            WHEN T4.wf_pre_cd='WB10' THEN '소나기'
            WHEN T4.wf_pre_cd='WB11' THEN '비/눈'
            WHEN T4.wf_pre_cd='WB13' THEN '눈/비'
            WHEN T4.wf_pre_cd='WB12' THEN '눈'
            ELSE '정보없음'
        END AS wf_pre_cd,
        CASE 
            WHEN T4.wf_info IS NULL THEN '정보없음'
            ELSE T4.wf_info
        END AS wf_info,
        CASE 
            WHEN T4.rn_st IS NULL THEN NULL
            ELSE T4.rn_st
        END AS rn_st,
        T2.reg_id as wc_reg_id,
        T4.reg_id as wl_reg_id,
        T2.data_key,
        T2.created_at,
        T2.updated_at
    FROM 
        raw_data.fct_afs_wc_info T2
    LEFT JOIN 
        raw_data.fct_medm_reg_list T3 
    ON T2.reg_id = T3.reg_id
    LEFT JOIN 
        raw_data.fct_afs_wl_info T4 
    ON LEFT(T2.reg_id, 3) = LEFT(T4.reg_id, 3)
    AND T2.tm_fc = T4.tm_st
    AND T2.tm_ef = T4.tm_ed
    INNER JOIN 
        MAX_TM_FC M ON T2.reg_id = M.reg_id AND T2.tm_fc = M.max_tm_fc
    WHERE 1=1
    ORDER BY 
        T2.reg_id, T2.tm_fc, T2.tm_ef;
"""
    cursor.execute(insert_temp_query)
    
    merge_query = """
    MERGE INTO mart_data.fct_afs_info
    USING temp_fct_afs_info AS source
    ON mart_data.fct_afs_info.reg_name = source.reg_name 
    AND mart_data.fct_afs_info.tm_fc = source.tm_fc 
    AND mart_data.fct_afs_info.tm_ef = source.tm_ef
    and mart_data.fct_afs_info.data_key = source.data_key
    WHEN MATCHED THEN
    UPDATE SET
        reg_name  = source.reg_name,
        tm_fc = source.tm_fc,
        tm_ef = source.tm_ef,
        min_ta  = source.min_ta,
        max_ta  = source.max_ta,
        wf_sky_cd = source.wf_sky_cd,
        Wf_pre_cd = source.Wf_pre_cd,
        wf_info = source.wf_info,
        rn_st = source.rn_st,
        wc_reg_id = source.wc_reg_id,
        wl_reg_id = source.wl_reg_id,
        data_key  = source.data_key,
        created_at  = source.created_at,
        updated_at  = source.updated_at
    WHEN NOT MATCHED THEN
    INSERT (reg_name, tm_fc, tm_ef, min_ta, max_ta, wf_sky_cd, Wf_pre_cd, wf_info, rn_st, wc_reg_id, wl_reg_id, data_key, created_at, updated_at)
    VALUES (source.reg_name, source.tm_fc, source.tm_ef, source.min_ta, source.max_ta, source.wf_sky_cd, source.Wf_pre_cd, source.wf_info, source.rn_st, source.wc_reg_id, source.wl_reg_id, source.data_key, source.created_at, source.updated_at);
    """
    try:
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        cursor.execute("TRUNCATE TABLE temp_kma_sfcdd3_stn_iist;")
        conn.commit()
        if affected_rows == 0:
            logging.error("ERROR: 적재할 데이터가 없습니다.")
            raise ValueError("ERROR: 적재할 데이터가 없습니다.")
        else:
            logging.info(f"성공적으로 적재된 행 수: {affected_rows}")
    except Exception as e:
        logging.error(f"Redshift 로드 실패: {e}")
        raise ValueError(f"Redshift 로드 실패: {e}")
    finally:
        cursor.close()
        conn.close()
    
with DAG(
    'mart_fct_afs_info_insert',
    default_args=default_args,
    description='mart_fct_afs_info insert',
    schedule_interval='0 7,19 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['중기', 'Daily', '2 time', 'mart'],
) as dag:
    dag.timezone = kst
    
    #선행 DAG
    wait_for_wc_task = ExternalTaskSensor(
        task_id='wait_for_wc_task',
        external_dag_id='fct_afs_wc_to_s3_and_redshift',
        external_task_id='fct_afs_wc_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    wait_for_wl_task = ExternalTaskSensor(
        task_id='wait_for_wl_task',
        external_dag_id='fct_afs_wl_to_s3_and_redshift',
        external_task_id='fct_afs_wl_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    mart_fct_afs_info_insert_task = PythonOperator(
        task_id='mart_fct_afs_info',
        python_callable=mart_fct_afs_info,
        execution_timeout=pendulum.duration(hours=1),
        dag=dag,
    )
    
    [wait_for_wc_task, wait_for_wl_task] >> mart_fct_afs_info_insert_task
