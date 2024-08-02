from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
from common.alert import SlackAlert
import pendulum
import logging


slackbot = SlackAlert('#airflow_log') 
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'chansu',
    'depends_on_past': True,  # 선행작업의존여부N
    'start_date': pendulum.datetime(2024, 8, 1, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'on_failure_callback': slackbot.failure_alert,
    'on_success_callback': slackbot.success_alert,
}

def mart_comr_api_iist(data_interval_end):
    logging.info("redshift 적재 시작")
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    #base_dt=data_interval_end.in_timezone(kst).strftime('%m%d')
    base_date = data_interval_end.in_timezone(kst).strftime('%Y%m%d')
    
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS temp_comr_api_list(
        DIV_NM VARCHAR(60),
        reg_name VARCHAR(60),
        tm_ef VARCHAR(8),
        min_ta NUMERIC(9,4),
        max_ta NUMERIC(9,4),
        wf_pre_cd VARCHAR(60),
        wf_info VARCHAR(60),
        data_key TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    cursor.execute("TRUNCATE TABLE temp_comr_api_list;")
    
    insert_temp_query = f"""
    INSERT INTO temp_comr_api_list (
        DIV_NM,
        reg_name,
        tm_ef,
        min_ta,
        max_ta,
        wf_pre_cd,
        wf_info,
        data_key,
        created_at,
        updated_at
        )
        SELECT '기상청' as DIV_NM,
        T3.reg_name,
        to_char(T2.tm_ef, 'YYYYMMDD') as tm_ef,
        T2.min_ta,
        T2.max_ta,
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
    where 1=1
    AND T2.tm_fc = '{base_date}' || '06'
    and T2.reg_id ='11B10101'
    union ALL
    select 'WeatherAPI' as DIV_NM,
        '서울' as reg_name,
        date as tm_ef,
        min_temp_c as min_ta,
        max_temp_c as max_ta,
        to_char(total_precip_mm, '99.99') AS wf_pre_cd,
        condition AS wf_info,
        data_key,
        created_at,
        updated_at
    from raw_data.WeatherAPI_LIST
    WHERE 1=1
    AND data_key = '{base_date}' || '07'
    AND date BETWEEN TO_CHAR('{base_date}'::date + INTERVAL '2 day', 'YYYYMMDD') 
        AND TO_CHAR('{base_date}'::date + INTERVAL '10 day', 'YYYYMMDD');
    """
    cursor.execute(insert_temp_query)
    affected_rows = cursor.rowcount
    logging.info(f"{affected_rows} rows 데이터를 읽었습니다.")
    
    merge_query = """
    MERGE INTO mart_data.comr_api_list
    USING temp_comr_api_list AS source
    ON mart_data.comr_api_list.div_nm = source.div_nm 
    AND mart_data.comr_api_list.reg_name = source.reg_name 
    and mart_data.comr_api_list.tm_ef = source.tm_ef
    WHEN MATCHED THEN
    UPDATE SET
        DIV_NM=source.DIV_NM,
        reg_name=source.reg_name,
        tm_ef=source.tm_ef,
        min_ta=source.min_ta,
        max_ta=source.max_ta,
        wf_pre_cd=source.wf_pre_cd,
        wf_info=source.wf_info,
        data_key=source.data_key,
        created_at=source.created_at,
        updated_at=source.updated_at
    WHEN NOT MATCHED THEN
    INSERT (DIV_NM, reg_name, tm_ef, min_ta, max_ta, wf_pre_cd, wf_info, data_key, created_at, updated_at)
    VALUES (source.DIV_NM, source.reg_name, source.tm_ef, source.min_ta, source.max_ta, source.wf_pre_cd, source.wf_info, source.data_key, source.created_at, source.updated_at);
    """
    try:
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        cursor.execute("TRUNCATE TABLE temp_comr_api_list;")
        conn.commit()
        logging.info(f"성공적으로 적재된 행 수: {affected_rows}")
    except Exception as e:
        logging.error(f"Redshift 로드 실패: {e}")
        raise ValueError(f"Redshift 로드 실패: {e}")
    finally:
        cursor.close()
        conn.close()
    
with DAG(
    'mart_comr_api_iist_1',
    default_args=default_args,
    description='마트테이블 기상예보 비교 적재',
    schedule_interval='0 7 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['중기','Daily', '1time','mart'],
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
    
    wait_for_fct_medm_reg_task = ExternalTaskSensor(
        task_id='wait_for_fct_medm_reg_task',
        external_dag_id='fct_medm_reg_to_s3_and_redshift',
        external_task_id='fct_medm_reg_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    wait_for_weatherAPI_task = ExternalTaskSensor(
        task_id='wait_for_weatherAPI_task',
        external_dag_id='weatherAPI_to_s3_redshift_task',
        external_task_id='weatherAPI_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    mart_comr_api_insert_task = PythonOperator(
        task_id='mart_comr_api_iist',
        python_callable=mart_comr_api_iist,
        execution_timeout=pendulum.duration(hours=1),
        dag=dag,
    )
    
    [wait_for_wc_task, wait_for_fct_medm_reg_task, wait_for_weatherAPI_task] >> mart_comr_api_insert_task
