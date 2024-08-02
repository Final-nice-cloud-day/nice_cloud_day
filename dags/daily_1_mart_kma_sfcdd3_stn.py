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

def mart_kma_sfcdd3_stn_iist():
    logging.info("redshift 적재 시작")
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
        
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS temp_kma_sfcdd3_stn_iist(
        stn_ko VARCHAR(60),
        tm_id VARCHAR(8),
        ta_avg NUMERIC(9,4),
        ta_min NUMERIC(9,4),
        ta_max NUMERIC(9,4),
        WS_MAX NUMERIC(9,4),
        HM_MIN NUMERIC(9,4),
        RN_DAY NUMERIC(9,4),
        SD_MAX NUMERIC(9,4),
        stn_id VARCHAR(60),
        data_key TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """)
    cursor.execute("TRUNCATE TABLE temp_kma_sfcdd3_stn_iist;")
        
    insert_temp_query = """
    INSERT INTO temp_kma_sfcdd3_stn_iist (
    stn_ko,
    tm_id,
    ta_avg,
    ta_min,
    ta_max,
    WS_MAX,
    HM_MIN,
    RN_DAY,
    SD_MAX,
    stn_id,
    data_key,
    created_at,
    updated_at
    )
    SELECT 
        T2.stn_ko, 
        T1.tm_id, 
        T1.ta_avg, 
        T1.ta_min, 
        T1.ta_max,
        case when T1.WS_MAX = -9 then 0
            else T1.WS_MAX 
        end as WS_MAX,
        case when T1.HM_MIN = -9 then NULL
            else T1.HM_MIN 
        end as HM_MIN,
        case when T1.RN_DAY = -9 then 0
            else T1.RN_DAY 
        end as RN_DAY,
        case when T1.SD_MAX = -9 then 0
            else T1.SD_MAX 
        end as SD_MAX,
        T1.stn_id,
        T1.data_key,
        T1.created_at,
        T1.updated_at
    from   raw_data.kma_sfcdd3_list T1
    left join raw_data.stn_inf_info T2
    on T1.stn_id = T2.stn_id
    where SUBSTRING(T1.tm_id, 5, 4) = TO_CHAR(SYSDATE, 'MMDD')
    AND T1.ta_avg <> -99
    AND T1.ta_min <> -99
    AND T1.ta_max <> -99;
    """
    cursor.execute(insert_temp_query)
    
    merge_query = """
    MERGE INTO mart_data.kma_sfcdd3_stn_iist
    USING temp_kma_sfcdd3_stn_iist AS source
    ON mart_data.kma_sfcdd3_stn_iist.stn_ko = source.stn_ko 
    AND mart_data.kma_sfcdd3_stn_iist.tm_id = source.tm_id 
    and mart_data.kma_sfcdd3_stn_iist.data_key = source.data_key
    WHEN MATCHED THEN
    UPDATE SET
        stn_ko=source.stn_ko,
        tm_id=source.tm_id,
        ta_avg=source.ta_avg,
        ta_min=source.ta_min,
        ta_max=source.ta_max,
        WS_MAX=source.WS_MAX,
        HM_MIN=source.HM_MIN,
        RN_DAY=source.RN_DAY,
        SD_MAX=source.SD_MAX,
        stn_id=source.stn_id,
        data_key =source.data_key,
        created_at=source.created_at,
        updated_at=source.updated_at
    WHEN NOT MATCHED THEN
    INSERT (stn_ko, tm_id, ta_avg, ta_min, ta_max, WS_MAX, HM_MIN, RN_DAY, SD_MAX, stn_id, data_key, created_at, updated_at)
    VALUES (source.stn_ko, source.tm_id, source.ta_avg, source.ta_min, source.ta_max, source.WS_MAX, source.HM_MIN, source.RN_DAY, source.SD_MAX, source.stn_id, source.data_key, source.created_at, source.updated_at);
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
    'mart_kma_sfcdd3_stn_list_insert',
    default_args=default_args,
    description='마트테이블 종관기상관측 적재',
    schedule_interval='0 7 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['중기', 'Daily', '1 time', 'mart'],
) as dag:
    dag.timezone = kst
    
    #선행 DAG
    wait_for_for_kma_sfcdd3_task = ExternalTaskSensor(
        task_id='wait_for_kma_sfcdd3_task',
        external_dag_id='kma_sfcdd3_to_s3_and_redshift',
        external_task_id='kma_stcdd3_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    wait_for_stn_inf_task = ExternalTaskSensor(
        task_id='wait_for_stn_inf_task',
        external_dag_id='stn_inf_to_s3_and_redshif',
        external_task_id='stn_inf_to_redshift',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=1800,
        dag=dag,
    )
    
    mart_kma_sfcdd3_stn_iist_insert_task = PythonOperator(
        task_id='mart_kma_sfcdd3_stn_iist',
        python_callable=mart_kma_sfcdd3_stn_iist,
        execution_timeout=pendulum.duration(hours=1),
        dag=dag,
    )
    
    [wait_for_for_kma_sfcdd3_task, wait_for_stn_inf_task] >> mart_kma_sfcdd3_stn_iist_insert_task
