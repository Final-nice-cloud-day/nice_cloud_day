from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
import logging
import pendulum

kst = pendulum.timezone("Asia/Seoul")  # 스케줄링을 한국 시간 기준으로 하기 위해서 설정

# 기본 DAG 설정
default_args = {
    'owner': 'bongho',
    'depends_on_past': True,
    'start_date': pendulum.datetime(2024, 8, 5, 6, 0, 0, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

dag = DAG(
    'Create_mart_table_fcst_shrt_v2',
    default_args=default_args,
    description='단기 육상 예보 마트 테이블 생성 (적재하는 방식으로)',
    schedule_interval='0 6,12,18 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['단기', 'Daliy', '3time', 'mart_data']
)

def mart_fcst_shrt_close_time():
    logging.info("Redshift 마트 테이블 적재 시작")
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try:
        # 임시테이블 생성 부터
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS temp_fcst_shrt_mart (
            reg_id VARCHAR(60),
            doname VARCHAR(60),
            reg_name VARCHAR(60),
            tm_fc TIMESTAMP,
            tm_ef TIMESTAMP,
            ta INT,
            rn_st INT,
            wf_pre_cd VARCHAR(10),
            wf_info VARCHAR(60),
            iso_3166_code VARCHAR(10),
            data_key TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """)
        logging.info("임시 테이블 생성 성공 : temp_fcst_shrt_mart")
        
        cursor.execute("TRUNCATE TABLE temp_fcst_shrt_mart;")
        
        # 임시 테이블에 데이터 넣기 (온도 -99 데이터 삭제 포함)
        insert_temp_table = """
        INSERT INTO temp_fcst_shrt_mart (
        reg_id,
        doname,
        reg_name,
        tm_fc,
        tm_ef,
        ta,
        rn_st,
        wf_pre_cd,
        wf_info,
        iso_3166_code,
        data_key,
        created_at,
        updated_at
        )
        SELECT 
            t1.reg_id,
            CASE
                WHEN (t1.reg_id LIKE '11B10101') THEN '서울특별시'
                WHEN (t1.reg_id LIKE '11C20404') THEN '세종특별자치시'
                WHEN (t1.reg_id LIKE '11C20401') THEN '대전광역시'
                WHEN (t1.reg_id LIKE '11H10701') THEN '대구광역시'
                WHEN (t1.reg_id LIKE '11H20101') THEN '울산광역시'
                WHEN (t1.reg_id LIKE '11H20201') THEN '부산광역시'
                WHEN (t1.reg_id LIKE '11F20501') THEN '광주광역시'
                WHEN (t1.reg_id LIKE '11B20201%' OR t1.reg_id LIKE '11A00101%') THEN '인천광역시'
                WHEN (t1.reg_id IN ('11E00101', '11E00102')) THEN '경상북도'
                WHEN (t1.reg_id LIKE '11B%') THEN '경기도'
                WHEN (t1.reg_id LIKE '11C10%') THEN '충청북도'
                WHEN (t1.reg_id LIKE '11C20%') THEN '충청남도'
                WHEN (t1.reg_id LIKE '11D%') THEN '강원도'
                WHEN (t1.reg_id LIKE '11F10%' OR t1.reg_id LIKE '21F1%') THEN '전라북도'
                WHEN (t1.reg_id LIKE '11F20%' OR t1.reg_id LIKE '21F2%') THEN '전라남도'
                WHEN (t1.reg_id LIKE '11G0%') THEN '제주특별자치도'
                WHEN (t1.reg_id LIKE '11H10%') THEN '경상북도'
                WHEN (t1.reg_id LIKE '11H20%') THEN '경상남도'
                ELSE '북한'
            END AS doname,
            t2.reg_name, 
            t1.tm_fc, 
            t1.tm_ef, 
            t1.ta, 
            t1.rn_st, 
            t1.wf_pre_cd, 
            t1.wf_info,
            t3.iso_3166_code, 
            t1.data_key,
            t1.created_at,
            t1.updated_at
        FROM raw_data.fct_afs_dl_info t1
        INNER JOIN raw_data.fct_shrt_reg_list t2
            ON t1.reg_id = t2.reg_id
            AND t1.data_key = t2.data_key
        LEFT JOIN mart_data.korea_region_codes t3
            ON t2.reg_name = t3.reg_name
        WHERE t1.ta <> -99;
        """
        cursor.execute(insert_temp_table)
        logging.info("임시 테이블에 데이터 적재 완료")

        # ISO-3166 코드 매핑, 경기도 광주 제거
        cursor.execute("""
        UPDATE temp_fcst_shrt_mart
        SET iso_3166_code = NULL
        WHERE reg_id = '11B20702';
        """)
        logging.info("경기도 광주 ISO-3166 코드 매핑 제거 완료")
        
        merge_query = """
        MERGE INTO mart_data.fcst_shrt_mart_st
        USING temp_fcst_shrt_mart AS source
            ON mart_data.fcst_shrt_mart_st.reg_id = source.reg_id 
            AND mart_data.fcst_shrt_mart_st.tm_fc = source.tm_fc 
            AND mart_data.fcst_shrt_mart_st.tm_ef = source.tm_ef
            and mart_data.fcst_shrt_mart_st.data_key = source.data_key
        WHEN MATCHED THEN
        UPDATE SET
            reg_id = source.reg_id,
            doname = source.doname,
            reg_name = source.reg_name,
            tm_fc = source.tm_fc,
            tm_ef = source.tm_ef,
            ta = source.ta,
            rn_st = source.rn_st,
            wf_pre_cd = source.Wf_pre_cd,
            wf_info = source.wf_info,
            iso_3166_code = source.iso_3166_code,
            data_key = source.data_key,
            created_at = source.created_at,
            updated_at = source.updated_at
        WHEN NOT MATCHED THEN
        INSERT (reg_id, doname, reg_name, tm_fc, tm_ef, ta, rn_st, wf_pre_cd, wf_info, iso_3166_code, data_key, created_at, updated_at)
        VALUES (source.reg_id, source.doname, source.reg_name, source.tm_fc, source.tm_ef, source.ta, source.rn_st, source.wf_pre_cd, source.wf_info, source.iso_3166_code, source.data_key, source.created_at, source.updated_at);
        """
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        cursor.execute("TRUNCATE TABLE temp_fcst_shrt_mart;")
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
        logging.info("모든 작업이 성공적으로 완료되었습니다.")

wait_for_fct_shrt_reg_to_s3_redshift = ExternalTaskSensor(
    task_id='wait_for_fct_shrt_reg_to_s3_redshift',
    external_dag_id='fct_shrt_reg_to_s3_redshift',
    external_task_id=None,
    check_existence=True,
    dag=dag,
)

wait_for_fct_afs_dl_to_s3_redshift = ExternalTaskSensor(
    task_id='wait_for_fct_afs_dl_to_s3_redshift',
    external_dag_id='fct_afs_dl_to_s3_redshift',
    external_task_id=None,
    check_existence=True,
    dag=dag,
)

execute_sql_task = PythonOperator(
    task_id='execute_sql',
    python_callable=mart_fcst_shrt_close_time,
    dag=dag,
)

[wait_for_fct_shrt_reg_to_s3_redshift, wait_for_fct_afs_dl_to_s3_redshift] >> execute_sql_task
