import logging

import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor

kst = pendulum.timezone("Asia/Seoul")  # 스케줄링을 한국 시간 기준으로 하기 위해서 설정

# 기본 DAG 설정
default_args = {
    "owner": "bongho",
    "depends_on_past": True,
    "start_date": pendulum.datetime(2024, 7, 31, 18, 0, 0, tz=kst),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5)
}

dag = DAG(
    "Create_mart_table_fcst_shrt",
    default_args=default_args,
    description="단기 육상 예보 마트 테이블 생성",
    schedule_interval="0 6,12,18 * * *",
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=["단기", "Daliy", "3time", "mart_data"]
)

def mart_fcst_shrt_close_time() -> None:
    redshift_hook = PostgresHook(postgres_conn_id="AWS_Redshift")
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    try:
        # 테이블 삭제
        drop_fcst_shrt_mart_sql = "DROP TABLE IF EXISTS mart_data.fcst_shrt_mart;"
        cursor.execute(drop_fcst_shrt_mart_sql)
        logging.info("테이블 'mart_data.fcst_shrt_mart'이 성공적으로 삭제되었습니다.")

        drop_fcst_shrt_close_time_sql = "DROP TABLE IF EXISTS mart_data.fcst_shrt_close_time;"
        cursor.execute(drop_fcst_shrt_close_time_sql)
        logging.info("테이블 'mart_data.fcst_shrt_close_time'이 성공적으로 삭제되었습니다.")

        # 테이블 생성 및 데이터 삽입
        create_fcst_shrt_mart_sql = """
        CREATE TABLE mart_data.fcst_shrt_mart AS
        SELECT t1.reg_id, t3.region_name, t2.reg_name, t3.iso_3166_code,
               t1.tm_fc,
               t1.tm_ef,
               t1.ta, t1.rn_st,
               t1.wf_pre_cd,
               t1.wf_info,
               t2.data_key
        FROM raw_data.fct_afs_dl_info t1
        INNER JOIN raw_data.fct_shrt_reg_list t2
        ON t1.reg_id = t2.reg_id
        AND t1.data_key = t2.data_key
        INNER JOIN mart_data.korea_region_codes t3
        ON t2.reg_name = t3.reg_name;
        """
        cursor.execute(create_fcst_shrt_mart_sql)
        logging.info("테이블 'mart_data.fcst_shrt_mart'이 성공적으로 생성되었습니다.")

        # 데이터 삭제
        delete_pre_data_sql = """
        DELETE FROM mart_data.fcst_shrt_mart
        WHERE ta = -99;
        """
        cursor.execute(delete_pre_data_sql)
        logging.info("테이블 'mart_data.fcst_shrt_mart'에서 온도 -99인 데이터 삭제 완료.")

        create_fcst_shrt_close_time_sql = """
        CREATE TABLE mart_data.fcst_shrt_close_time AS
        SELECT * FROM mart_data.fcst_shrt_mart
        WHERE tm_ef = (SELECT min(tm_ef) FROM mart_data.fcst_shrt_mart);
        """
        cursor.execute(create_fcst_shrt_close_time_sql)
        logging.info("테이블 'mart_data.fcst_shrt_close_time'이 성공적으로 생성되었습니다.")

        # 데이터 삭제
        delete_from_fcst_shrt_close_time_sql = """
        DELETE FROM mart_data.fcst_shrt_close_time
        WHERE reg_id = '11B20702';
        """
        cursor.execute(delete_from_fcst_shrt_close_time_sql)
        logging.info("테이블 'mart_data.fcst_shrt_close_time'에서 경기도 광주 데이터 삭제 완료.")

        conn.commit()
        logging.info("모든 작업이 성공적으로 완료되었습니다.")
    except Exception as e:
        conn.rollback()
        logging.error(f"작업 실패: {e}")
    finally:
        cursor.close()
        conn.close()

# 외부 DAG 센서 설정
wait_for_fct_shrt_reg_to_s3_redshift = ExternalTaskSensor(
    task_id="wait_for_fct_shrt_reg_to_s3_redshift",
    external_dag_id="fct_shrt_reg_to_s3_redshift",
    external_task_id=None,
    check_existence=True,
    dag=dag,
)

wait_for_fct_afs_dl_to_s3_redshift = ExternalTaskSensor(
    task_id="wait_for_fct_afs_dl_to_s3_redshift",
    external_dag_id="fct_afs_dl_to_s3_redshift",
    external_task_id=None,
    check_existence=True,
    dag=dag,
)

# PythonOperator 설정
execute_sql = PythonOperator(
    task_id="execute_sql",
    python_callable=mart_fcst_shrt_close_time,
    dag=dag,
)

# DAG 의존성 설정
[wait_for_fct_shrt_reg_to_s3_redshift, wait_for_fct_afs_dl_to_s3_redshift] >> execute_sql
