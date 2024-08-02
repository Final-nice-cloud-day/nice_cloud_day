from slack_sdk import WebClient
from airflow.hooks.base import BaseHook
import pendulum
import os
import re



class SlackAlert:
    def __init__(self, channel):
        self.channel = channel
        self.token = 'xoxb-7346747924534-7513805658660-6w9C6HRZD14KxjWAaKEPlw9J'
        self.client = WebClient(token=self.token)
        self.kst = pendulum.timezone("Asia/Seoul")
        
    def get_task_logs(self, task_instance):
        log_base = '/opt/airflow/logs'
        try_number_chk = task_instance.try_number - 1
        log_path = os.path.join(
            log_base,
            f"dag_id={task_instance.dag_id}",
            f"run_id={task_instance.run_id}",
            f"task_id={task_instance.task_id}",
            f"attempt={try_number_chk}.log"
        )
        if os.path.exists(log_path):
            print(f"Log file found at: {log_path}")
            with open(log_path, 'r') as log_file:
                logs = log_file.read()
                filtered_logs = self.filter_logs(logs)
                return filtered_logs
        return '로그 정보가 없습니다.'
    
    def filter_logs(self, logs):
        pattern = re.compile(r'.*(INFO - \d+ rows 데이터를 읽었습니다.|INFO - \d+ 건 저장되었습니다.|INFO - 성공적으로 적재된 행 수: \d+|ERROR - .+).*')
        filtered_logs = "\n".join(pattern.findall(logs))
        return filtered_logs

    def failure_alert(self, context):
        task_instance = context.get("task_instance")
        logs = self.get_task_logs(task_instance)
        text = (
            f'\n`DAG` : {task_instance.dag_id}'
            f'\n`Task` : {task_instance.task_id}'
            f'\n`Run ID` : {context.get("run_id")}'
            f'\n`Date` : {pendulum.now(self.kst).strftime("%Y-%m-%d %H:%M:%S")}'
            f'\n`Logs` : {logs}'
        )
        self.client.chat_postMessage(
            channel=self.channel,
            text='Process Failed :cloud:',
            attachments=[{
                "mrkdwn_in": ["text"],
                "title": "",
                "text": text,
                "color": "danger"
            }]
        )

    def success_alert(self, context):
        task_instance = context.get("task_instance")
        logs = self.get_task_logs(task_instance)
        text = (
            f'\n`DAG` : {task_instance.dag_id}'
            f'\n`Task` : {task_instance.task_id}'
            f'\n`Run ID` : {context.get("run_id")}'
            f'\n`Date` : {pendulum.now(self.kst).strftime("%Y-%m-%d %H:%M:%S")}'
            f'\n`Logs` : {logs}'
        )
        self.client.chat_postMessage(
            channel=self.channel,
            text='Process Success :sunny:',
            attachments=[{
                "mrkdwn_in": ["text"],
                "title": "",
                "text": text,
                "color": "good"
            }]
        )

    def default_alert(self, context):
        text = context
        self.client.chat_postMessage(
            channel=self.channel,
            text=text,
            attachments=[{
                "mrkdwn_in": ["text"],
                "title": "",
                "text": text,
                "color": "good"
            }]
        )
