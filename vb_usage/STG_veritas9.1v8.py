#!/usr/bin/env python3
import requests
import urllib3
from prometheus_client import start_http_server, Gauge
import time
from datetime import datetime, timedelta
import socketgit
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Конфигурация
NBU_API_URL = "https://xxx.xxx.xxx.xxx:443/netbackup"
NBU_API_KEY = "xxxx"
EXPORTER_PORT = 9988
SCRAPE_INTERVAL = 30  # Интервал проверки
REQUEST_TIMEOUT = 15  # Таймаут запросов в секундах
MAX_RETRIES = 3  # Количество попыток повторного подключения
HOURS_24 = 24

# Отключение предупреждений о SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Метрики Prometheus
gauge_jobs_status = Gauge('nbu_jobs_status', 'Backup Job Status',
                          ['status', 'jobType', 'startTime', 'endTime', 'jobId', 'policyName', 'jobState'])
gauge_24h_jobs_status = Gauge('nbu_24h_jobs_status', 'Backup Job Status (last 24 hours)',
                              ['status', 'jobType', 'startTime', 'endTime', 'jobId', 'policyName', 'jobState'])
gauge_storage_used_capacity = Gauge('nbu_storage_used_capacity', 'Storage Used Capacity', ['storage_id'])
gauge_storage_free_capacity = Gauge('nbu_storage_free_capacity', 'Storage Free Capacity', ['storage_id'])
gauge_storage_total_capacity = Gauge('nbu_storage_total_capacity', 'Storage Total Capacity', ['storage_id'])


def parse_time(timestamp):
    """Парсер времени, возвращающий Unix timestamp"""
    if not timestamp:
        return 0  # Возвращаем 0 если timestamp отсутствует

    try:
        if isinstance(timestamp, (int, float)):
            # Если это уже число, предполагаем что это Unix timestamp
            return timestamp
        elif isinstance(timestamp, str):
            # Обработка строковых форматов
            if timestamp.endswith('Z'):
                timestamp = timestamp[:-1]
            dt = datetime.fromisoformat(timestamp)
            return dt.timestamp()
    except Exception as e:
        print(f"Error parsing timestamp {timestamp}: {e}", file=sys.stderr)

    return 0  # Возвращаем 0 в случае ошибки


def is_within_24h(timestamp):
    """Проверяет, находится ли timestamp в пределах последних 24 часов"""
    if not timestamp:
        return False

    try:
        timestamp_float = float(timestamp)
        time_diff = time.time() - timestamp_float
        return time_diff <= (HOURS_24 * 3600)
    except (ValueError, TypeError):
        return False


# Собираем метрики
def collect_metrics():
    # Метрики заданий
    def job_collect():
        jobs_url = f"{NBU_API_URL}/admin/jobs?page%5Blimit%5D=1000"
        headers = {'Authorization': NBU_API_KEY, 'Accept': 'application/vnd.netbackup+json;version=6.0'}

        job_response = requests.get(jobs_url, headers=headers, verify=False)
        job_data = job_response.json()

        gauge_jobs_status.clear()
        gauge_24h_jobs_status.clear()

        if 'data' in job_data:
            for job in job_data['data']:
                attrs = job.get('attributes', {})
                job_id = job.get('id', 'unknown')
                start_time = parse_time(attrs.get('startTime'))

                # Пишем основную метрику
                gauge_jobs_status.labels(
                    status=attrs.get('status'),
                    startTime=start_time,
                    endTime=parse_time(attrs.get('endTime')),
                    policyName=attrs.get('policyName'),
                    jobType=attrs.get('jobType'),
                    jobState=attrs.get('state'),
                    jobId=job_id
                ).set(1)

                # Пишем метрику для последних 24 часов, если задание в этом периоде
                if is_within_24h(start_time):
                    gauge_24h_jobs_status.labels(
                        status=attrs.get('status'),
                        startTime=start_time,
                        endTime=parse_time(attrs.get('endTime')),
                        policyName=attrs.get('policyName'),
                        jobType=attrs.get('jobType'),
                        jobState=attrs.get('state'),
                        jobId=job_id
                    ).set(1)

    job_collect()

    # Метрики хранилища
    # Used capacity
    def storage_used_collect():
        storage_used_url = f"{NBU_API_URL}/storage/storage-units?page%5Blimit%5D=10"
        headers = {'Authorization': NBU_API_KEY, 'Accept': 'application/vnd.netbackup+json;version=6.0'}

        storage_used_data_response = requests.get(storage_used_url, headers=headers, verify=False)
        storage_used_data = storage_used_data_response.json()

        gauge_storage_used_capacity.clear()

        if 'data' in storage_used_data:
            for storage_used in storage_used_data['data']:
                attrs = storage_used.get('attributes', {})
                storage_id = storage_used.get('id', 'unknown')
                used_bytes = attrs.get('usedCapacityBytes')

                gauge_storage_used_capacity.labels(storage_id=storage_id).set(used_bytes)

    storage_used_collect()

    # Total Capacity
    def storage_total_collect():
        storage_total_url = f"{NBU_API_URL}/storage/storage-units?page%5Blimit%5D=10"
        headers = {'Authorization': NBU_API_KEY, 'Accept': 'application/vnd.netbackup+json;version=6.0'}

        storage_total_data_response = requests.get(storage_total_url, headers=headers, verify=False)
        storage_total_data = storage_total_data_response.json()

        gauge_storage_total_capacity.clear()

        if 'data' in storage_total_data:
            for storage_total in storage_total_data['data']:
                attrs = storage_total.get('attributes', {})
                storage_id = storage_total.get('id', 'unknown')
                total_bytes = attrs.get('totalCapacityBytes')

                gauge_storage_total_capacity.labels(storage_id=storage_id).set(total_bytes)

    storage_total_collect()

    # Free Capacity
    def storage_free_collect():
        storage_free_url = f"{NBU_API_URL}/storage/storage-units?page%5Blimit%5D=10"
        headers = {'Authorization': NBU_API_KEY, 'Accept': 'application/vnd.netbackup+json;version=6.0'}

        storage_free_data_response = requests.get(storage_free_url, headers=headers, verify=False)
        storage_free_data = storage_free_data_response.json()

        gauge_storage_free_capacity.clear()

        if 'data' in storage_free_data:
            for storage_free in storage_free_data['data']:
                attrs = storage_free.get('attributes', {})
                storage_id = storage_free.get('id', 'unknown')
                free_bytes = attrs.get('freeCapacityBytes')

                gauge_storage_free_capacity.labels(storage_id=storage_id).set(free_bytes)

    storage_free_collect()


if __name__ == '__main__':
    print(f"Starting NetBackup exporter on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)

    while True:
        collect_metrics()
        time.sleep(SCRAPE_INTERVAL)
