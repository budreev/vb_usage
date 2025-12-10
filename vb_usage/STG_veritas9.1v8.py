#!/usr/bin/env python3
import logging
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import urljoin


import requests
import urllib3
from prometheus_client import start_http_server, Gauge
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

NBU_API_URL = os.getenv('NBU_API_URL')
NBU_API_KEY = os.getenv('NBU_API_KEY')
EXPORTER_PORT = os.getenv('EXPORTER_PORT')
REQUEST_TIMEOUT = os.getenv('REQUEST_TIMEOUT')
MAX_RETRIES = os.getenv('MAX_RETRIES')
HOURS_24 = os.getenv('HOURS_24')
SCRAPE_INTERVAL = os.getenv('SCRAPE_INTERVAL')


# Отключение предупреждений о SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOG_FILE = Path(__file__).resolve().with_suffix('.log')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Метрики Prometheus
gauge_jobs_status = Gauge('nbu_jobs_status', 'Backup Job Status',
                          ['status', 'jobType', 'startTime', 'endTime', 'jobId', 'policyName', 'jobState'])
gauge_24h_jobs_status = Gauge('nbu_24h_jobs_status', 'Backup Job Status (last 24 hours)',
                              ['status', 'jobType', 'startTime', 'endTime', 'jobId', 'policyName', 'jobState'])
gauge_storage_used_capacity = Gauge('nbu_storage_used_capacity', 'Storage Used Capacity', ['storage_id'])
gauge_storage_free_capacity = Gauge('nbu_storage_free_capacity', 'Storage Free Capacity', ['storage_id'])
gauge_storage_total_capacity = Gauge('nbu_storage_total_capacity', 'Storage Total Capacity', ['storage_id'])

headers = {
    'Authorization': NBU_API_KEY,
    'Accept': 'application/vnd.netbackup+json;version=6.0'
}

session = requests.Session()
retries = Retry(
    total=MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)


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
        return time_diff <= float(HOURS_24 * 3600)
    except (ValueError, TypeError):
        return False


# Собираем метрики
def collect_metrics():
    def fetch_all_pages(initial_url, label):
        """Получить все страницы ответа NetBackup, следуя ссылкам next."""
        url = initial_url
        visited = set()
        page_count = 0
        item_count = 0

        while url and url not in visited:
            visited.add(url)
            page_count += 1

            response = session.get(url, headers=headers, verify=False, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()

            current_page_items = payload.get('data', []) or []
            item_count += len(current_page_items)
            logger.info(
                "Fetched %s page %s with %s items from %s", label, page_count, len(current_page_items), url
            )

            for item in current_page_items:
                yield item

            links = payload.get('links', {}) or {}
            next_link = links.get('next')
            next_href = next_link.get('href') if isinstance(next_link, dict) else next_link
            url = urljoin(url, next_href) if next_href else None

        logger.info("Finished fetching %s: %s pages, %s total items", label, page_count, item_count)

    # Метрики заданий
    def job_collect():
        jobs_url = f"{NBU_API_URL}/admin/jobs?page%5Blimit%5D=200"
        recent_threshold = (datetime.utcnow() - timedelta(hours=float(HOURS_24))).timestamp()

        gauge_jobs_status.clear()
        gauge_24h_jobs_status.clear()

        for job in fetch_all_pages(jobs_url, "jobs"):
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
            if start_time and start_time >= recent_threshold:
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

    # Метрики хранилища — собираем все страницы один раз
    def storage_collect():
        storage_url = f"{NBU_API_URL}/storage/storage-units?page%5Blimit%5D=200"

        gauge_storage_used_capacity.clear()
        gauge_storage_total_capacity.clear()
        gauge_storage_free_capacity.clear()

        for storage in fetch_all_pages(storage_url, "storage-units"):
            attrs = storage.get('attributes', {})
            storage_id = storage.get('id', 'unknown')

            gauge_storage_used_capacity.labels(storage_id=storage_id).set(attrs.get('usedCapacityBytes'))
            gauge_storage_total_capacity.labels(storage_id=storage_id).set(attrs.get('totalCapacityBytes'))
            gauge_storage_free_capacity.labels(storage_id=storage_id).set(attrs.get('freeCapacityBytes'))

    storage_collect()


if __name__ == '__main__':
    print(f"Starting NetBackup exporter on port {EXPORTER_PORT}")
    start_http_server(EXPORTER_PORT)

    while True:
        collect_metrics()
        time.sleep(float(SCRAPE_INTERVAL))
