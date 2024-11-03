import atexit
import datetime
import json
import logging
import os
import sys
import time

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)
apscheduler_logger = logging.getLogger("apscheduler")
apscheduler_logger.setLevel(logging.CRITICAL)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def http_get(task):
    headers = task.get("headers", {})
    response = requests.get(task["url"], headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"GET {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"GET {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_head(task):
    headers = task.get("headers", {})
    response = requests.head(task["url"], headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"HEAD {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"HEAD {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_options(task):
    headers = task.get("headers", {})
    response = requests.options(task["url"], headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"OPTIONS {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"OPTIONS {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_trace(task):
    headers = task.get("headers", {})
    response = requests.request("TRACE", task["url"], headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"TRACE {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"TRACE {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_put(task):
    headers = task.get("headers", {})
    response = requests.put(task["url"], json=task.get("data", {}), headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"PUT {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"PUT {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_delete(task):
    headers = task.get("headers", {})
    response = requests.delete(task["url"], headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"DELETE {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"DELETE {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_post(task):
    headers = task.get("headers", {})
    response = requests.post(task["url"], json=task.get("data", {}), headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"POST {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"POST {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


def http_patch(task):
    headers = task.get("headers", {})
    response = requests.patch(task["url"], json=task.get("data", {}), headers=headers)
    if 400 <= response.status_code < 600:
        logger.warning(f"PATCH {task['url']} - Status Code: {response.status_code} - Response: {response.content}")
    else:
        logger.info(f"PATCH {task['url']} - Status Code: {response.status_code} - Response: {response.content}")


http_methods = {
    "GET": http_get,
    "HEAD": http_head,
    "OPTIONS": http_options,
    "TRACE": http_trace,
    "PUT": http_put,
    "DELETE": http_delete,
    "POST": http_post,
    "PATCH": http_patch,
}


def cdcron():
    logger.info(f"Current timezone is {datetime.datetime.now().astimezone().strftime("%Z (%z)")}")
    scheduler = BackgroundScheduler()

    try:
        workloadfile = os.getenv("WORKLOAD_FILE", os.path.join(sys.path[0], "workload.json"))
        with open(workloadfile, encoding="utf=8") as f:
            workload = json.load(f)
    except json.decoder.JSONDecodeError:
        logger.error(f"{workloadfile} is not an JSON file")
        os._exit(1)
    except FileNotFoundError:
        logger.error(f"{workloadfile} doesn't exist or env variable WORKLOAD_FILE not set")
        os._exit(1)
    except Exception as e:
        logger.error(f"{e}")
        os._exit(1)

    for task in workload:
        method = task["method"].upper()
        if method in http_methods:
            trigger = CronTrigger.from_crontab(task["cron"])
            scheduler.add_job(http_methods[method], trigger, args=[task])
            logger.info(f"Scheduled {method} request to {task['url']} with cron '{task['cron']}'")
        else:
            logger.warning(f"Method '{method}' not supported for task: {task}")

    scheduler.start()
    logger.info("Scheduler started...")
    atexit.register(scheduler.shutdown)

    while True:
        time.sleep(1)
