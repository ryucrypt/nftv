from time import sleep
import requests
import json
import logging
import logging.handlers
import os
import sys


SLEEP = 1
SUCCESS = 5832585
FAIL = 16734296
TIMEOUT = 60

def post_alert(type, job, msg):
    with open("config.json") as file:
        ENV = json.load(file)
    if type == "pass":
        color = SUCCESS
        title = f"{job} - PASSED"
        content = ""
    else:
        color = FAIL
        title = f"{job} - FAILED"
        content = ENV["mention"]
    output = {
        "content": content,
        "embeds": [
            {
                "title": title,
                "color": color,
                "description": msg
            }
        ],
        "attachments": []
    }

    r = requests.post(ENV["webhook"], json=output, timeout=TIMEOUT)
    r.raise_for_status()
    sleep(SLEEP)

def setup_logger(name):
    with open("config.json") as file:
        ENV = json.load(file)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(filename=os.path.join(ENV["logs"], f"{name}.log"),
        maxBytes=1024 * 1024, backupCount=5)
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s'))
    logger.addHandler(handler)

    return logger