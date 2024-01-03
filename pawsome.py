#!/usr/bin/python3
import os
from time import sleep
from datetime import datetime
import json

import requests
from tendo import singleton

from common import post_alert, setup_logger

NAME = os.path.splitext(os.path.basename(__file__))[0]
ENV = {}
AA = "https://{}/atomicassets/v1/assets?template_id=730860&page={}&limit=100&order=desc&sort=asset_id"
HEADERS = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
ASSETS = "pawsome"
CONFIG = "drip_config"
RETRY = 3
SLEEP = 1
TIMEOUT = 60

def get_assets(page):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                AA.format(ENV["aa"], page),
                headers={"apikey": ENV["aa_key"]},
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.json()["data"]
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def upload_assets(data):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.post(
                ENV["supabase_url"] + ASSETS,
                headers=HEADERS,
                json=data,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            break
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def update_timestamp():
    retry_count = 0
    while retry_count < RETRY:
        try:
            
            r = requests.post(
                ENV["supabase_url"] + CONFIG,
                headers=HEADERS,
                json=[{"pawsome_update": f"{int(datetime.now().timestamp())}"}],
                timeout=TIMEOUT
            )
            r.raise_for_status()
            break
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

try:
    logger = setup_logger(NAME)
    try:
        me = singleton.SingleInstance()
    except:
        msg = "Already running, exiting"
        logger.error(msg)
        post_alert("fail", NAME, msg)
        exit()

    start = datetime.now()
    with open("config.json") as file:
        ENV = json.load(file)
    HEADERS["apikey"] = ENV["supabase_key"]
    HEADERS["Authorization"] += ENV["supabase_key"]

    page = 1
    while True:
        try:
            data = get_assets(page)
            if len(data) < 1:
                break
        except Exception as err:
            msg = f"Page {page}: Fetching assets failed, {str(err)}"
            logger.exception(msg, exc_info=err)
            post_alert("fail", NAME, msg)
            break

        output = []
        for i in data:
            output.append({
                "asset_id": int(i["asset_id"]),
                "owner": i["owner"]
            })
        try:
            upload_assets(output)
        except Exception as err:
            msg = f"Page {page}: Assets upload failed, {str(err)}"
            logger.exception(msg, exc_info=err)
            post_alert("fail", NAME, msg)

        page += 1
    update_timestamp()
    end = datetime.now()
    logger.info(end - start)
except Exception as err:
    msg = f"Main loop failed, {str(err)}"
    logger.exception(msg, exc_info=err)
    post_alert("fail", NAME, msg)
