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
AH = "https://{}/atomicassets/v1/assets?collection_name={}&template_id={}&page={}&limit=100&order=asc&sort=asset_id"
TRANSFERS = "https://{}/atomicassets/v1/transfers?asset_id={}&page=1&limit=100&order=desc"
HEADERS = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
RATES = "drip_template_data"
ASSETS = "drip_asset_data2"
CONFIG = "drip_config"
TEMPLATES = "get_templates"
BLOCK = "blocklist_main"
RETRY = 3
SLEEP = 1
THROTTLE = 0
TIMEOUT = 60

def get_block():
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                ENV["supabase_url"] + BLOCK + "?select=*",
                headers=HEADERS,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.json()
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def get_rates():
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                ENV["supabase_url"] + RATES + "?select=*&order=collection.asc,template_id.asc",
                headers=HEADERS,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.json()
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def get_config():
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                ENV["supabase_url"] + CONFIG + "?select=*",
                headers=HEADERS,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.json()
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def get_assets(collection, template_id, page):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                AH.format(ENV["aa"], collection, template_id, page),
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

def get_sender(asset_id):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                TRANSFERS.format(ENV["aa"], asset_id),
                headers={"apikey": ENV["aa_key"]},
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.json()["data"][0]["sender_name"]
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

def get_templates():
    retry_count = 0
    output = []
    params = {"lim": 1000, "off": 0}
    while True:
        try:
            r = requests.post(
                ENV["supabase_url"] + "/rpc/" + TEMPLATES,
                headers=HEADERS,
                json=params,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            if len(r.json()) < 1:
                return output
            output += r.json()
            params["off"] += 1000
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def del_assets(template_id):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.delete(
                ENV["supabase_url"] + ASSETS + f"?template_id=eq.{template_id}",
                headers=HEADERS,
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

def del_burnt(asset_id):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.delete(
                ENV["supabase_url"] + ASSETS + f"?asset_id=eq.{asset_id}",
                headers=HEADERS,
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
        exit(1)

    status = 0
    start = datetime.now()
    with open("config.json") as file:
        ENV = json.load(file)
    HEADERS["apikey"] = ENV["supabase_key"]
    HEADERS["Authorization"] += ENV["supabase_key"]
    block = get_block()
    rates = get_rates()
    config = get_config()
    templates = get_templates()
    THROTTLE = float([item["value"] for item in config if item["config"] == "throttle"][0])

    for i in templates:
        found = [item for item in rates if item["template_id"] == i["template_id"]]
        if len(found) < 1:
            try:
                del_assets(i["template_id"])
                logger.info(f"{i['template_id']}: Old template deleted")
            except Exception as err:
                msg = f"{i['template_id']}: Deleting old template failed, {str(err)}"
                logger.exception(msg, exc_info=err)
                post_alert("fail", NAME, msg)
                status = 1
                continue

    for i in rates:
        page = 1
        col = i["collection"]
        tid = i["template_id"]
        base = i["drip_amount"]

        while True:
            try:
                data = get_assets(col, tid, page)
                if len(data) < 1:
                    break
            except Exception as err:
                msg = f"{col} - {tid} - {page}: Fetching assets failed, {str(err)}"
                logger.exception(msg, exc_info=err)
                post_alert("fail", NAME, msg)
                status = 1
                break

            output = []
            for j in data:
                mint = int(j["template_mint"])
                asset_id = int(j["asset_id"])
                name = j["data"]["name"]
                owner = j["owner"]
                max_supply = int(j["template"]["max_supply"])
                issued = int(j["template"]["issued_supply"])
                logger.info(f"{col} - {tid}: {asset_id}, {owner}")

                """
                if owner == None:
                    try:
                        del_burnt(asset_id)
                        logger.info(f"{col} - {tid}: Burnt {asset_id} deleted")
                        continue
                    except Exception as err:
                        msg = f"{col} - {tid}: Failed to delete burnt {asset_id}, {str(err)}"
                        logger.exception(msg, exc_info=err)
                        post_alert("fail", NAME, msg)
                        continue
                """

                # Custom handling for custodial staking
                if owner in ENV["custodial"]:
                    try:
                        owner = get_sender(asset_id)
                        logger.info(f"Custodial staked, original owner: {owner}")
                    except Exception as err:
                        msg = f"{col} - {tid}: Fetching sender for {asset_id} failed, {str(err)}"
                        logger.exception(msg, exc_info=err)
                        post_alert("fail", NAME, msg)
                        status = 1
                        continue

                bl_check = [item for item in block if item["collection"] == owner]
                if len(bl_check) > 0:
                    logger.info(f"Blocked account: {owner}")
                    drip = 0
                elif owner == None:
                    drip = 0
                else:
                    drip = base

                if i["ownership"] and "data" in j["mutable_data"] and j["mutable_data"]["data"] != "":
                    recipient = json.loads(j["mutable_data"]["data"])
                    if len(recipient) > 0 and recipient[0]["recipient"] != owner:
                        drip = 0

                bonus = 1
                if j["template_mint"] in i["mint_bonuses"]:
                    bonus = i["mint_bonuses"][j["template_mint"]]
                throttle = 1
                if i["throttle"]:
                    throttle = THROTTLE

                gross = drip * bonus
                net = gross * throttle

                output.append({
                    "asset_id": asset_id,
                    "collection": col,
                    "template_id": tid,
                    "name": name,
                    "owner": owner,
                    "max_supply": max_supply,
                    "issued_supply": issued,
                    "mint_number": mint,
                    "drip_amount": drip,
                    "mint_number_bonus": bonus,
                    "gross_drip_amount": gross,
                    "throttle_tax_reducer": throttle,
                    "net_drip_amount": net
                })

            try:
                upload_assets(output)
            except Exception as err:
                msg = f"{col} - {tid} - {page}: Assets upload failed, {str(err)}"
                logger.exception(msg, exc_info=err)
                post_alert("fail", NAME, msg)
                status = 1

            page += 1
    end = datetime.now()
    logger.info(end - start)
    exit(status)
except Exception as err:
    msg = f"Main loop failed, {str(err)}"
    logger.exception(msg, exc_info=err)
    post_alert("fail", NAME, msg)
    exit(1)
