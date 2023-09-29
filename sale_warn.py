#!/usr/bin/python3
import os
from time import sleep
from datetime import datetime
import copy
import json

import requests
from eosapi import EosApi
from tendo import singleton

from common import post_alert, setup_logger


NAME = os.path.splitext(os.path.basename(__file__))[0]
ENV = {}
RPC = "https://{}"
AH = "https://{}/atomicmarket/v1/assets?collection_name={}&template_id={}&page={}&limit=1000&order=asc&sort=asset_id"
HEADERS = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
ACTION = {
    "account": "atomicassets",
    "name": "setassetdata",
    "authorization": [
        {
            "actor": "",
            "permission": "active"
        }
    ],
    "data": {
        "authorized_editor": "",
        "asset_owner": "",
        "asset_id": 0,
        "new_mutable_data": []
    }
}
CONFIG = "sale_warning_data"
RETRY = 3
SLEEP = 1
TIMEOUT = 60

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

def transact(actions, api):
    retry_count = 0
    trx = {"actions": actions}
    while True:
        try:
            r = api.push_transaction(trx)
            return r["transaction_id"]
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
    api = EosApi(rpc_host=RPC.format(ENV["rpc"]))
    api.import_key(ENV["acc"], ENV["acc_key"], "active")
    api.session.headers["apikey"] = ENV["rpc_key"]
    config = get_config()

    for i in config:
        page = 1
        col = i["collection"]
        tid = i["template_id"]
        normal = i["normal"]
        warning = i["warning"]

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
                asset_id = int(j["asset_id"])
                name = j["data"]["name"]
                owner = j["owner"]

                logger.info(f"{col} - {tid} - {name}: {asset_id}, {owner}")

                change = False
                img = ""
                if len(j["sales"]) > 0:
                    if j["mutable_data"]["img"] != warning:
                        change = True
                        img = warning
                else:
                    if j["mutable_data"]["img"] != normal:
                        change = True
                        img = normal

                if change:
                    action = copy.deepcopy(ACTION)
                    action["authorization"][0]["actor"] = ENV["acc"]
                    action["data"]["authorized_editor"] = ENV["acc"]
                    action["data"]["asset_id"] = asset_id
                    action["data"]["asset_owner"] = owner
                    for k in j["mutable_data"].keys():
                        if k == "img":
                            action["data"]["new_mutable_data"].append({
                                "key": k,
                                "value": [
                                    "string",
                                    img
                                ]
                            })
                        else:
                            action["data"]["new_mutable_data"].append({
                                "key": k,
                                "value": [
                                    "string",
                                    j["mutable_data"][k]
                                ]
                            })

                    try:
                        txn_id = transact([action], api)
                        logger.info(f"Updated {txn_id}")
                    except Exception as err:
                        try:
                            error = json.loads(str(err).replace("transaction error: ", ""))
                            msg = f"Set image failed for {asset_id}, {name} - {error['error']['details'][0]['message']}"
                        except:
                            msg = f"Set image failed for {asset_id}, {name} - {str(err)}"
                        logger.exception(msg, exc_info=err)
                        post_alert("fail", NAME, msg)
                        actions = []

            page += 1
    end = datetime.now()
    logger.info(end - start)
    exit(status)
except Exception as err:
    msg = f"Main loop failed, {str(err)}"
    logger.exception(msg, exc_info=err)
    post_alert("fail", NAME, msg)
    exit(1)
