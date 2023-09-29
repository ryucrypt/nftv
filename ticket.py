#!/usr/bin/python3
import os
from time import sleep
from datetime import datetime
import copy
import json
import random

import requests
from eosapi import EosApi
from tendo import singleton

from common import post_alert, setup_logger


NAME = os.path.splitext(os.path.basename(__file__))[0]
ENV = {}
SKIP = []
AH = "https://{}/atomicassets/v1/assets?collection_name={}&template_id={}&page={}&limit=100&order=asc&sort=asset_id"
RPC = "https://{}"
HEADERS = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
ACTION = {
    "account": "atomicassets",
    "name": "mintasset",
    "authorization": [
        {
            "actor": "",
            "permission": "active"
        }
    ],
    "data": {
        "authorized_minter": "",
        "collection_name": "",
        "schema_name": "",
        "template_id": 0,
        "new_asset_owner": "",
        "immutable_data": [],
        "mutable_data": [],
        "tokens_to_back": []
    }
}
LOG = "ticket_log"
RETRY = 3
SLEEP = 1
LIMIT = 5

def get_assets(collection, template_id, page):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.get(
                AH.format(ENV["aa"], collection, template_id, page),
                headers={"apikey": ENV["aa_key"]},
                timeout=60
            )
            r.raise_for_status()
            return r.json()["data"]
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def upload_log(data):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.post(
                ENV["supabase_url"] + LOG,
                headers=HEADERS,
                json=data,
                timeout=60
            )
            r.raise_for_status()
            break
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

    start = datetime.now()
    with open("config.json") as file:
        ENV = json.load(file)
    with open("skip2.json") as file:
        SKIP = json.load(file)
    HEADERS["apikey"] = ENV["supabase_key"]
    HEADERS["Authorization"] += ENV["supabase_key"]
    api = EosApi(rpc_host=RPC.format(ENV["rpc"]))
    api.import_key(ENV["acc"], ENV["acc_key"], "active")
    api.session.headers["apikey"] = ENV["rpc_key"]

    page = 1
    col = ENV["toptix_col"]
    tid = ENV["toptix_tid"]
    tickets =[i["template_id"] for i in ENV["toptix_choices"]]
    weights = [i["weight"] for i in ENV["toptix_choices"]]

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

        actions = []
        assets = []
        for index, i in enumerate(data):
            if i["owner"] == None or int(i["asset_id"]) in SKIP:
                logger.info(f"{i['owner']} - {i['asset_id']}: Skipped")
                continue
            choice = random.choices(tickets, weights=weights)[0]
            action = copy.deepcopy(ACTION)
            action["authorization"][0]["actor"] = ENV["acc"]
            action["data"]["authorized_minter"] = ENV["acc"]
            action["data"]["new_asset_owner"] = i["owner"]
            action["data"]["collection_name"] = ENV["toptix_col"]
            action["data"]["schema_name"] = ENV["toptix_schema"]
            action["data"]["template_id"] = choice
            actions.append(action)
            assets.append(int(i["asset_id"]))

            if len(actions) >= LIMIT or index == len(data) - 1:
                try:
                    txn_id = transact(actions, api)
                except Exception as err:
                    try:
                        error = json.loads(str(err).replace("transaction error: ", ""))
                        msg = f"Minting failed for {','.join([item for item in assets])} - {error['error']['details'][0]['message']}"
                    except:
                        msg = f"Minting failed for {','.join([item for item in assets])} - {str(err)}"
                    logger.exception(msg, exc_info=err)
                    post_alert("fail", NAME, msg)
                    actions = []
                    continue
                logger.info(f"Minted {txn_id}")
                output = []
                for ind, j in enumerate(actions):
                    tname = [i["name"] for i in ENV["toptix_choices"] if i["template_id"] == j["data"]["template_id"]]
                    logger.info(f"{j['data']['new_asset_owner']} - {assets[ind]}: {j['data']['template_id']} - {tname[0]}")
                    output.append({
                        "txn_id": txn_id,
                        "to": j["data"]["new_asset_owner"],
                        "template_id": j["data"]["template_id"],
                        "asset_id": assets[ind],
                        "name": tname[0]
                    })
                try:
                    upload_log(output)
                except Exception as err:
                    msg = f"Failed to upload log for {','.join([item for item in assets])} - {str(err)}"
                    logger.exception(msg, exc_info=err)
                    post_alert("fail", NAME, msg)
                actions = []
                assets = []
        page += 1

    end = datetime.now()
    logger.info(end - start)
except Exception as err:
    msg = f"Main loop failed, {str(err)}"
    logger.exception(msg, exc_info=err)
    post_alert("fail", NAME, msg)
