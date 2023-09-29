#!/usr/bin/python3
import os
from time import sleep
from datetime import datetime
import copy
import decimal
import json

import requests
from eosapi import EosApi
from tendo import singleton

from common import post_alert, setup_logger


NAME = os.path.splitext(os.path.basename(__file__))[0]
ENV = {}
SKIP = []
RPC = "https://{}"
HEADERS = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
HEADERS2 = {
    "apikey": "",
    "Authorization": "Bearer ",
    "Prefer": "resolution=merge-duplicates"
}
ACTION = {
    "account": "",
    "name": "transfer",
    "authorization": [
        {
            "actor": "",
            "permission": "active"
        }
    ],
    "data": {
        "from": "",
        "to": "",
        "quantity": "",
        "memo": "Drip transfer"
    }
}
LOG = "drip_log"
CONFIG = "drip_config"
WALLETS = "get_wallets"
DRIP = "get_drip"
DRIPS = "get_all_drip"
RETRY = 3
SLEEP = 1
LIMIT = 10
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

def upload_log(data):
    retry_count = 0
    while retry_count < RETRY:
        try:
            r = requests.post(
                ENV["supabase_url"] + LOG,
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

def get_wallets():
    retry_count = 0
    output = []
    params = {"lim": 1000, "off": 0}
    while True:
        try:
            r = requests.post(
                ENV["supabase_url"] + "rpc/" + WALLETS,
                headers=HEADERS,
                json=params,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            if len(r.json()) < 1:
                return output
            output += [item["wallet"] for item in r.json()]
            params["off"] += 1000
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def get_drip(wallet):
    retry_count = 0
    params = {"wallet": wallet}
    while True:
        try:
            r = requests.post(
                ENV["supabase_url"] + "rpc/" + DRIP,
                headers=HEADERS,
                json=params,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            return r.text
        except Exception as err:
            retry_count += 1
            if retry_count >= RETRY:
                raise err
            else:
                sleep(SLEEP)

def get_drips():
    retry_count = 0
    output = []
    params = {"lim": 1000, "off": 0}
    while True:
        try:
            r = requests.post(
                ENV["supabase_url"] + "rpc/" + DRIPS,
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

def get_dist():
    retry_count = 0
    output = []
    params = {"lim": 1000, "off": 0}
    while True:
        try:
            r = requests.post(
                ENV["supabase2_url"] + "rpc/" + WALLETS,
                headers=HEADERS2,
                json=params,
                timeout=TIMEOUT
            )
            r.raise_for_status()
            if len(r.json()) < 1:
                return output
            output += [item["address"] for item in r.json()]
            params["off"] += 1000
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

def formatToken(amount, precision):
    rounded_num = decimal.Decimal(str(amount)).quantize(
        decimal.Decimal("0." + "0" * precision))
    return str(rounded_num)

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
    with open("skip.json") as file:
        SKIP = json.load(file)
    HEADERS["apikey"] = ENV["supabase_key"]
    HEADERS["Authorization"] += ENV["supabase_key"]
    HEADERS2["apikey"] = ENV["supabase2_key"]
    HEADERS2["Authorization"] += ENV["supabase2_key"]
    api = EosApi(rpc_host=RPC.format(ENV["rpc"]))
    api.import_key(ENV["acc"], ENV["acc_key"], "active")
    api.session.headers["apikey"] = ENV["rpc_key"]
    config = get_config()
    mode = [item["value"] for item in config if item["config"] == "vip_only"][0]
    if mode == "true":
        wallets = get_dist()
        logger.info("Wallet count: " + str(len(wallets)))
        logger.info(wallets)
    else:
        wallets = get_wallets()

    try:
        drips = get_drips()
    except Exception as err:
        msg = f"Failed to get drips, {str(err)}"
        logger.exception(msg, exc_info=err)
        post_alert("fail", NAME, msg)
        exit(1)

    actions = []
    for index, i in enumerate(wallets):
        if i in SKIP:
            logger.info(f"{i}: Skipped")
            continue

        found = [item["drip"] for item in drips if item["wallet"] == i]
        if len(found) == 0:
            drip = 0
        else:
            drip = found[0]

        if drip != 0:
            amount = float(drip)
            action = copy.deepcopy(ACTION)
            action["account"] = ENV["token_contract"]
            action["authorization"][0]["actor"] = ENV["acc"]
            action["data"]["from"] = ENV["acc"]
            action["data"]["to"] = i
            action["data"]["quantity"] = formatToken(amount, ENV["token_precision"]) + " " + ENV["token_sym"]
            actions.append(action)
        else:
            logger.info(f"{i}: No drip")

        if len(actions) >= LIMIT or (index == len(wallets) - 1 and len(actions) > 0):
            try:
                txn_id = transact(actions, api)
            except Exception as err:
                try:
                    error = json.loads(str(err).replace("transaction error: ", ""))
                    msg = f"Transfer failed for {','.join([item['data']['to'] for item in actions])} - {error['error']['details'][0]['message']}"
                except:
                    msg = f"Transfer failed for {','.join([item['data']['to'] for item in actions])} - {str(err)}"
                logger.exception(msg, exc_info=err)
                post_alert("fail", NAME, msg)
                actions = []
                status = 1
                continue
            logger.info(f"Transferred {txn_id}")
            output = []
            for j in actions:
                logger.info(f"{j['data']['to']}: {j['data']['quantity']}")
                output.append({
                    "txn_id": txn_id,
                    "to": j["data"]["to"],
                    "amount": float(j["data"]["quantity"].split()[0]),
                    "token": ENV["token_sym"]
                })
            try:
                upload_log(output)
            except Exception as err:
                msg = f"Failed to upload log for {','.join([item['data']['to'] for item in actions])} - {str(err)}"
                logger.exception(msg, exc_info=err)
                post_alert("fail", NAME, msg)
                status = 1
            actions = []

    end = datetime.now()
    logger.info(end - start)
    exit(status)
except Exception as err:
    msg = f"Main loop failed, {str(err)}"
    logger.exception(msg, exc_info=err)
    post_alert("fail", NAME, msg)
    exit(status)
