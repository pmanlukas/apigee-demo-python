# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Flask, request, jsonify
import pandas as pd
import asyncio
import aiohttp
import json
import headers
from enum import Enum

class Status(Enum):
    NONE=1
    FETCHING=2
    DONE=3

app = Flask(__name__)
status =Status.NONE
lock = asyncio.Lock()
result: pd.DataFrame

class Verint:

    url = "https://mj-sapi-fa-dev-we1.azurewebsites.net/api/verint_employees_anonymizer?code=H38B9ek639ISYDl6hY7F83Igt7EbEFJ_cIoMiB7tfVBLAzFuMnJmSg=="

    url_2 = "https://wfo.mt4.verintcloudservices.com/wfo/fis-api/v1/schedules?retrievalStartDate=2023-01-01T00%3A00%3A00Z&retrievalEndDate=2023-01-30T00%3A00%3A00Z&employeeLookupKey=userName&employeeIdentifierList=Testuser1"

    headers = headers.verintHeaders

    async def load(preload):
        global lock, status
        if preload:
            print("preloading data for Verint")
            with open("verint.json") as file:
                j = json.load(file)
        else:
            print(">>> loading data from Verint <<<")
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(headers=Verint.headers, timeout=timeout) as session:
                async with session.get(Verint.url) as resp:
                    j = await resp.json()
        rows = []
        for e in j['data']:
            att = e.get('attributes')
            p = att.get('person')
            a = p.get('address') or {'city':'','country':''}

            rows.append({
                'id': e['id'],
                'first_name': p['firstName'],
                'last_name': p['lastName'],
                'city': a['city'],
                'country': a['country'],
                'source':'verint'})
        df = pd.DataFrame(rows)
        return True, df

    async def load_url_2():
        print(">>> loading data from Verint (scheduled) <<<")
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(headers=Verint.headers, timeout=timeout) as session:
            async with session.get(Verint.url_2) as resp:
                j = await resp.json()
        return j

class TS:

    url = "https://mj-sapi-fa-dev-we1.azurewebsites.net/api/tsqws_employees_anonymizer?code=nNOwHfjfDgCaHaUcUE0c4YUMqLwd2BikuX9Y_SmoKz60AzFuqn-Hfw=="

    url_2 = "https://wfm.saas2.timesquare.fr/api/feed/personnes/947013/plannings_mod?datetime-min=2023-01-01&datetime-max=2023-01-30&start-index=1&max-results=2147483646"

    headers = headers.tsqHeaders

    async def load(preload):
        if preload:
            print("preloading data from TS")
            with open("tsq.json") as file:
                j = json.load(file)
        else:
            print(">>> loading data from TS <<<")
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(headers=TS.headers, timeout=timeout) as session:
                async with session.get(TS.url) as resp:
                    j = await resp.json()
        rows = []
        for e in j['entries']:
            a = e.get('adresse') or {'ville':'','pays':''}            
            rows.append({
                'id': e['matricule'],
                'first_name': e['prenom'],
                'last_name': e['nom'],
                'city': a['ville'],
                'country': a['pays'],
                'source':'tsq'})
        df = pd.DataFrame(rows)
        return True, df

    async def load_url_2():
        print(">>> loading data from TS <<<")
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(headers=TS.headers, timeout=timeout) as session:
            async with session.get(TS.url_2) as resp:
                j = await resp.json()
        return j

async def loadData(url: str, headers: dict = None):
    print(f"load {url}")
    data = pd.read_json(url)
    return True, data


async def mergeData(preload):
    global status,result,lock
    try:
        results = await asyncio.gather(
            Verint.load(preload),
            TS.load(preload)
        )
    except Exception as e:
        print("exception")
        async with lock:
            status = Status.NONE
        raise e
    print(">>> loading done <<<")
    resultA = results[0]
    resultB = results[1]
    if resultA[0] and resultB[0]:
        dataA = resultA[1]
        dataB = resultB[1]
        tmp = pd.concat([dataA,dataB])
        #tmp = dataA.merge(dataB, how="outer",left_on="id", right_on="key")
        async with lock:
            result = tmp
            status = Status.DONE


@app.route('/trigger', methods=['GET'])
async def trigger():
    global lock, result, status
    async with lock:
        if status == Status.FETCHING:
            return jsonify({"status": "fetching"})
        else:
            status = Status.FETCHING
    preload = request.args.get("local", "yes")
    await mergeData(preload=="yes")
    return jsonify({"status": "ok"})


@app.route('/', methods=['GET'])
async def test():
    return jsonify({"status": "running"})

@app.route('/employees', methods=['GET'])
async def employees():
    global result
    try:
        page = int(request.args.get("page", "0"))
    except ValueError:
        return jsonify({"error": "parameter page must be an integer"}), 400
    try:
        page_size = int(request.args.get("page_size", "5"))
    except ValueError:
        return jsonify({"error": "parameter page_size must be an integer"}), 400
    orient = request.args.get("orient", "records")
    if not orient in ["records", "columns", "values", "table", "split"]:
        return jsonify({"error": "parameter orient must be records, columns, table, split or values"}), 400
    fname = request.args.get("first_name", "")
    lname = request.args.get("last_name", "")
    start = page * page_size
    end = start + page_size
    current = result
    if fname != '':
        current = current[current['first_name'] == fname]
    if lname != '':
        current = current[current['last_name'] == lname]
    return current.iloc[start:end].to_json(orient=orient)


if __name__ == "__main__":
    asyncio.run(mergeData(True))
    app.run()
