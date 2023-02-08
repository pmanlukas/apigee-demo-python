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

from dateutil import parser
from flask import Flask, request, jsonify
import pandas as pd
import asyncio
import aiohttp
import json
import headers
from enum import Enum
from pytz import timezone
import random
from datetime import datetime, timedelta
import re

def gen_datetime(min_year=1991, max_year=2022):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return (start + (end - start) * random.random()).strftime("%Y-%m-%d")

class Status(Enum):
    NONE = 1
    FETCHING = 2
    DONE = 3


app = Flask(__name__)
status = Status.NONE
lock = asyncio.Lock()
result: pd.DataFrame


class Verint:

    url = "https://mj-sapi-fa-dev-we1.azurewebsites.net/api/verint_employees_anonymizer?code=H38B9ek639ISYDl6hY7F83Igt7EbEFJ_cIoMiB7tfVBLAzFuMnJmSg=="

    url_2 = "https://wfo.mt4.verintcloudservices.com/wfo/fis-api/v1/schedules?retrievalStartDate=2023-01-01T00%3A00%3A00Z&retrievalEndDate=2023-01-30T00%3A00%3A00Z&employeeLookupKey=userName&employeeIdentifierList=Testuser1"

    headers1 = headers.verintHeaders
    headers2 = headers.verintHeaders

    async def load(preload):
        global lock, status
        if preload:
            print("preloading data for Verint")
            with open("verint2.json") as file:
                j = json.load(file)
        else:
            print(">>> loading data from Verint <<<")
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(headers=Verint.headers1, timeout=timeout) as session:
                async with session.get(Verint.url) as resp:
                    j = await resp.json()
        rows = []
        for e in j['data']:
            att = e.get('attributes')
            p = att.get('person')
            u = att.get('user')
            un = ""
            if u:
                un = u.get('username')
            a = p.get('address') or {'city': '', 'country': ''}

            '''rows.append({
                'uniquePersonId': e['id'],
                'firstName': p['firstName'],
                'lastName': p['lastName'],
                "dateOfBirth": "1965-09-30",
                "emailCompany": "laborum aliquip",
                "employeeId": "dolor",
                "hrId": "est dolor",
                "companyCode": "dolor do",
                'city': a['city'],
                'country': a['country'],
                'source': 'verint'})'''

            hrId = att.get('employeeNumber') or ""
            rows.append({
                "uniquePersonId": un,
                "firstName": p['firstName'],
                "lastName": p['lastName'],
                "dateOfBirth": "1965-09-30",
                "emailCompany": "laborum aliquip",
                "employeeId": "dolor",
                "hrId": hrId,
                "companyCode": "dolor do",
                "companyName": "labore",
                "contracts": [],
                "activeContract": {},
                "locations": [
                    {
                        "dateStart":gen_datetime(),
                        "dateEnd": gen_datetime(),
                        "location": {
                            "locationId": "Duis quis labore exercitation",
                            "siteCode": "mollit ex",
                            "siteName": "labore non est sit dolor",
                            "countryCodeIso3": "ex commodo tempor magna in",
                            "countryName": "nisi ut non"
                        }
                    },
                    {
                        "dateStart": gen_datetime(),
                        "dateEnd": gen_datetime(),
                        "location": {
                            "locationId": "ut ullamco",
                            "siteCode": "minim Lorem sunt sit quis",
                            "siteName": "ad dolore tempor consequat Duis",
                            "countryCodeIso3": "eu dolore nulla qui",
                            "countryName": a['country'],
                        }
                    }
                ],
                "activeLocation": {
                    "locationId": "velit",
                    "siteCode": "dolor minim est",
                    "siteName": "eiusmod occaecat e",
                    "countryCodeIso3": "aliquip velit",
                    "countryName": a['country']
                },
                "schedules": [],
                "origin":"verint"
            })
        df = pd.DataFrame(rows)
        return True, df

    async def load_url_2():
        print(">>> loading data from Verint (scheduled) <<<")
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(headers=Verint.headers2, timeout=timeout) as session:
            async with session.get(Verint.url_2) as resp:
                j = await resp.text()
        return j


class TS:

    url = "https://mj-sapi-fa-dev-we1.azurewebsites.net/api/tsqws_employees_anonymizer?code=nNOwHfjfDgCaHaUcUE0c4YUMqLwd2BikuX9Y_SmoKz60AzFuqn-Hfw=="

    url_2 = "https://wfm.saas2.timesquare.fr/api/feed/personnes/947013/plannings_mod?datetime-min=2023-01-01&datetime-max=2023-01-30&start-index=1&max-results=2147483646"

    headers1 = headers.tsqHeaders

    async def load(preload):
        if preload:
            print("preloading data from TS")
            with open("tsq2.json") as file:
                j = json.load(file)
        else:
            print(">>> loading data from TS <<<")
            timeout = aiohttp.ClientTimeout(total=600)
            async with aiohttp.ClientSession(headers=TS.headers1, timeout=timeout) as session:
                async with session.get(TS.url) as resp:
                    j = await resp.json()
        rows = []
        for e in j['entries']:
            a = e.get('adresse') or {'ville': '', 'pays': ''}
            '''rows.append({
                'id': e['matricule'],
                'first_name': e['prenom'],
                'last_name': e['nom'],
                'city': a['ville'],
                'country': a['pays'],
                'source': 'tsq'})'''
            uid = e.get('matriculePaye') or 'none'
            eid = e.get('uri')
            if eid:
                eid = re.findall(r'\d+', eid)[-1]
            else:
                eid =""
            rows.append({
                "uniquePersonId": uid,
                "firstName": e['prenom'],
                "lastName": e['nom'],
                "dateOfBirth": "1965-09-30",
                "emailCompany": "laborum aliquip",
                "employeeId": eid,
                "hrId":  e['matricule'],
                "companyCode": "dolor do",
                "companyName": "labore",
                "contracts": [],
                "activeContract": {},
                "locations": [
                    {
                        "dateStart":gen_datetime(),
                        "dateEnd": gen_datetime(),
                        "location": {
                            "locationId": "Duis quis labore exercitation",
                            "siteCode": "mollit ex",
                            "siteName": "labore non est sit dolor",
                            "countryCodeIso3": "ex commodo tempor magna in",
                            "countryName": "nisi ut non"
                        }
                    },
                    {
                       "dateStart":gen_datetime(),
                        "dateEnd": gen_datetime(),
                        "location": {
                            "locationId": "ut ullamco",
                            "siteCode": "minim Lorem sunt sit quis",
                            "siteName": "ad dolore tempor consequat Duis",
                            "countryCodeIso3": "eu dolore nulla qui",
                            "countryName": a['pays']
                        }
                    }
                ],
                "activeLocation": {
                    "locationId": "velit",
                    "siteCode": "dolor minim est",
                    "siteName": "eiusmod occaecat e",
                    "countryCodeIso3": "aliquip velit",
                    "countryName": a['pays'],
                },
                "schedules": [],
                "origin":"TSQ"
            })
        df = pd.DataFrame(rows)
        return True, df

    async def load_url_2():
        print(">>> loading data from TS (scheduled) <<<")
        timeout = aiohttp.ClientTimeout(total=600)
        async with aiohttp.ClientSession(headers=TS.headers1, timeout=timeout) as session:
            async with session.get(TS.url_2) as resp:
                j = await resp.text()
        return j


async def loadData(url: str, headers: dict = None):
    print(f"load {url}")
    data = pd.read_json(url)
    return True, data


async def mergeData(preload):
    global status, result, lock
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
        tmp = pd.concat([dataA, dataB])
        #tmp = dataA.merge(dataB, how="outer",left_on="id", right_on="key")
        async with lock:
            result = tmp
            status = Status.DONE


@app.route('/load_verint', methods=['GET'])
async def load_verint():
    timeout = aiohttp.ClientTimeout(total=600)
    async with aiohttp.ClientSession(headers=Verint.headers1, timeout=timeout) as session:
        async with session.get(Verint.url) as resp:
            j = await resp.json()
            return jsonify(j)

@app.route('/load_tsq', methods=['GET'])
async def load_tsq():
    timeout = aiohttp.ClientTimeout(total=600)
    async with aiohttp.ClientSession(headers=TS.headers1, timeout=timeout) as session:
        async with session.get(TS.url) as resp:
            j = await resp.json()
            return jsonify(j)

@app.route('/trigger', methods=['GET'])
async def trigger():
    global lock, result, status
    async with lock:
        if status == Status.FETCHING:
            return jsonify({"status": "fetching"})
        else:
            status = Status.FETCHING
    preload = request.args.get("local", "yes")
    await mergeData(preload == "yes")
    return jsonify({"status": "ok"})


@app.route('/', methods=['GET'])
async def test():
    return jsonify({"status": "running"})


@app.route('/employees', methods=['GET'])
async def employees():
    global result
    if result.empty:
        return jsonify({"error": "please call /trigger first"}), 400
    
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
    fname = request.args.get("firstName", "")
    lname = request.args.get("lastName", "")
    origin = request.args.get("origin", "")
    start = page * page_size
    end = start + page_size
    current = result
    if fname != '':
        current = current[current['firstName'] == fname]
    if lname != '':
        current = current[current['lastName'] == lname]
    if origin != '':
        current = current[current['origin'] == origin]
    return current.iloc[start:end].to_json(orient=orient)

@app.route('/schedules', methods=['GET'])
async def schedules():
    try:
        rsdStr = request.args.get("retrievalStartDate", "2023-01-01T00:00:00Z")
        rsd = parser.parse(rsdStr)
    except BaseException:
        return jsonify({"error": f"retrievalStartDate format error: {rsdStr}"}), 400
    try:
        redStr = request.args.get("retrievalEndDate", "2023-01-30T00:00:00Z")
        red = parser.parse(redStr)
    except BaseException:
        return jsonify({"error": f"retrievalStartDate format error: {rsdStr}"}), 400
    try:
        page_size = int(request.args.get("page_size", "5"))
    except ValueError:
        return jsonify({"error": "parameter page_size must be an integer"}), 400

    with open("verint_response_09012023.json") as file:
        j = json.load(file)

    verint_data = j['data']
    att = verint_data.get('attributes')
    att.pop('calendarEventAssignmentList')
    verint_sil = att.get('scheduleInformationList')
    for si in verint_sil:
        shifts = si.get('shifts') or []
        filtered = []
        for shift in shifts:
            est = shift.get('mainShiftEvent').get('eventStartTime')
            stmp = parser.parse(est)
            if stmp >= rsd and stmp <= red:
                filtered.append(shift)

        si['shifts'] = filtered
    with open("TSQ_response_09012023.json") as file:
        j = json.load(file)
    entries = j['entries']

    # schedule information list
    sil = []
    for e in entries:
        shifts = []
        composantsPlanning = e['composantsPlanning']
        for cp in composantsPlanning:
            tachesPlanning = cp.get('tachesPlanning') or []
            # main shift event container
            mses = []
            for tp in tachesPlanning:
                p = tp['periode']
                stmp = parser.parse(p['debut'])
                if stmp < rsd or stmp > red:
                    continue
                begin = str(stmp.astimezone(timezone('UTC'))
                            ).replace('+00:00', 'Z')
                stmp_e = parser.parse(p['fin'])
                duration = (stmp_e - stmp).total_seconds() / 60
                an = tp['tache']['href']
                # main shift event
                mse = {
                    'description': '',
                    'preShiftOvertime': '',
                    'postShiftOvertime': '',
                    'activityName': an,
                    'eventStartTime': begin,
                    'duration': str(int(duration))
                }
                mses.append(mse)
            pausesPlanning = cp.get('pausesPlanning') or []
            # shift activity list
            sal = []
            for pp in pausesPlanning:
                an = pp.get('type')
                p = tp['periode']
                stmp = parser.parse(p['debut'])
                if stmp < rsd or stmp > red:
                    continue
                begin = str(stmp.astimezone(timezone('UTC'))
                            ).replace('+00:00', 'Z')
                stmp_e = parser.parse(p['fin'])
                duration = (stmp_e - stmp).total_seconds() / 60
                activity = {
                    'activityName': an,
                    'eventStartTime': begin,
                    'duration': str(int(duration))
                }
                sal.append(activity)
            mse = mses[0] if len(mses) > 0 else ''
            shift = {
                'mainShiftEvent': mse,
                'shiftActivityList': sal
            }
            if len(mses) > 0 or len(sal) > 0:
                shifts.append(shift)

        # schedule information
        si = {
            'shifts': shifts
        }
        if len(shifts) > 0:
            sil.append(si)
    verint_sil.append(sil)

    return jsonify(verint_data)


if __name__ == "__main__":
    asyncio.run(mergeData(True))
    app.run()
