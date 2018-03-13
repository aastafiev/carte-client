#!/usr/bin/env python
# -*- coding: utf-8 -*-


import settings

import os
import sys
import logging

from utils.utils import load_cfg

import asyncio
import aiohttp

import xml.etree.ElementTree as ETree
from typing import Tuple


async def fetch(session: aiohttp.ClientSession, url: str, body: str) -> Tuple[str, int, str]:
    async with session.post(url=url, data=body) as response:
        return await response.text(), int(response.status), response.reason


async def main(conf: dict, num_try: int = 5) -> None:
    def check_code(code: int, reason: str, task_desc: str):
        if code != 200:
            logging.debug(f'Fail to run task: {task_desc}. HTTP Reason: {reason}, HTTP Code {code}')
            raise SystemExit(f'Fail to run task: {task_desc}. HTTP Reason: {reason}, HTTP Code {code}')

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    auth = aiohttp.BasicAuth(conf['carte-master']['auth_login'], conf['carte-master']['auth_pass'])

    url_execute_job = '{}/kettle/executeJob/'.format(conf['carte-master']['url'])
    url_job_status = '{}/kettle/jobStatus/'.format(conf['carte-master']['url'])
    url_run_job = '{}/kettle/startJob/'.format(conf['carte-master']['url'])

    async with aiohttp.ClientSession(headers=headers, auth=auth) as session:
        logging.info(f"Running Job: HTTP POST -> {url_execute_job}, job -> {conf['job-entry']['job']}")
        req_execute_job = 'rep={rep}&job={job}&user={user}&pass={pass}&level={level}'.format(**conf['job-entry'])
        xml_run_job, http_code, http_reason = await fetch(session, url_execute_job, req_execute_job)
        check_code(http_code, http_reason, url_execute_job)
        xml_et = ETree.fromstring(xml_run_job)
        carte_result = xml_et.find('result').text
        carte_msg = xml_et.find('message').text
        carte_job_id = xml_et.find('id').text
        logging.debug(f'carte_result -> {carte_result}, carte_msg -> {carte_msg}, carte_job_id -> {carte_job_id}')

        if carte_result != 'OK':
            logging.debug(f"The job ({conf['job-entry']['job']}) couldn't run: {carte_msg}")
            raise SystemExit(f"The job ({conf['job-entry']['job']}) couldn't run: {carte_msg}")

        req_job_status = 'xml=Y&id={}'.format(carte_job_id)
        req_run_job = 'xml=Y&id={}'.format(carte_job_id)
        logging.debug('Waiting 5 sec')
        await asyncio.sleep(5)

        run_status = False
        for idx in range(num_try):
            logging.debug(f'Checking Job status (attempt {idx+1}): HTTP POST -> {url_job_status}, '
                          f'job -> {conf["job-entry"]["job"]}')
            xml_job_status, http_code, http_reason = await fetch(session, url_job_status, req_job_status)
            check_code(http_code, http_reason, url_job_status)
            xml_et = ETree.fromstring(xml_job_status)
            job_status_desc = xml_et.find('status_desc').text
            logging.debug(f'job_status_desc -> {job_status_desc}')

            if job_status_desc == 'Running':
                run_status = True
                break
            else:
                logging.debug('Waiting 1 sec')
                await asyncio.sleep(1)
                logging.debug(f'Trying to re-run Job (attempt {idx+1}): HTTP POST -> {url_run_job} '
                              f'job -> {conf["job-entry"]["job"]}')
                xml_run_job, http_code, http_reason = await fetch(session, url_run_job, req_run_job)
                check_code(http_code, http_reason, url_run_job)

        if run_status:
            logging.info(f"Job {conf['job-entry']['job']} executed successful")
        else:
            logging.info(f'Failed to run Job (attempt {idx+1})')
            raise SystemExit(f'Failed to run Job (attempt {idx+1})')


if __name__ == '__main__':
    logging.getLogger('asyncio').setLevel(logging.INFO)
    try:
        if len(sys.argv) > 1:
            CONFIG = load_cfg(os.path.abspath(sys.argv[1]))
        else:
            CONFIG = load_cfg(os.path.join(settings.PROJECT_DIR, 'etc', 'config.yml'))

        logging.basicConfig(level=logging.getLevelName(CONFIG['other']['log_level'].upper()),
                            format=settings.DEFAULT_LOG_FORMAT)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(CONFIG))
    except FileNotFoundError as err:
        sys.exit(str(err))
