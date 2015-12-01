import os
import dbm
import time

import click
import requests

from stackcollector.log import get_logger, configure_logging

configure_logging()
log = get_logger()


def collect(dbpath, host, port):
    try:
        resp = requests.get('http://{}:{}?reset=true'.format(host, port))
        resp.raise_for_status()
    except (requests.ConnectionError, requests.HTTPError) as exc:
        log.warning('Error collecting data', error=exc, host=host, port=port)
        return

    data = resp.content.splitlines()
    try:
        save(data, host, port, dbpath)
    except Exception as exc:
        log.warning('Error saving data', error=exc, host=host, port=port)
        return

    log.info('Data collected', host=host, port=port,
             num_stacks=len(data) - 2)


def save(data, host, port, dbpath):
    now = int(time.time())
    with dbm.open(dbpath, 'c') as db:
        for line in data[2:]:
            try:
                stack, value = line.split()
            except ValueError:
                continue

            entry = '{}:{}:{}:{} '.format(host, port, now, int(value)).encode()
            if stack in db:
                db[stack] += entry
            else:
                db[stack] = entry


@click.command()
@click.option('--dbpath', '-d', default='/var/tmp/stackcollector/db')
@click.option('--host', '-h', multiple=True)
@click.option('--ports', '-p')
@click.option('--interval', '-i', type=int, default=60)
@click.option('--ports_dir', type=str, default=None,
              help="If collector and stacksampler are running on same machine")
def run(dbpath, host, ports, interval, ports_dir):
    if not ports and not ports_dir:
        raise click.BadOptionUsage("--ports or --ports_dir is required!")

    if ports:
        if '..' in ports:
            start, end = ports.split('..')
            start = int(start)
            end = int(end)
            ports = range(start, end + 1)
        elif ',' in ports:
            ports = [int(p) for p in ports.split(',')]
        else:
            ports = [int(ports)]

    while True:
        for h in host:
            if ports_dir and os.path.isdir(ports_dir):
                for port in os.listdir(ports_dir):
                    collect(dbpath, h, port)
            else:
                for port in ports:
                    collect(dbpath, h, port)

        time.sleep(interval)


if __name__ == '__main__':
    run()
