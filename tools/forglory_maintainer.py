"""Keep ForGlory warm and dispatch its daily collection at 22:00 Moscow time.

Runs within GitHub Actions for < 6 hours. A workflow_dispatch continuation
starts the next stint; a daily scheduled event is a recovery fallback.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MOSCOW = timezone(timedelta(hours=3))
STATE_PATH = Path('.github/forglory-maintainer-state.json')
RENDER_URL = 'https://forglory.onrender.com/'
PING_INTERVAL_SECONDS = 300
GIT_KEEPALIVE_SECONDS = 45 * 24 * 3600


def log(message: str) -> None:
    print(f'{datetime.now(timezone.utc).isoformat(timespec="seconds")} {message}', flush=True)


def run_git(*args: str) -> str:
    return subprocess.check_output(['git', *args], text=True).strip()


def sync_branch(branch: str) -> None:
    run_git('checkout', branch)
    run_git('pull', '--ff-only', 'origin', branch)


def commit_file(path: Path, message: str, branch: str) -> None:
    run_git('config', 'user.name', 'github-actions[bot]')
    run_git('config', 'user.email', '41898282+github-actions[bot]@users.noreply.github.com')
    run_git('add', str(path))
    if run_git('diff', '--cached', '--name-only', '--', str(path)):
        run_git('commit', '-m', message)
    # A previous push may have failed. Retry the existing commit, too.
    if int(run_git('rev-list', '--count', f'origin/{branch}..HEAD')) > 0:
        run_git('pull', '--rebase', 'origin', branch)
        run_git('push', 'origin', f'HEAD:{branch}')


def load_state() -> dict:
    try:
        state = json.loads(STATE_PATH.read_text(encoding='utf-8'))
        return state if isinstance(state, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def save_state(state: dict, branch: str) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    commit_file(STATE_PATH, 'chore: record daily ForGlory collection dispatch [skip ci]', branch)


def is_collection_due(now_utc: datetime, last_day: str | None) -> bool:
    local = now_utc.astimezone(MOSCOW)
    return local.hour >= 22 and local.date().isoformat() != last_day

def hold_for_collection(now_utc: datetime, elapsed_seconds: float, base_seconds: int) -> bool:
    """Avoid changing runners during the Moscow 22:00 collection window."""
    local = now_utc.astimezone(MOSCOW)
    minutes = local.hour * 60 + local.minute
    # GitHub jobs have a 6h hard limit. Never extend beyond 5h50.
    return 21 * 60 <= minutes < 22 * 60 + 5 and elapsed_seconds < min(
        base_seconds + 3900, 21000
    )


def dispatch_collection(token: str, repository: str, branch: str) -> None:
    url = f'https://api.github.com/repos/{repository}/actions/workflows/get-data.yml/dispatches'
    payload = json.dumps({'ref': branch}).encode('utf-8')
    request = Request(url, data=payload, method='POST', headers={
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'Content-Type': 'application/json',
        'User-Agent': 'ForGlory-Maintainer/1.0',
    })
    with urlopen(request, timeout=20) as response:
        if response.status != 204:
            raise RuntimeError(f'Unexpected collection dispatch status: {response.status}')


def enable_collection_workflow(token: str, repository: str) -> None:
    """Recover a workflow disabled by GitHub due to inactivity."""
    url = f'https://api.github.com/repos/{repository}/actions/workflows/get-data.yml/enable'
    request = Request(url, data=b'', method='PUT', headers={
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'ForGlory-Maintainer/1.0',
    })
    with urlopen(request, timeout=20) as response:
        if response.status != 204:
            raise RuntimeError(f'Unable to enable collection workflow: HTTP {response.status}')


def ping_render() -> None:
    command = [
        'curl', '--fail', '--silent', '--show-error', '--location',
        '--max-time', '180', '--retry', '2', '--retry-delay', '10',
        '--user-agent', 'ForGlory-KeepAlive/1.0', '--output', '/dev/null',
        RENDER_URL,
    ]
    try:
        result = subprocess.run(command, timeout=600, check=False)
        log('Render ping OK' if result.returncode == 0 else f'Render ping failed: {result.returncode}')
    except Exception as error:
        log(f'Render ping error: {type(error).__name__}: {error}')


def keep_repository_active(branch: str) -> None:
    age_seconds = time.time() - int(run_git('log', '-1', '--format=%ct'))
    if age_seconds < GIT_KEEPALIVE_SECONDS:
        return
    file = Path('.github/forglory-activity.txt')
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(datetime.now(timezone.utc).isoformat(timespec='seconds') + '\n', encoding='utf-8')
    commit_file(file, 'chore: keep ForGlory schedules active [skip ci]', branch)
    log('Repository activity heartbeat committed.')


def main(max_seconds: int) -> int:
    token = os.environ.get('GH_TOKEN', '')
    repository = os.environ.get('GITHUB_REPOSITORY', '')
    branch = os.environ.get('GITHUB_REF_NAME', 'main')
    if not token or not repository:
        raise RuntimeError('GH_TOKEN or GITHUB_REPOSITORY is missing')

    sync_branch(branch)
    try:
        enable_collection_workflow(token, repository)
        log('Collection workflow enabled.')
    except Exception as error:
        log(f'Unable to enable collection workflow (dispatch will retry): {error}')
    state = load_state()
    last_day = state.get('last_collection_day')
    log(f'Starting: last collection dispatch day={last_day!r}; Moscow 22:00.')

    next_ping = 0.0
    next_heartbeat_check = 0.0
    ping_thread: threading.Thread | None = None
    start = time.monotonic()
    retry_dispatch_after = 0.0
    state_save_retry_after = 0.0
    pending_state_save = False

    while True:
        tick = time.monotonic()
        elapsed = tick - start
        if elapsed >= max_seconds and not hold_for_collection(
            datetime.now(timezone.utc), elapsed, max_seconds
        ):
            break
        if tick >= next_ping:
            if ping_thread is None or not ping_thread.is_alive():
                ping_thread = threading.Thread(target=ping_render, daemon=True)
                ping_thread.start()
            next_ping = tick + PING_INTERVAL_SECONDS

        if tick >= next_heartbeat_check:
            try:
                keep_repository_active(branch)
            except Exception as error:
                log(f'Heartbeat failed; will retry: {error}')
            next_heartbeat_check = tick + 3600

        now_utc = datetime.now(timezone.utc)
        if pending_state_save and tick >= state_save_retry_after:
            try:
                save_state(state, branch)
                pending_state_save = False
                log(f'Collection dispatch state stored: {last_day}')
            except Exception as error:
                log(f'State push failed; will retry in 60s: {error}')
                state_save_retry_after = tick + 60

        if tick >= retry_dispatch_after and is_collection_due(now_utc, last_day):
            collection_day = now_utc.astimezone(MOSCOW).date().isoformat()
            try:
                dispatch_collection(token, repository, branch)
                last_day = collection_day
                state['last_collection_day'] = collection_day
                state['last_collection_dispatch_utc'] = datetime.now(timezone.utc).isoformat(timespec='seconds')
                pending_state_save = True
                log(f'Get Player Data dispatched for Moscow day {collection_day}')
            except (HTTPError, URLError, TimeoutError, RuntimeError) as error:
                log(f'Collection dispatch failed; retry in 60s: {error}')
                retry_dispatch_after = tick + 60
        time.sleep(1)

    if pending_state_save:
        try:
            save_state(state, branch)
        except Exception as error:
            log(f'ERROR: Could not save successful dispatch: {error}')
            return 1
    log('Maintainer stint complete; continuation workflow will be dispatched.')
    return 0


if __name__ == '__main__':
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--max-seconds', type=int, default=17100)
    args = argument_parser.parse_args()
    try:
        sys.exit(main(args.max_seconds))
    except Exception as exc:
        log(f'FATAL: {type(exc).__name__}: {exc}')
        sys.exit(1)
