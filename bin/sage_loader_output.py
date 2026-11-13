#!/usr/bin/env python3
"""Show the SAGE loader STDERR/STDOUT for an image launched through SAGEWebService.

Given part of an image name, find the LSM processing task(s) in the JACS JBoss
server log, locate each task's SageLoaderResult directory, and print the
contents of its sge_error and sge_output files.

The link from image to output is made through the task ID:

    Init LSM_NAMES with value '<...image...>' ...   [CONTEXT: task <ID>]
    Set script location to: <DIR>/sge_config/...    [CONTEXT: task <ID>]

Usage:
    sage_loader_output.py FLPO_20261113103222301_153732
    sage_loader_output.py FLPO_20261113103222301_153732 --all
"""

import argparse
import re
import shlex
import subprocess
import sys

DEFAULT_HOST = "jacs-data"
LOG_DIR = "/opt/servers/jacs-data/jboss-4.2.3.GA/server/default/log"
# Dated rotations (server.log.2026-11-12, possibly gzipped) oldest first, then the
# current log, so later entries win. Missing files are skipped so grep doesn't
# exit 2 when no rotations exist yet.
LOG_FILES = (
    f'for f in {LOG_DIR}/server.log.2* {LOG_DIR}/server.log; do '
    f'[ -r "$f" ] && printf "%s\\n" "$f"; done'
)

# A CONTEXT tag is either "task N" or "task N (child of M)"
CONTEXT = r"\[CONTEXT: task (?P<task>\d+)(?: \(child of \d+\))?\]"
LSM_RE = re.compile(
    r"^(?P<ts>\S+ \S+) .*Init LSM_NAMES with value '(?P<lsms>[^']*)'.*" + CONTEXT
)
# Other grid services (alignment, MIPs, archive...) log the same messages under
# the same pipeline task, so only SageLoaderService lines identify the loader run.
SCRIPT_RE = re.compile(
    r"^(?P<ts>\S+ \S+) .*\[SageLoaderService\] Set script location to: "
    r"(?P<dir>\S+)/sge_config/\S+\s+" + CONTEXT
)
JOB_RE = re.compile(
    r"^(?P<ts>\S+ \S+) .*\[SageLoaderService\] Task was submitted to the cluster "
    r"as job (?P<job>\d+)\s+" + CONTEXT
)


def run(cmd, host):
    """Run a shell command on host (or locally if host is empty); return stdout."""
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "LogLevel=ERROR", host, cmd] if host \
        else ["bash", "-c", cmd]
    result = subprocess.run(argv, capture_output=True, text=True, errors="replace")
    # grep exits 1 for "no match"; anything higher is a real failure
    if result.returncode > 1:
        sys.exit(f"Command failed on {host or 'localhost'}: {cmd}\n{result.stderr.strip()}")
    return result.stdout


def grep_logs(pattern, host):
    """Fixed-string search across the current and rotated server logs."""
    cmd = f"({LOG_FILES}) | xargs -r zgrep -hF -- {shlex.quote(pattern)}"
    return run(cmd, host).splitlines()


def find_tasks(image, host):
    """Return [{task, ts, lsms}] for LSM pipeline tasks whose LSM names contain image."""
    tasks = {}
    for line in grep_logs("Init LSM_NAMES with value", host):
        m = LSM_RE.match(line)
        if m and image in m["lsms"] and m["task"] not in tasks:
            tasks[m["task"]] = {"task": m["task"], "ts": m["ts"], "lsms": m["lsms"]}
    return sorted(tasks.values(), key=lambda t: t["ts"])


def find_result_dirs(task_ids, host):
    """Map task ID -> (result dir, cluster job ID or None)."""
    wanted = set(task_ids)
    dirs, jobs = {}, {}
    for line in grep_logs("Set script location to:", host):
        m = SCRIPT_RE.match(line)
        if m and m["task"] in wanted:
            dirs[m["task"]] = m["dir"]
    for line in grep_logs("Task was submitted to the cluster as job", host):
        m = JOB_RE.match(line)
        if m and m["task"] in wanted:
            jobs[m["task"]] = m["job"]
    return {t: (dirs[t], jobs.get(t)) for t in dirs}


def show_files(result_dir, host):
    """Print every STDERR and STDOUT file in a SageLoaderResult directory."""
    d = shlex.quote(result_dir)
    cmd = (
        f"for f in {d}/sge_error/* {d}/sge_output/sageLoaderOutput.*; do "
        f'[ -e "$f" ] || continue; '
        f'printf "\\n==== %s (%s bytes) ====\\n" "$f" "$(wc -c < "$f" | tr -d " ")"; '
        f'cat "$f"; done'
    )
    out = run(cmd, host)
    print(out if out.strip() else f"\n(no sge_error or sge_output files in {result_dir})")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("image", help="all or part of the image name, e.g. FLPO_20261113103222301_153732")
    parser.add_argument("--all", action="store_true",
                        help="show every matching run, not just the most recent")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"host with the JBoss log (default: {DEFAULT_HOST})")
    parser.add_argument("--local", action="store_true",
                        help="run on this machine instead of over SSH")
    args = parser.parse_args()
    host = "" if args.local else args.host

    tasks = find_tasks(args.image, host)
    if not tasks:
        sys.exit(f"No LSM processing tasks found for '{args.image}' in {LOG_DIR}/server.log*")
    if not args.all:
        if len(tasks) > 1:
            print(f"{len(tasks)} runs found; showing the most recent (use --all for every run)")
        tasks = tasks[-1:]

    results = find_result_dirs([t["task"] for t in tasks], host)
    for t in tasks:
        print(f"\n######## {t['ts']}  task {t['task']}")
        print(f"LSMs: {t['lsms']}")
        if t["task"] not in results:
            print("No SageLoader result directory logged for this task")
            continue
        result_dir, job = results[t["task"]]
        if job:
            print(f"Cluster job: {job}")
        print(f"Result dir: {result_dir}")
        show_files(result_dir, host)


if __name__ == "__main__":
    main()
