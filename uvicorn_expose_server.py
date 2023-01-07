#!/usr/bin/env python3
"""Combination of uvicorn_server.py and expose_server.py"""

import os
import time
import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser(
        description="start, stop and status util for uvicorn and expose server."
    )
    parser.add_argument(
        "command", type=str, help="start, stop, check status or see logs",
        choices=("start", "stop", "status", "address")
    )
    parser.add_argument("--port", "-p", type=int, help="port number", default=8000)
    args = parser.parse_args()

    uv_pid_path = os.path.expanduser(f"~/.uv_{args.port}.pid")
    uv_log_path = os.path.expanduser(f"~/.uv_{args.port}.log")

    ex_pid_path = os.path.expanduser(f"~/.ex_{args.port}.pid")
    ex_log_path = os.path.expanduser(f"~/.ex_{args.port}.log")

    if args.command == "start":

        if os.path.exists(uv_pid_path):
            print(f"uvicorn pid file ({uv_pid_path}) aleady exists. Turn off uvicorn first.")
        if os.path.exists(ex_pid_path):
            uvicorn_expose_server.py(f"expose pid file ({ex_pid_path}) aleady exists. Turn off expose first.")

        # Start the uvicorn server
        command = f"nohup uvicorn retriever_server:app --port {args.port} > {uv_log_path} 2>&1 & \necho $! > {uv_pid_path}"
        subprocess.Popen(command, shell=True)

        time.sleep(1)
        if not os.path.exists(uv_pid_path):
            exit(f"The uvicorn server started but the pid file ({uv_pid_path}) couldn not be found.")
        if not os.path.exists(uv_log_path):
            exit(f"The uvicorn server started but the log file ({uv_log_path}) couldn not be found.")

        with open(uv_pid_path, "r") as file:
            pid = file.read().strip()
        print(f"The uvicorn server has started with pid: {pid}. See the logs by: './uvicorn_server.py -p {args.port} log'")

        # Wait for the uvicorn server to start properly (probably not necessary)
        print("Waiting for 5s.")
        time.sleep(5)

        # Start the expose expose_server
        command = f"nohup lt --port {args.port} > {ex_log_path} 2>&1 & \necho $! > {ex_pid_path}"
        subprocess.Popen(command, shell=True)

        time.sleep(1)
        if not os.path.exists(ex_pid_path):
            exit(f"The expose server started but the pid file ({ex_pid_path}) couldn not be found.")
        if not os.path.exists(ex_log_path):
            exit(f"The uvicorn server started but the log file ({ex_log_path}) couldn not be found.")

        with open(ex_pid_path, "r") as file:
            pid = file.read().strip()
        print(f"The expose server has started with pid: {pid}.")

        print("Here is the output from the server. Terminating output won't affect the process.")
        command = f"cat {ex_log_path}"
        print(command)
        subprocess.call(command, shell=True)

    elif args.command == "stop":

        for name, pid_path, log_path, in zip(
            ["uvicorn", "expose"], [uv_pid_path, ex_pid_path], [uv_log_path, ex_log_path],
        ):

            if not os.path.exists(pid_path):
                print(f"{name} pid file ({pid_path}) not found. Recheck if it's running or turn it off manually.")
                continue

            command = f"pkill -F {pid_path}"
            print(command)
            subprocess.call(command, shell=True)

            if os.path.exists(pid_path):
                os.remove(pid_path)

            if os.path.exists(log_path):
                os.remove(log_path)

            print(f"Successfully stopped {name} server.")

    elif args.command == "status":

        for name, pid_path, in zip(["uvicorn", "expose"], [uv_pid_path, ex_pid_path]):
            if os.path.exists(pid_path):
                print(f"{name} pid file ({pid_path}) does exist.")
            else:
                print(f"{name} pid file ({pid_path}) does NOT exist.")

    elif args.command == "address":

        if not os.path.exists(ex_log_path):
            print("expose log file does not exist, so the address can't be determined.")
        else:
            command = f"cat {log_path}"
            print(command)
            subprocess.call(command, shell=True)


if __name__ == '__main__':
    main()
