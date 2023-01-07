#!/usr/bin/env python3
import os
import time
import json
import _jsonnet
import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser(description="Start, stop, check status or see logs of uvicorn server.")
    parser.add_argument(
        "command", type=str, help="start, stop, check status or see logs",
        choices=("start", "stop", "status", "log")
    )
    parser.add_argument("--port", "-p", type=int, help="port number", default=8000)
    args = parser.parse_args()

    pid_path = os.path.expanduser(f"~/.uv_{args.port}.pid")
    log_path = os.path.expanduser(f"~/.uv_{args.port}.log")
    config_path = os.path.expanduser(f"~/.uv_{args.port}.json")

    if args.command == "start":

        if not os.path.exists(".retriever_config.jsonnet"):
            exit("The .retriever_config.jsonnet is not available.")

        if os.path.exists(pid_path):
            exit(f"uvicorn pid file ({pid_path}) aleady exists. Turn off uvicorn first.")

        command = f"nohup uvicorn retriever_server:app --port {args.port} > {log_path} 2>&1 & \necho $! > {pid_path}"
        subprocess.Popen(command, shell=True)

        time.sleep(1)
        if not os.path.exists(pid_path):
            exit(f"The uvicorn server started but the pid file ({pid_path}) couldn not be found.")
        if not os.path.exists(log_path):
            exit(f"The uvicorn server started but the log file ({log_path}) couldn not be found.")

        with open(pid_path, "r") as file:
            pid = file.read().strip()
        print(f"The uvicorn server has started with pid: {pid}. See the logs by: './uvicorn_server.py -p {args.port} log'")

        print("Used retriever args:")
        config = json.dumps(json.loads(_jsonnet.evaluate_file(".retriever_config.jsonnet")), indent=4)
        print(config)
        with open(config_path, "w") as file:
            file.write(config)

    elif args.command == "stop":

        if not os.path.exists(pid_path):
            exit(f"uvicorn pid file ({pid_path}) not found. Recheck if it's running or turn it off manually.")

        command = f"pkill -F {pid_path}"
        print(command)
        subprocess.call(command, shell=True)

        if os.path.exists(pid_path):
            os.remove(pid_path)

        if os.path.exists(log_path):
            os.remove(log_path)

    elif args.command == "status":

        if os.path.exists(pid_path):
            print(f"uvicorn pid file ({pid_path}) does exist.")
        else:
            print(f"uvicorn pid file ({pid_path}) does NOT exist.")

        print("\nHere is output of lsof filtered for uvicorn processes.")
        command = "lsof -i -P -n | grep LISTEN | grep uvicorn"
        print(command)
        subprocess.call(command, shell=True)

        if os.path.exists(config_path):
            subprocess.call(f"cat {config_path}", shell=True)
        else:
            print(f"The config path ({config_path}) not found.")

    elif args.command == "log":

        if not os.path.exists(log_path):
            print("uvicorn log file does not exist.")
        else:
            command = f"tail -f {log_path}"
            print(command)
            subprocess.call(command, shell=True)


if __name__ == '__main__':
    main()
