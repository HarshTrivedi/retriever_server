#!/usr/bin/env python3
import os
import time
import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser(description="Start, stop, check status or see logs of expose server.")
    parser.add_argument(
        "command", type=str, help="start, stop, check status or see logs",
        choices=("start", "stop", "status", "log")
    )
    parser.add_argument("--port", "-p", type=int, help="port number", default=8000)
    args = parser.parse_args()

    pid_path = os.path.expanduser(f"~/.ex_{args.port}.pid")
    log_path = os.path.expanduser(f"~/.ex_{args.port}.log")

    if args.command == "start":

        if os.path.exists(pid_path):
            exit(f"expose pid file ({pid_path}) aleady exists. Turn off expose first.")

        command = f"bore local {args.port} --to bore.pub > {log_path} 2>&1 & \necho $! > {pid_path}"
        subprocess.Popen(command, shell=True)

        time.sleep(1)
        if not os.path.exists(pid_path):
            exit(f"The expose server started but the pid file ({pid_path}) couldn not be found.")
        if not os.path.exists(log_path):
            exit(f"The uvicorn server started but the log file ({log_path}) couldn not be found.")

        with open(pid_path, "r") as file:
            pid = file.read().strip()
        print(f"The expose server has started with pid: {pid}.")

        print("Here is the output from the server. Terminating output won't affect the process.")
        command = f"cat {log_path}"
        print(command)
        subprocess.call(command, shell=True)

    elif args.command == "stop":

        if not os.path.exists(pid_path):
            exit(f"expose pid file ({pid_path}) not found. Recheck if it's running or turn it off manually.")

        command = f"pkill -F {pid_path}"
        print(command)
        subprocess.call(command, shell=True)

        if os.path.exists(pid_path):
            os.remove(pid_path)

        if os.path.exists(log_path):
            os.remove(log_path)

    elif args.command == "status":

        if os.path.exists(pid_path):
            print(f"expose pid file ({pid_path}) does exist.")
        else:
            print(f"expose pid file ({pid_path}) does NOT exist.")
        
        print("\nHere is output of lsof filtered for expose processes.")
        command = "lsof -i -P -n | grep LISTEN | grep bore"
        print(command)
        subprocess.call(command, shell=True)

    elif args.command == "log":

        if not os.path.exists(log_path):
            print("expose log file does not exist.")
        else:
            command = f"cat {log_path}"
            print(command)
            subprocess.call(command, shell=True)

if __name__ == '__main__':
    main()
