import os
import argparse
import subprocess
import _jsonnet
import json

def main():
    parser = argparse.ArgumentParser(description="Start/stop elasticsearch server.")
    parser.add_argument("command", type=str, help="start or stop", choices=("start", "stop"))
    args = parser.parse_args()

    es_pid_path = os.path.expanduser("~/.es_pid")
    elasticsearch_path = json.loads(
        _jsonnet.evaluate_file(".global_config.jsonnet")
    )["ELASTICSEARCH_PATH"]

    if args.command == "start":

        if os.path.exists(es_pid_path):
            exit("ES PID file aleady exists. Turn off ES first.")

        command = f"{elasticsearch_path} --daemonize --silent --pidfile {es_pid_path}"
        subprocess.call(command, shell=True)

    elif args.command == "stop":

        if not os.path.exists(es_pid_path):
            exit("ES PID file not found. Recheck if it's running or turn it off manually.")

        command = f"pkill -F {es_pid_path}"
        subprocess.call(command, shell=True)

        if os.path.exists(es_pid_path):
            os.remove(es_pid_path)

if __name__ == '__main__':
    main()
