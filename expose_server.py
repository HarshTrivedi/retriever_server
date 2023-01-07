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
    parser.add_argument("port", type=int, help="port number", default=8000)
    args = parser.parse_args()

    pid_path = os.path.expanduser(f"~/.ex_{args.port}.pid")
    log_path = os.path.expanduser(f"~/.ex_{args.port}.log")

    if args.command == "start":

        if os.path.exists(log_path):
            exit("expose pid file aleady exists. Turn off expose first.")

        command += f"nohup lt --port {args.port} > {log_path} &"
        subprocess.call(command, shell=True)

        time.sleep(3)

        if not os.path.exists(log_path):
            exit("The expose server did not start correctly. Couldn't find the log file.")

        command += f"cat {log_path}"
        subprocess.call(command, shell=True)

    elif args.command == "stop":

        if not os.path.exists(pid_path):
            exit("expose pid file not found. Recheck if it's running or turn it off manually.")

        command = f"pkill -F {pid_path}"
        subprocess.call(command, shell=True)

        if os.path.exists(pid_path):
            os.remove(pid_path)

    elif args.command == "status":

        if os.path.exists(pid_path):
            print("expose pid file does exist.")
        else:
            print("expose pid file does NOT exist.")
        
        print("\nHere is output of lsof filtered for expose processes.")
        command = "lsof -i -P -n | grep LISTEN | grep lt"
        subprocess.call(command, shell=True)

    elif args.command == "log":

        if os.path.exists(log_path):
            print("expose log file does not exist.")
        else:
            command = f"cat {log_path}"
            subprocess.call(command, shell=True)

if __name__ == '__main__':
    main()
