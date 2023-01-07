import os
import argparse
import subprocess


def main():
    parser = argparse.ArgumentParser(description="Start, stop, check status or see logs of uvicorn server.")
    parser.add_argument(
        "command", type=str, help="start, stop, check status or see logs",
        choices=("start", "stop", "status", "log")
    )
    parser.add_argument("port", type=int, help="port number", default=8000)
    args = parser.parse_args()

    pid_path = os.path.expanduser(f"~/.uv_{args.port}.pid")
    log_path = os.path.expanduser(f"~/.uv_{args.port}.log")

    if args.command == "start":

        if os.path.exists(log_path):
            exit("uvicorn pid file aleady exists. Turn off uvicorn first.")

        command = f"nohup uvicorn retriever_server:app --port {args.port} > {log_path} &"
        print(command)
        subprocess.call(command, shell=True)

    elif args.command == "stop":

        if not os.path.exists(pid_path):
            exit("uvicorn pid file not found. Recheck if it's running or turn it off manually.")

        command = f"pkill -F {pid_path}"
        print(command)
        subprocess.call(command, shell=True)

        if os.path.exists(pid_path):
            os.remove(pid_path)

    elif args.command == "status":

        if os.path.exists(pid_path):
            print("uvicorn pid file does exist.")
        else:
            print("uvicorn pid file does NOT exist.")
        
        print("\nHere is output of lsof filtered for uvicorn processes.")
        command = "lsof -i -P -n | grep LISTEN | grep uvicorn"
        print(command)
        subprocess.call(command, shell=True)

    elif args.command == "log":

        if os.path.exists(log_path):
            print("uvicorn log file does not exist.")
        else:
            command = f"cat {log_path}"
            print(command)
            subprocess.call(command, shell=True)


if __name__ == '__main__':
    main()
