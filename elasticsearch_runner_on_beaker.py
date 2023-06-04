#!/usr/bin/env python3
import os
import argparse
import subprocess

from run_on_beaker_lib import get_beaker_config, make_image


def main():
    parser = argparse.ArgumentParser(
        description="Start/stop elasticsearch search on beaker onpremise cluster."
    )
    parser.add_argument("--force", action="store_true", help="delete the image if it already exists.")
    parser.add_argument(
        "command", type=str, help="command", choices=("start", "stop")
    )
    args = parser.parse_args()

    nohup_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), ".elasticsearch.nohup"
    )

    if args.command == "start":
        image_name = "elasticsearch"
        docker_file_name = "Dockerfile_elasticsearch"
        make_image(
            image_name=image_name, docker_file_name=docker_file_name, update_if_exists=args.force
        )
        print("Done with potential image creation.")

        user_name = get_beaker_config()["user_name"]
        beaker_workspace = get_beaker_config()["beaker_workspace"]

        command = f'''
nohup beaker session create \
--image beaker://{user_name}/{image_name} \
--workspace {beaker_workspace} \
--env JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
--port 9200:9200 \
> {nohup_file_path} &
        '''.strip()
        print(f"Running: {command}")
        subprocess.run(command, shell=True)

    else:

        if os.path.exists(nohup_file_path):
            with open(nohup_file_path, "r") as file:
                content = file.read()
            session_lines = [line.strip() for line in content.strip().split("\n") if "Starting session" in line]
            if session_lines:
                session_name = session_lines[0].split("...")[0].split(" ")[2]
                command = f"beaker session stop {session_name}"
            print(f"Running: {command}")
            subprocess.run(command, shell=True)
            os.remove(nohup_file_path)
        else:
            print("No nohup file found to obtain the running session.")


if __name__ == "__main__":
    main()
