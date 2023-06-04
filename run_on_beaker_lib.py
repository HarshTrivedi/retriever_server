import os
import json
import subprocess
from typing import Dict


def get_beaker_config():
    # NOTE: Don't add beakerizer dependency as it needs to be run w/o conda.
    beaker_config_file_path = ".project-beaker-config.json"
    if not os.path.exists(beaker_config_file_path):
        raise Exception("The beaker_config_file_path not available.")
    with open(beaker_config_file_path, "r") as file:
        beaker_config = json.load(file)
    return beaker_config


def make_image(
    image_name: str,
    docker_file_name: str,
    build_args: Dict = None,
    update_if_exists: bool = False,
):

    user_name = get_beaker_config()["user_name"]
    beaker_workspace = get_beaker_config()["beaker_workspace"]

    build_args = build_args or {}
    build_args_str = ""
    if build_args:
        build_args_str = "--build-arg "
        build_args_str += " ".join([f"{key}={value}" for key, value in build_args.items()])

    directory = os.path.dirname(os.path.realpath(__file__))
    dockerfile_path = os.path.join("dockerfiles", docker_file_name)
    command = f"docker build -t {image_name} {directory} -f {dockerfile_path} {build_args_str}".strip()
    print(f"Running: {command}")
    subprocess.run(command, shell=True, stdout=open(os.devnull, 'wb'))

    command = f"beaker image inspect --format json {user_name}/{image_name}"
    try:
        image_is_present = subprocess.call(command, shell=True, stdout=open(os.devnull, 'wb')) == 0
    except:
        image_is_present = False

    if image_is_present and not update_if_exists:
        print("Image already exists.")
        return

    if image_is_present:
        command = f"beaker image delete {user_name}/{image_name}"
        print(f"Running: {command}")
        subprocess.run(command, stdout=open(os.devnull, 'wb'), shell=True)

    command = f"beaker image create {image_name} --name {image_name} --workspace {beaker_workspace}"
    print(f"Running: {command}")
    subprocess.run(command, shell=True)
