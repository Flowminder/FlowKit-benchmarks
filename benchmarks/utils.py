# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import json
import docker
import shutil
from pathlib import Path


def get_benchmark_dbs_dir():
    conf_file_path = Path(os.getenv("ASV_CONF_DIR"))
    with open(conf_file_path / "asv.conf.json") as fin:
        config = json.load(fin)
    return conf_file_path / config["benchmark_dbs_dir"]


def keep_containers_alive():
    conf_file_path = Path(os.getenv("ASV_CONF_DIR"))
    with open(conf_file_path / "asv.conf.json") as fin:
        config = json.load(fin)
    return config["keep_containers_alive"]


def keep_flowdb_volumes():
    conf_file_path = Path(os.getenv("ASV_CONF_DIR"))
    with open(conf_file_path / "asv.conf.json") as fin:
        config = json.load(fin)
    return config["keep_flowdb_volumes"]


def reuse_containers():
    conf_file_path = Path(os.getenv("ASV_CONF_DIR"))
    with open(conf_file_path / "asv.conf.json") as fin:
        config = json.load(fin)
    return config["reuse_containers"]


def get_redis(docker_client):
    try:
        redis_container = docker_client.containers.get("flowkit_benchmarks_redis")
    except docker.errors.NotFound:
        docker_client.images.pull("bitnami/redis", "latest")
        redis_container = docker_client.containers.run(
            "bitnami/redis",
            detach=True,
            auto_remove=True,
            ports={"6379/tcp": None},
            name="flowkit_benchmarks_redis",
            environment={"REDIS_PASSWORD": "fm_redis"},
        )
    port_config = docker_client.api.inspect_container(redis_container.id)[
        "NetworkSettings"
    ]["Ports"]["6379/tcp"][0]
    redis_container.port = port_config["HostPort"]
    redis_container.host = port_config["HostIp"]
    return redis_container


def get_env_var(name):
    """
    Helper function to return the value of an environment variable,
    printing a meaningful error if it is not defined.

    Parameters
    ----------
    name : str
        Name of the environment variable.

    Returns
    -------
    str
        The value of the environment variable.
    """
    try:
        return os.environ[name]
    except KeyError:
        raise RuntimeError(f"Environment variable {name} is undefined. ")


def update_containers_to_remove_file(flowdb_container, redis_container):
    benchmark_dbs_dir = get_benchmark_dbs_dir()
    with open(
        benchmark_dbs_dir.parent / "containers_to_clean_up", "a+"
    ) as cleanup_file:
        to_remove = set(l.rstrip() for l in cleanup_file.readlines())
        to_remove.add(redis_container.name)
        to_remove.add(flowdb_container.name)
        cleanup_file.truncate(0)
        cleanup_file.write("\n".join(to_remove))


def update_volumes_to_remove_file(flowdb_config):
    benchmark_dbs_dir = get_benchmark_dbs_dir()
    with open(benchmark_dbs_dir.parent / "dirs_to_clean_up", "a+") as cleanup_file:
        to_remove = set(l.rstrip() for l in cleanup_file.readlines())
        to_remove.add(str(benchmark_dbs_dir / flowdb_config.volume_name))
        to_remove.add(str(benchmark_dbs_dir / flowdb_config.base.volume_name))
        cleanup_file.truncate(0)
        cleanup_file.write("\n".join(to_remove))
