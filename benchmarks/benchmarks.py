# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import timeit
import flowmachine
import docker
import os
import json
from pathlib import Path
from flowmachine.features import (
    daily_location,
    ModalLocation,
    Flows,
    TotalLocationEvents,
)
from .utils import get_env_var
from .config import FLOWDB_CONFIGS, FLOWDB_CONFIG_PARAM_NAMES
from .flowdb_config import FlowDBConfig


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


def setup(*args):
    print(f"Running setup for {args}")
    docker_client = docker.from_env()

    flowdb_config = FlowDBConfig(*args[:6], root_directory=get_benchmark_dbs_dir())
    flowdb_container = flowdb_config.create_db(docker_client)
    redis_container = get_redis(docker_client)
    flowmachine.connect(
        db_port=flowdb_container.port,
        db_host=flowdb_container.host,
        redis_port=redis_container.port,
        redis_host=redis_container.host,
        redis_password="fm_redis",
    )
    flowmachine.redis_container = redis_container
    flowmachine.flowdb_container = flowdb_container
    flowmachine.flowdb_config = flowdb_config
    print(
        f"Connected. Flushing redis '{redis_container.name}' on {redis_container.host}:{redis_container.port}."
    )
    flowmachine.core.Query.redis.flushdb()
    print("Wiping any cache tables.")
    for q in flowmachine.core.Query.get_stored():
        q.invalidate_db_cache()


def teardown(*args):
    print(f"Running teardown for {args}")
    try:
        flowmachine.core.Query.redis.flushdb()
        print(
            f"Killing any queries still running on {flowmachine.flowdb_container.name}"
        )
        with flowmachine.core.Query.connection.engine.begin():  # Kill any running queries
            flowmachine.core.Query.connection.engine.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='flowmachine';"
            )
        for q in flowmachine.core.Query.get_stored():
            q.invalidate_db_cache()
        flowmachine.core.Query.connection.engine.dispose()
        del flowmachine.core.Query.connection
    finally:
        if keep_containers_alive():
            pass
        else:
            print(f"Stopping {flowmachine.redis_container.name}")
            flowmachine.redis_container.stop()
            print(f"Stopping {flowmachine.flowdb_container.name}")
            flowmachine.flowdb_container.stop()


class DailyLocation:

    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [["last", "most-common"]]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["daily_location_method"]
    timer = timeit.default_timer
    timeout = 1200

    def setup(self, *args):
        self.dl = daily_location(date="2016-01-01", method=args[-1])
        self.dl.turn_off_caching()

    def time_daily_location(self, *args):

        _ = self.dl.store().result()

    def track_daily_location_cost(self, *args):
        return self.dl.explain(format="json")[0]["Plan"]["Total Cost"]


class AggregateDailyLocation:
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [
        [True, False],
        ["last", "most-common"],
    ]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["is_cached", "daily_location_method"]
    timer = timeit.default_timer
    timeout = 1200

    def setup(self, *args):
        dl = daily_location(date="2016-01-01", method=args[-1])
        if args[-2]:
            dl.store().result()
        self.dl = dl.aggregate()
        self.dl.turn_off_caching()

    def time_aggregate_daily_location(self, *args):

        _ = self.dl.store().result()

    def track_aggregate_daily_location_cost(self, *args):
        return self.dl.explain(format="json")[0]["Plan"]["Total Cost"]


class ModalLocationWithCaching:
    timer = timeit.default_timer
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [
        [0, 3, 7],
        ["last", "most-common"],
    ]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["n_cached", "daily_location_method"]
    timeout = 1200

    def setup(self, *args):
        dates = [
            "2016-01-01",
            "2016-01-02",
            "2016-01-03",
            "2016-01-04",
            "2016-01-05",
            "2016-01-06",
            "2016-01-07",
        ]
        stored_daily_locs = [
            daily_location(date=date, method=args[-1]).store()
            for date in dates[: args[-2]]
        ]
        for d in stored_daily_locs:
            d.result()
        daily_locs = [daily_location(date=date, method=args[-1]) for date in dates]
        self.ml = ModalLocation(*daily_locs)
        self.ml.turn_off_caching()

    def time_modal_location(self, *args):
        _ = self.ml.store().result()

    def track_modal_location_cost(self, *args):
        return self.ml.explain(format="json")[0]["Plan"]["Total Cost"]


class AggregateModalLocation:
    timer = timeit.default_timer
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [[True, False]]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["is_cached"]
    timeout = 1200

    def setup(self, *args):
        dates = [
            "2016-01-01",
            "2016-01-02",
            "2016-01-03",
            "2016-01-04",
            "2016-01-05",
            "2016-01-06",
            "2016-01-07",
        ]
        daily_locs = [daily_location(date=date) for date in dates]
        ml = ModalLocation(*daily_locs)
        if args[-1]:
            ml.store().result()
        self.ml = ml.aggregate()
        self.ml.turn_off_caching()

    def time_aggregate_modal_location(self, *args):
        _ = self.ml.store().result()

    def track_aggregate_modal_location_cost(self, *args):
        return self.ml.explain(format="json")[0]["Plan"]["Total Cost"]


class FlowSuite:
    timer = timeit.default_timer
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [[0, 1, 2]]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["n_cached"]
    timeout = 1200

    def setup(self, *args):
        dates = [
            "2016-01-01",
            "2016-01-02",
            "2016-01-03",
            "2016-01-04",
            "2016-01-05",
            "2016-01-06",
            "2016-01-07",
        ]
        daily_locs = [daily_location(date=date) for date in dates]
        mls = [ModalLocation(*daily_locs[:3]), ModalLocation(*daily_locs[3:])]
        stored_mls = [ml.store() for ml in mls[: args[-1]]]
        for ml in stored_mls:
            ml.result()
        self.fl = Flows(*mls)
        self.fl.turn_off_caching()

    def time_flow(self, *args):
        _ = self.fl.store().result()

    def track_flow_cost(self, *args):
        return self.fl.explain(format="json")[0]["Plan"]["Total Cost"]


class TotalLocationEventsSuite:
    timer = timeit.default_timer
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + [
        ["cell", "admin3"],
        ["day", "min"],
        ["out", "both"],
    ]
    param_names = FLOWDB_CONFIG_PARAM_NAMES + ["level", "interval", "direction"]
    timeout = 1200

    def setup(self, *args):
        self.tle = TotalLocationEvents(
            "2016-01-01",
            "2016-01-07",
            level=args[-3],
            interval=args[-2],
            direction=args[-1],
        )
        self.tle.turn_off_caching()

    def time_total_location_events(self, *args):
        _ = self.tle.store().result()

    def track_total_location_events_cost(self, *args):
        return self.tle.explain(format="json")[0]["Plan"]["Total Cost"]