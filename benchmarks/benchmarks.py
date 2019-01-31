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
    subscriber_location_cluster,
    EventScore,
    MeaningfulLocations,
    MeaningfulLocationsAggregate,
    MeaningfulLocationsOD,
)
from .utils import get_env_var
from .config import FLOWDB_CONFIGS, FLOWDB_CONFIG_PARAM_NAMES
from .flowdb_config import FlowDBConfig


def make_params(params_dict):
    """
    Return a list of params and a list of their names, including the FlowDB
    params defined in config.py and any additional params in params_dict.
    """
    params = [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + list(params_dict.values())
    param_names = FLOWDB_CONFIG_PARAM_NAMES + list(params_dict.keys())
    return params, param_names


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
    params, param_names = make_params(
        {"daily_location_method": ["last", "most-common"]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        self.query = daily_location(date="2016-01-01", method=args[-1])
        self.query.turn_off_caching()

    def time_daily_location(self, *args):

        _ = self.query.store().result()

    def track_daily_location_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class AggregateDailyLocation:
    params, param_names = make_params(
        {"is_cached": [True, False], "daily_location_method": ["last", "most-common"]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        dl = daily_location(date="2016-01-01", method=args[-1])
        if args[-2]:
            dl.store().result()
        self.query = dl.aggregate()
        self.query.turn_off_caching()

    def time_aggregate_daily_location(self, *args):

        _ = self.query.store().result()

    def track_aggregate_daily_location_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class ModalLocationWithCaching:
    params, param_names = make_params(
        {"n_cached": [0, 3, 7], "daily_location_method": ["last", "most-common"]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

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
        self.query = ModalLocation(*daily_locs)
        self.query.turn_off_caching()

    def time_modal_location(self, *args):
        _ = self.query.store().result()

    def track_modal_location_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class AggregateModalLocation:
    params, param_names = make_params({"is_cached": [True, False]})
    timer = timeit.default_timer
    timeout = 1200
    version = 0

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
        self.query = ml.aggregate()
        self.query.turn_off_caching()

    def time_aggregate_modal_location(self, *args):
        _ = self.query.store().result()

    def track_aggregate_modal_location_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class FlowSuite:
    params, param_names = make_params({"n_cached": [0, 1, 2]})
    timer = timeit.default_timer
    timeout = 1200
    version = 0

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
        self.query = Flows(*mls)
        self.query.turn_off_caching()

    def time_flow(self, *args):
        _ = self.query.store().result()

    def track_flow_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class TotalLocationEventsSuite:
    params, param_names = make_params(
        {
            "level": ["cell", "admin3"],
            "interval": ["day", "min"],
            "direction": ["out", "both"],
        }
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        self.query = TotalLocationEvents(
            "2016-01-01",
            "2016-01-07",
            level=args[-3],
            interval=args[-2],
            direction=args[-1],
        )
        self.query.turn_off_caching()

    def time_total_location_events(self, *args):
        _ = self.query.store().result()

    def track_total_location_events_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class HartiganClusterSuite:
    params, param_names = make_params(
        {"hours": [(4, 17), "all"], "radius": [0.1, 10.0]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        self.query = subscriber_location_cluster(
            "hartigan", "2016-01-01", "2016-01-07", hours=args[-2], radius=args[-1]
        )
        self.query.turn_off_caching()

    def time_hartigan_cluster(self, *args):
        _ = self.query.store().result()

    def track_hartigan_cluster_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class EventScoreSuite:
    params, param_names = make_params(
        {"level": ["versioned-cell", "admin3"], "hours": [(4, 17), "all"]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        self.query = EventScore(
            start="2016-01-01", stop="2016-01-07", level=args[-2], hours=args[-1]
        )
        self.query.turn_off_caching()

    def time_event_score(self, *args):
        _ = self.query.store().result()

    def track_event_score_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class MeaningfulLocationsSuite:
    params, param_names = make_params(
        {"label": ["day", "unknown"], "caching": [True, False]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        hc = subscriber_location_cluster(
            "hartigan", "2016-01-01", "2016-01-07", radius=1.0
        )
        es = EventScore(start="2016-01-01", stop="2016-01-07", level="versioned-site")
        if args[-1]:
            hc.store().result()
            es.store().result()
        self.query = MeaningfulLocations(
            clusters=hc,
            scores=es,
            labels={
                "evening": {
                    "type": "Polygon",
                    "coordinates": [
                        [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                    ],
                },
                "day": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                    ],
                },
            },
            label=args[-2],
        )
        self.query.turn_off_caching()

    def time_meaningful_locations(self, *args):
        _ = self.query.store().result()

    def track_meaningful_locations_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class MeaningfulLocationsAggregateSuite:
    params, param_names = make_params(
        {"level": ["admin1", "admin3"], "caching": [True, False]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        ml = MeaningfulLocations(
            clusters=subscriber_location_cluster(
                "hartigan", "2016-01-01", "2016-01-07", radius=1.0
            ),
            scores=EventScore(
                start="2016-01-01", stop="2016-01-07", level="versioned-site"
            ),
            labels={
                "evening": {
                    "type": "Polygon",
                    "coordinates": [
                        [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                    ],
                },
                "day": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                    ],
                },
            },
            label="unknown",
        )
        if args[-1]:
            ml.store().result()
        self.query = MeaningfulLocationsAggregate(
            meaningful_locations=ml, level=args[-2]
        )
        self.query.turn_off_caching()

    def time_meaningful_locations_aggregate(self, *args):
        _ = self.query.store().result()

    def track_meaningful_locations_aggregate_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class MeaningfulLocationsODSuite:
    params, param_names = make_params(
        {"level": ["admin1", "admin3"], "caching": [True, False]}
    )
    timer = timeit.default_timer
    timeout = 1200
    version = 0

    def setup(self, *args):
        ml1 = MeaningfulLocations(
            clusters=subscriber_location_cluster(
                "hartigan", "2016-01-01", "2016-01-04", radius=1.0
            ),
            scores=EventScore(
                start="2016-01-01", stop="2016-01-04", level="versioned-site"
            ),
            labels={
                "evening": {
                    "type": "Polygon",
                    "coordinates": [
                        [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                    ],
                },
                "day": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                    ],
                },
            },
            label="day",
        )
        ml2 = MeaningfulLocations(
            clusters=subscriber_location_cluster(
                "hartigan", "2016-01-05", "2016-01-07", radius=1.0
            ),
            scores=EventScore(
                start="2016-01-05", stop="2016-01-07", level="versioned-site"
            ),
            labels={
                "evening": {
                    "type": "Polygon",
                    "coordinates": [
                        [[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]
                    ],
                },
                "day": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]
                    ],
                },
            },
            label="evening",
        )
        if args[-1]:
            ml1.store().result()
            ml2.store().result()
        self.query = MeaningfulLocationsOD(
            meaningful_locations_a=ml1, meaningful_locations_b=ml2, level=args[-2]
        )
        self.query.turn_off_caching()

    def time_meaningful_locations_aggregate(self, *args):
        _ = self.query.store().result()

    def track_meaningful_locations_aggregate_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]
