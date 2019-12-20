# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import timeit
import flowmachine
import docker
from abc import ABCMeta, abstractmethod
from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    daily_location,
    ModalLocation,
    Flows,
    TotalLocationEvents,
    HartiganCluster,
    CallDays,
    SubscriberLocations,
    EventScore,
    MeaningfulLocations,
    MeaningfulLocationsAggregate,
    MeaningfulLocationsOD,
    TotalNetworkObjects,
    AggregateNetworkObjects,
    LocationIntroversion,
    UniqueSubscriberCounts,
    RadiusOfGyration,
)
from flowmachine.features.dfs import DFSTotalMetricAmount
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from .utils import *
from .config import FLOWDB_CONFIGS, FLOWDB_CONFIG_PARAM_NAMES
from .flowdb_config import FlowDBConfig


def setup(*args):
    print(f"Running setup for {args}")
    docker_client = docker.from_env()

    flowdb_config = FlowDBConfig(*args[:6], root_directory=get_benchmark_dbs_dir())
    flowdb_container = flowdb_config.create_db(docker_client)
    redis_container = get_redis(docker_client)
    flowmachine.connect(
        flowdb_port=flowdb_container.port,
        flowdb_host=flowdb_container.host,
        flowdb_password="foo",
        redis_port=redis_container.port,
        redis_host=redis_container.host,
        redis_password="fm_redis",
    )
    flowmachine.redis_container = redis_container
    flowmachine.flowdb_container = flowdb_container
    flowmachine.flowdb_config = flowdb_config
    print(f"Connected. Resetting cache.")
    flowmachine.core.cache.reset_cache(
        flowmachine.core.Query.connection, flowmachine.core.Query.redis
    )
    if reuse_containers() and keep_containers_alive():
        pass
    elif reuse_containers():
        update_containers_to_remove_file(flowdb_container, redis_container)
        if not keep_flowdb_volumes():
            update_volumes_to_remove_file(flowdb_config)


def teardown(*args):
    print(f"Running teardown for {args}")
    benchmark_dbs_dir = get_benchmark_dbs_dir()
    try:
        print(
            f"Killing any queries still running on {flowmachine.flowdb_container.name}"
        )
        with flowmachine.core.Query.connection.engine.begin():  # Kill any running queries
            flowmachine.core.Query.connection.engine.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='flowmachine';"
            )
        print(f"Resetting cache.")
        flowmachine.core.cache.reset_cache(
            flowmachine.core.Query.connection, flowmachine.core.Query.redis
        )
        flowmachine.core.Query.connection.engine.dispose()
        del flowmachine.core.Query.connection
    finally:
        if reuse_containers() and keep_containers_alive():
            pass
        elif reuse_containers():
            pass
        else:
            print(f"Stopping {flowmachine.redis_container.name}")
            flowmachine.redis_container.stop()
            print(f"Stopping {flowmachine.flowdb_container.name}")
            flowmachine.flowdb_container.stop()
            if not keep_flowdb_volumes():
                print(f"Removing {benchmark_dbs_dir/flowmachine.flowdb_container.name}")
                shutil.rmtree(benchmark_dbs_dir / flowmachine.flowdb_config.volume_name)
                update_volumes_to_remove_file(flowdb_config.base)


class BaseBenchmarkSuite(metaclass=ABCMeta):
    """
    Base class for the query benchmarks. Defines two benchmarks:
    - time_query: Tracks the time taken to run and store the query.
    - track_cost: Runs 'explain' on the query, and tracks the total cost.

    The abstract method 'setup' must be defined in any derived class,
    to define self.query and perform any other setup.
    """

    timer = timeit.default_timer
    timeout = 1200
    version = 0

    _params = ()
    _param_names = ()

    @property
    def params(self):
        return [list(set(x)) for x in zip(*FLOWDB_CONFIGS)] + list(self._params)

    @property
    def param_names(self):
        return FLOWDB_CONFIG_PARAM_NAMES + list(self._param_names)

    @abstractmethod
    def setup(self, *args):
        raise NotImplementedError

    def time_query(self, *args):

        _ = self.query.store().result()

    def track_cost(self, *args):
        return self.query.explain(format="json")[0]["Plan"]["Total Cost"]


class DailyLocation(BaseBenchmarkSuite):
    _params = [["last", "most-common"]]
    _param_names = ["daily_location_method"]

    def setup(self, *args):
        self.query = daily_location(date="2016-01-01", method=args[-1])
        self.query.turn_off_caching()


class AggregateDailyLocation(BaseBenchmarkSuite):
    _params = [[True, False], ["last", "most-common"]]
    _param_names = ["is_cached", "daily_location_method"]

    def setup(self, *args):
        dl = daily_location(date="2016-01-01", method=args[-1])
        if args[-2]:
            dl.store().result()
        self.query = SpatialAggregate(locations=dl)
        self.query.turn_off_caching()


class ModalLocationWithCaching(BaseBenchmarkSuite):
    _params = [[0, 3, 7], ["last", "most-common"]]
    _param_names = ["n_cached", "daily_location_method"]
    timeout = 1800

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


class AggregateModalLocation(BaseBenchmarkSuite):
    _params = [[True, False]]
    _param_names = ["is_cached"]

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
        self.query = SpatialAggregate(locations=ml)
        self.query.turn_off_caching()


class FlowSuite(BaseBenchmarkSuite):
    _params = [[0, 1, 2]]
    _param_names = ["n_cached"]

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


class TotalLocationEventsSuite(BaseBenchmarkSuite):
    _params = [
        [{"spatial_unit_type": "cell"}, {"spatial_unit_type": "admin", "level": 3}],
        ["day", "min"],
        ["out", "both"],
    ]
    _param_names = ["spatial_unit", "interval", "direction"]

    def setup(self, *args):
        spatial_unit_params = args[-3]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        self.query = TotalLocationEvents(
            "2016-01-01",
            "2016-01-07",
            spatial_unit=spatial_unit,
            interval=args[-2],
            direction=args[-1],
        )
        self.query.turn_off_caching()


class HartiganClusterSuite(BaseBenchmarkSuite):
    _params = [[(4, 17), "all"], [0.1, 10.0]]
    _param_names = ["hours", "radius"]

    def setup(self, *args):
        self.query = HartiganCluster(
            calldays=CallDays(
                SubscriberLocations(
                    start="2016-01-01",
                    stop="2016-01-07",
                    hours=args[-2],
                    spatial_unit=make_spatial_unit("versioned-site"),
                )
            ),
            radius=args[-1],
        )
        self.query.turn_off_caching()


class EventScoreSuite(BaseBenchmarkSuite):
    _params = [
        [
            {"spatial_unit_type": "versioned-cell"},
            {"spatial_unit_type": "admin", "level": 3},
        ],
        [(4, 17), "all"],
    ]
    _param_names = ["spatial_unit", "hours"]

    def setup(self, *args):
        spatial_unit_params = args[-2]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        self.query = EventScore(
            start="2016-01-01",
            stop="2016-01-07",
            spatial_unit=spatial_unit,
            hours=args[-1],
        )
        self.query.turn_off_caching()


class MeaningfulLocationsSuite(BaseBenchmarkSuite):
    _params = [["day", "unknown"], [True, False]]
    _param_names = ["label", "caching"]

    def setup(self, *args):
        hc = HartiganCluster(
            calldays=CallDays(
                SubscriberLocations(
                    start="2016-01-01",
                    stop="2016-01-07",
                    spatial_unit=make_spatial_unit("versioned-site"),
                )
            ),
            radius=1.0,
        )
        es = EventScore(
            start="2016-01-01",
            stop="2016-01-07",
            spatial_unit=make_spatial_unit("versioned-site"),
        )
        do_caching = args[-1]
        if do_caching:
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


class MeaningfulLocationsAggregateSuite(BaseBenchmarkSuite):
    _params = [
        [
            {"spatial_unit_type": "admin", "level": 1},
            {"spatial_unit_type": "admin", "level": 3},
        ],
        [True, False],
    ]
    _param_names = ["spatial_unit", "caching"]

    def setup(self, *args):
        spatial_unit_params = args[-2]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        ml = MeaningfulLocations(
            clusters=HartiganCluster(
                calldays=CallDays(
                    SubscriberLocations(
                        start="2016-01-01",
                        stop="2016-01-07",
                        spatial_unit=make_spatial_unit("versioned-site"),
                    )
                ),
                radius=1.0,
            ),
            scores=EventScore(
                start="2016-01-01",
                stop="2016-01-07",
                spatial_unit=make_spatial_unit("versioned-site"),
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
        do_caching = args[-1]
        if do_caching:
            ml.store().result()
        self.query = MeaningfulLocationsAggregate(
            meaningful_locations=ml, spatial_unit=spatial_unit
        )
        self.query.turn_off_caching()


class MeaningfulLocationsODSuite(BaseBenchmarkSuite):
    _params = [
        [
            {"spatial_unit_type": "admin", "level": 1},
            {"spatial_unit_type": "admin", "level": 3},
        ],
        [True, False],
    ]
    _param_names = ["spatial_unit", "caching"]

    def setup(self, *args):
        spatial_unit_params = args[-2]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        ml1 = MeaningfulLocations(
            clusters=HartiganCluster(
                calldays=CallDays(
                    SubscriberLocations(
                        start="2016-01-01",
                        stop="2016-01-04",
                        spatial_unit=make_spatial_unit("versioned-site"),
                    )
                ),
                radius=1.0,
            ),
            scores=EventScore(
                start="2016-01-01",
                stop="2016-01-04",
                spatial_unit=make_spatial_unit("versioned-site"),
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
            clusters=HartiganCluster(
                calldays=CallDays(
                    SubscriberLocations(
                        start="2016-01-01",
                        stop="2016-01-07",
                        spatial_unit=make_spatial_unit("versioned-site"),
                    )
                ),
                radius=1.0,
            ),
            scores=EventScore(
                start="2016-01-05",
                stop="2016-01-07",
                spatial_unit=make_spatial_unit("versioned-site"),
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
        do_caching = args[-1]
        if do_caching:
            ml1.store().result()
            ml2.store().result()
        self.query = MeaningfulLocationsOD(
            meaningful_locations_a=ml1,
            meaningful_locations_b=ml2,
            spatial_unit=spatial_unit,
        )
        self.query.turn_off_caching()


class TotalNetworkObjectsSuite(BaseBenchmarkSuite):
    _params = [
        ["minute", "day"],
        ["cell", "versioned-cell", "versioned-site"],
        [
            {"spatial_unit_type": "admin", "level": 0},
            {"spatial_unit_type": "admin", "level": 3},
        ],
    ]
    _param_names = ["total_by", "network_object", "spatial_unit"]

    def setup(self, *args):
        spatial_unit_params = args[-1]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        self.query = TotalNetworkObjects(
            "2016-01-01",
            "2016-01-07",
            total_by=args[-3],
            network_object=make_spatial_unit(args[-2]),
            spatial_unit=spatial_unit,
        )
        self.query.turn_off_caching()


class AggregateNetworkObjectsSuite(BaseBenchmarkSuite):
    _params = [
        ["avg", "max", "min", "median", "mode", "stddev", "variance"],
        ["hour", "month"],
        [True, False],
    ]
    _param_names = ["statistic", "aggregate_by", "caching"]

    def setup(self, *args):
        tno = TotalNetworkObjects(
            "2016-01-01",
            "2016-01-07",
            total_by="minute",
            spatial_unit=make_spatial_unit("admin", level=3),
        )
        do_caching = args[-1]
        if do_caching:
            tno.store().result()
        self.query = AggregateNetworkObjects(
            total_network_objects=tno, statistic=args[-3], aggregate_by=args[-2]
        )
        self.query.turn_off_caching()


class DFSTotalMetricAmountSuite(BaseBenchmarkSuite):
    _params = [["amount", "discount", "fee", "commission"], ["admin0", "admin3"]]
    _param_names = ["metric", "aggregation_unit"]

    def setup(self, *args):
        self.query = DFSTotalMetricAmount(
            start_date="2016-01-01",
            end_date="2016-01-07",
            metric=args[-2],
            aggregation_unit=args[-1],
        )
        self.query.turn_off_caching()


class LocationIntroversionSuite(BaseBenchmarkSuite):
    _params = [
        [
            {"spatial_unit_type": "cell"},
            {"spatial_unit_type": "versioned-cell"},
            {"spatial_unit_type": "admin", "level": 3},
            {"spatial_unit_type": "admin", "level": 0},
        ],
        ["in", "both"],
    ]
    _param_names = ["spatial_unit", "direction"]

    def setup(self, *args):
        spatial_unit_params = args[-2]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        self.query = LocationIntroversion(
            "2016-01-01", "2016-01-07", spatial_unit=spatial_unit, direction=args[-1]
        )
        self.query.turn_off_caching()


class UniqueSubscriberCountsSuite(BaseBenchmarkSuite):
    _params = [
        [
            {"spatial_unit_type": "cell"},
            {"spatial_unit_type": "versioned-cell"},
            {"spatial_unit_type": "admin", "level": 3},
            {"spatial_unit_type": "admin", "level": 0},
        ],
        [(4, 17), "all"],
    ]
    _param_names = ["spatial_unit", "hours"]

    def setup(self, *args):
        spatial_unit_params = args[-2]
        spatial_unit = make_spatial_unit(**spatial_unit_params)
        self.query = UniqueSubscriberCounts(
            "2016-01-01", "2016-01-07", spatial_unit=spatial_unit, hours=args[-1]
        )
        self.query.turn_off_caching()


class RadiusOfGyrationSuite(BaseBenchmarkSuite):
    def setup(self, *args):
        self.query = RadiusOfGyration("2016-01-01", "2016-01-07")
        self.query.turn_off_caching()


class JoinedSpatialAggregateRadiusOfGyrationSuite(BaseBenchmarkSuite):
    _params = [
        ["avg", "max", "min", "median", "mode", "stddev", "variance"],
        [True, False],
        [True, False],
    ]
    _param_names = ["method", "metric_cached", "locations_cached"]

    def setup(self, *args):
        rog = RadiusOfGyration("2016-01-01", "2016-01-02")
        dl = daily_location(date="2016-01-01", method="last")
        if args[-2]:
            rog.store().result()
        if args[-1]:
            dl.store().result()
        self.query = JoinedSpatialAggregate(metric=rog, locations=dl, method=args[-3])
        self.query.turn_off_caching()
