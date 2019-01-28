# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import docker
import shutil
import os
import json
from pathlib import Path
from hashlib import md5
from time import sleep
import pandas as pd
import psycopg2 as pg
from synthie.synthetic_cdr_data import export_synthetic_cdr_data
from halo import Halo
import logging


class FlowDBConfig:
    def __init__(
        self,
        num_days=7,
        num_subscribers=1000,
        num_cells=1000,
        num_calls_per_day=1000,
        analyze=False,
        cluster=False,
        jit=False,
        stats_target=1000,
        seed=1234,
        indexes=["msisdn", "msisdn_counterpart", "tac", "location_id", "datetime"],
        root_directory="./benchmark_dbs",
    ):
        self.num_days = num_days
        self.num_subscribers = num_subscribers
        self.num_calls_per_day = num_calls_per_day
        self.num_cells = num_cells
        self.analyze = analyze
        self.cluster = cluster
        self.jit = jit
        self.stats_target = stats_target
        self.indexes = sorted(indexes)
        self.seed = seed
        self.root_directory = Path.cwd() / Path(root_directory)
        self.commit = os.getenv("ASV_COMMIT")

    def __enter__(self):
        client = docker.from_env()
        self.flowdb_container = self.create_db(client)
        return self.flowdb_container

    def __exit__(self, exc_type, exc_value, traceback):
        self.flowdb_container.stop()
        client = docker.from_env()
        del self.flowdb_container

    def copy_base(self) -> Path:
        # Try and copy the base to a new directory.
        try:
            shutil.copytree(
                self.root_directory / self.base.volume_name,
                self.root_directory / self.volume_name,
            )
            os.remove(self.root_directory / self.volume_name / "tuned")
        except FileNotFoundError as e:  # Directory will be created
            (self.root_directory / self.volume_name).mkdir(parents=True)
            logging.debug(
                f"No base found, flowdb will set up at {self.root_directory / self.volume_name}"
            )
        except FileExistsError as e:  # Already setup here
            logging.debug(
                f"{self.root_directory / self.volume_name} already exists, setup will be skipped."
            )
        return self.root_directory / self.volume_name

    def create_db(self, client):
        try:
            flowdb_container = client.containers.get(self.volume_name)
            port_config = client.api.inspect_container(flowdb_container.id)[
                "NetworkSettings"
            ]["Ports"]["5432/tcp"][0]
            flowdb_container.port = port_config["HostPort"]
            flowdb_container.host = port_config["HostIp"]
            return flowdb_container
        except docker.errors.NotFound:
            pass  # Make the container instead
        dir_path = self.root_directory / self.volume_name
        spinner = Halo(
            text=f"Creating FlowDB for params {self.__dict__} at {self.volume_name}",
            spinner="dots",
        )
        spinner.start()
        spinner.info()
        if not self.base.is_created and self.volume_name != self.base.volume_name:
            spinner.text = f"Creating base flowdb ({self.base.__dict__}) "
            spinner.info()
            spinner.start()
            self.base.create_db(client).stop()
        else:
            if self.volume_name == self.base.volume_name:
                spinner.text = "Am base."
                spinner.info()
            if self.base.is_created:
                spinner.text = "Base already created."
                spinner.info()

        spinner.succeed()
        spinner.text = f"Copying base FlowDB from {self.base.volume_name}"
        spinner.start()
        db_path = self.copy_base()
        spinner.succeed()
        spinner.text = "Creating container"
        spinner.start()
        # In-container config
        environment = {
            "FM_PASSWORD": "foo",
            "API_PASSWORD": "foo",
            "POSTGRES_PASSWORD": "flowflow",
            "JIT": self.jit,
            "STATS_TARGET": self.stats_target,
        }
        # Create a container
        client.images.build(
            path=f"{os.getenv('ASV_ENV_DIR')}/project/flowdb",
            tag=f"flowdb:{self.commit}",
            rm=True,
        )
        flowdb_container = client.containers.run(
            f"flowdb:{self.commit}",
            name=self.volume_name,
            shm_size="1G",
            detach=True,
            healthcheck={"test": "pg_isready -h localhost"},
            auto_remove=True,
            environment=environment,
            ports={"5432/tcp": None},
            volumes={
                str(Path.cwd() / db_path): {
                    "bind": "/var/lib/postgresql/data",
                    "mode": "rw",
                }
            },
            user=f"{os.geteuid()}:{os.getegid()}",
        )
        spinner.succeed(f"Created container: {flowdb_container.name}")
        # Wait for container to be 'healthy'
        spinner.text = "Waiting for container to be ready."
        spinner.start()
        try:
            while (
                flowdb_container.exec_run("pg_isready -h localhost -U flowdb").exit_code
                != 0
            ):
                sleep(5)
        except Exception as e:
            logging.error(flowdb_container.__dict__["attrs"]["State"])
            logging.error(f"{e}")
            raise e
        # Force a config update and restart
        spinner.text = "Updating configuration.."
        flowdb_container.exec_run(
            "bash /docker-entrypoint-initdb.d/002_tweak_config.sh"
        )
        flowdb_container.restart()
        spinner.succeed()
        spinner.text = "Waiting for container to be ready."
        spinner.start()
        while (
            flowdb_container.exec_run("pg_isready -h localhost -U flowdb").exit_code
            != 0
        ):
            sleep(5)
        spinner.text = "Container ready."
        spinner.succeed()
        port_config = client.api.inspect_container(flowdb_container.id)[
            "NetworkSettings"
        ]["Ports"]["5432/tcp"][0]
        flowdb_container.port = port_config["HostPort"]
        flowdb_container.host = port_config["HostIp"]
        status = flowdb_container.exec_run(
            [
                "bash",
                "-c",
                f"echo '{self.config}' > /var/lib/postgresql/data/setup.conf",
            ]
        )
        if status.exit_code != 0:
            raise RuntimeError

        if (
            flowdb_container.exec_run(
                "[ -e /var/lib/postgresql/data/populated ]"
            ).exit_code
            == 1
        ):
            spinner.text = "Populating."
            spinner.start()
            self.populate_db(flowdb_container)
            flowdb_container.exec_run("touch /var/lib/postgresql/data/populated")
            spinner.succeed()
        else:
            spinner.text = "Populated."
            spinner.succeed()

        if (
            flowdb_container.exec_run("[ -e /var/lib/postgresql/data/tuned ]").exit_code
            == 1
        ):
            spinner.text = "Tuning."
            spinner.start()
            self.tune_table(flowdb_container)
            flowdb_container.exec_run("touch /var/lib/postgresql/data/tuned")
            spinner.succeed()
        else:
            spinner.text = "Tuned."
            spinner.succeed()

        return flowdb_container

    def populate_db(self, container):
        date_range = pd.date_range("2016-01-01", periods=self.num_days)
        conn_str = (
            f"postgresql://flowdb:flowflow@{container.host}:{container.port}/flowdb"
        )
        export_synthetic_cdr_data(
            conn_str=conn_str,
            reset_flowdb=True,
            progressbar=True,
            num_subscribers=self.num_subscribers,
            num_calls_per_day=self.num_calls_per_day,
            num_cells=self.num_cells,
            seed=self.seed,
            start_date=str(date_range[0].date()),
            end_date=str(date_range[-1].date()),
        )

    def tune_table(self, container):
        conn_str = (
            f"postgresql://flowdb:flowflow@{container.host}:{container.port}/flowdb"
        )
        conn = pg.connect(conn_str)
        date_range = [
            d.date().strftime("%Y%m%d")
            for d in pd.date_range("2016-01-01", periods=self.num_days)
        ]
        spinner = Halo(text="Tuning tables.", spinner="dots")
        spinner.start()
        with conn:
            with conn.cursor() as curs:
                for d in date_range:
                    table = f"calls_{d}"
                    for ix in self.indexes:
                        spinner.text = f"Indexing {table} on {ix}"
                        curs.execute(
                            f"CREATE INDEX IF NOT EXISTS {table}_{ix}_idx ON events.{table} ({ix});"
                        )
                        spinner.succeed()
                    if self.cluster and len(self.indexes) > 0:
                        spinner.text = (
                            f"Clustering {table} on {table}_{self.indexes[0]}_idx"
                        )
                        curs.execute(
                            f"CLUSTER events.{table} USING {table}_{self.indexes[0]}_idx;"
                        )
                        spinner.succeed()
                    if self.analyze:
                        spinner.text = f"Analyzing {table}"
                        curs.execute(f"ANALYZE events.{table};")
                        spinner.succeed()

                if self.analyze:
                    spinner.text = "Analyzing events.calls"
                    curs.execute(f"ANALYZE events.calls;")
                    spinner.succeed()
        conn.close()

    @property
    def config(self) -> str:
        return "\n".join(
            f"{param}:{json.dumps(getattr(self, param))}"
            for param in [
                "num_days",
                "num_subscribers",
                "num_calls_per_day",
                "num_cells",
                "analyze",
                "cluster",
                "stats_target",
                "indexes",
                "seed",
            ]
        )

    @property
    def volume_name(self) -> str:
        name_hash = md5()
        for param in [
            "num_days",
            "num_subscribers",
            "num_calls_per_day",
            "num_cells",
            "analyze",
            "cluster",
            "stats_target",
            "indexes",
            "seed",
            "jit",
            "commit",
        ]:
            name_hash.update(str(getattr(self, param)).encode())
        return f"flowdb_{name_hash.hexdigest()}"

    @property
    def is_created(self):
        return (self.root_directory / self.volume_name).is_dir() and (
            self.root_directory / self.volume_name / "populated"
        ).exists()

    @property
    def base(self):
        """
        Return a suitable 'base' flowdb to tweak.
        """
        return FlowDBConfig(
            num_days=self.num_days,
            num_calls_per_day=self.num_calls_per_day,
            num_cells=self.num_cells,
            analyze=False,
            cluster=False,
            jit=False,
            stats_target=1000,
            root_directory=self.root_directory,
        )
