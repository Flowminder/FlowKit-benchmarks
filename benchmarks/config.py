#
# FLOWDB_BENCHMARK_INSTANCES describes what kind of synthetic CDR
# data should be installed in the flowdb instances. Currently there
# is only a single instance but we will likely want multiple ones
# in the future.
#
# Note: the port(s) specified below need to be kept in sync with the
#       ports provided as environment variables to the docker-compose
#       file used for bringing up the benchmark docker containers.
#

FLOWDB_CONFIG_PARAM_NAMES = ["num_days", "num_subscribers", "num_cells", "num_calls_per_day", "analyze", "cluster", "jit"]


FLOWDB_CONFIGS = [
    (7, 1000, 400, 10000, True, True, False),
]

