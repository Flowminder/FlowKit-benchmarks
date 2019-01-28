#!/usr/bin/env bash

# some useful options:
#   --show-stderr

# Note: for convenience we are passing the default value for --config here,
# but this can still be overwritten by the user if they manually provide
# the --config switch. There will be no error, it will simply use the
# user-provided value.
pipenv run asv run $@
