# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os


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
