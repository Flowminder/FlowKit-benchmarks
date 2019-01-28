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
