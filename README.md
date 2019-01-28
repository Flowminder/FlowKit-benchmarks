# FlowKit benchmarks

This is the benchmark suite for FlowKit.

These benchmarks track the performance of various features in FlowKit over time.

The benchmarks are run using [airspeed velocity](https://asv.readthedocs.io/en/stable/).

## Quick start

### Creating the Pipenv environment

First, create the pipenv environment to make the `asv` command available for running the benchmarks. This only needs to be done once.

```bash
pipenv install
```

### Running the benchmarks

Run the benchmarks using the provided script, which uses the `asv run` command:

```bash
bash run_benchmarks.sh --show-stderr
```

See the [asv documentation](https://asv.readthedocs.io/en/stable/commands.html#asv-run) for more options which can be passed to this script.

After running the benchmarks, you can then either commit and push the results or discard them (see options 1 and 2 below). We recommend that you add new results in separate commits from code changes.

```bash
# Option 1: commit the results and push them to Github
git add results/  # add any new benchmark results
git commit -m "Add results for commit <xxx> (run on <machine_name>)"
git push

# Option 2: discard the latest results
git clean -f results/
```

_Note:_ the very first time you run the benchmarks on a new machine, the `run_benchmarks.sh` command will ask you some questions to gather informations about the machine. See ["Running benchmarks"](https://asv.readthedocs.io/en/stable/using.html#running-benchmarks) in the asv docs for details.

#### Running the benchmarks against a local FlowKit clone

During development, you will typically want to run the benchmarks against a local clone of the FlowKit repository.
To do this, first create a copy of the file `asv.conf.json`:

```bash
cp asv.conf.json asv.conf.local.json
```

Then modify the `"repo"` setting so that it points to the location of your local FlowKit clone, and modify the `"branches"` setting to specify the branch or branches you would like to benchmark.

When running benchmarks, you also need to pass the option `--config=./asv.conf.local.json` to the `run_benchmarks.sh` script (or to `asv` directly).

_Note:_ it is also totally fine to edit `asv.conf.json` directly, but because this file is under version control it can become a little cumbersome to remember to exclude it from commits, which is why we recommend the above method with a separate local config file.

_Note:_ airspeed velocity will create and use an internal clone of the FlowKit repository, regardless of whether the `"repo"` setting points to a remote or local repository. Uncommitted changes will not be included in this internal clone. If you have made experimental changes which you want to benchmark, we recommend that you commit the changes, run the benchmarks, and then revert to the previous commit if you do not want to keep those changes.

### Publishing the benchmarks on Github-pages

After running the benchmarks, you can also publish them to Github pages as follows.

```bash
pipenv run asv gh-pages
```
