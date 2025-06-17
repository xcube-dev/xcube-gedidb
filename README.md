# xcube-gedidb

[![Unittest xcube-gedidb](https://github.com/xcube-dev/xcube-gedidb/actions/workflows/unittest.yml/badge.svg)](https://github.com/xcube-dev/xcube-gedidb/actions/workflows/unittest.yml)
[![codecov xcube-gedidb](https://codecov.io/github/xcube-dev/xcube-gedidb/graph/badge.svg?token=pWeOFkbcL8)](https://codecov.io/github/xcube-dev/xcube-gedidb)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-clms/blob/main/LICENSE)

The `xcube-gedidb` Python package provides an
[xcube data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
that enables access to datasets hosted by the
[Global Ecosystem Dynamics Investigation (GEDI)](https://gedi.umd.edu/).
The data store is called `"gedidb"` and implemented as
an [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html).

It uses the [gedidb](https://gedidb.readthedocs.io/en/latest/)
package as a dependency, which is licensed under the 
[European Union Public License v1.2](https://github.com/simonbesnard1/gedidb/blob/main/LICENSE).

## Setup

### Installing the `xcube-gedidb` plugin from the repository

To install `xcube-gedidb` directly from the git repository, clone the repository,
`cd` into `xcube-gedidb` as follows and follow the steps below:

```bash
git clone https://github.com/xcube-dev/xcube-gedidb.git
cd xcube-gedidb
conda env create -f environment.yml
conda activate xcube-gedidb
pip install .
```

This sets up a new conda environment, installs all the dependencies required
for `xcube-gedidb`, and then installs `xcube-gedidb` directly from the repository
into the environment.

### Installing the `xcube-gedidb` plugin from the PyPi

This method assumes that you have an existing environment created from the 
[environment.yml](https://github.com/xcube-dev/xcube-gedidb/blob/main/environment.yml),
and you want to install `xcube-gedidb` into it.
With the existing environment activated, execute this command:

```bash
pip install xcube-gedi
```

If xcube and any other necessary dependencies are not already installed, they
will be installed automatically.


## Testing

To run the unit test suite:

```bash
pytest
```

## Some notes on the strategy of unit-testing for some tests

The unit test suite
uses [pytest-recording](https://pypi.org/project/pytest-recording/) to mock
https requests via the Python
library requests for some of the unit tests. During development an actual HTTP
request is performed and the
responses are saved in `cassettes/*.yaml` files. During testing, only the
`cassettes/*.yaml` files are used without an actual HTTP request. During
development, to save the responses to `cassettes/*.yaml`, run:

```bash
pytest -v -s --record-mode new_episodes
```

Note that --record-mode new_episodes overwrites all cassettes. If one only wants
to write cassettes which are not saved already, --record-mode once can be used.
pytest-recording supports all records modes given
by [VCR.py](https://vcrpy.readthedocs.io/en/latest/usage.html#record-modes.
After recording the
cassettes, testing can be then performed as usual.


## Citation:
This project uses the `gediDB` API (Besnard et al., 2025) as a dependency.

Besnard, S., Dombrowski, F., & Holcomb, A. (2025). gediDB (2025.2.0). Zenodo. 
https://doi.org/10.5281/zenodo.13885229