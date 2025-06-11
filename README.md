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
package under the hood.

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

### Installing the `xcube-gedidb` plugin from the conda-forge

This method assumes that you have an existing environment, and you want to
install `xcube-gedidb` into it.
With the existing environment activated, execute this command:

```bash
mamba install --channel conda-forge xcube-gedidb
```

If xcube and any other necessary dependencies are not already installed, they
will be installed automatically.
