# xcube-gedi

[![Unittest xcube-gedi](https://github.com/xcube-dev/xcube-gedi/actions/workflows/unittest.yml/badge.svg)](https://github.com/xcube-dev/xcube-gedi/actions/workflows/unittest.yml)
[![codecov xcube-gedi](https://codecov.io/github/xcube-dev/xcube-gedi/graph/badge.svg?token=pWeOFkbcL8)](https://codecov.io/github/xcube-dev/xcube-gedi)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License](https://img.shields.io/github/license/dcs4cop/xcube-smos)](https://github.com/xcube-dev/xcube-clms/blob/main/LICENSE)

The `xcube-gedi` Python package provides an
[xcube data store](https://xcube.readthedocs.io/en/latest/api.html#data-store-framework)
that enables access to datasets hosted by the
[Global Ecosystem Dynamics Investigation (GEDI)](https://gedi.umd.edu/).
The data store is called `"gedi"` and implemented as
an [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html).
It uses the [gedidb](https://gedidb.readthedocs.io/en/latest/)
package under the hood.

## Setup <a name="setup"></a>

### Installing the xcube-gedi plugin from the repository <a name="install_source"></a>

To install xcube-gedi directly from the git repository, clone the repository,
`cd` into `xcube-gedi`, and follow the steps below:

```bash
git clone https://github.com/xcube-dev/xcube-gedi.git
cd xcube-gedi
conda env create -f environment.yml
conda activate xcube-gedi
pip install .
```

This sets up a new conda environment, installs all the dependencies required
for `xcube-gedi`, and then installs `xcube-gedi` directly from the repository
into the environment.

### Installing the xcube-gedi plugin from the conda-forge

This method assumes that you have an existing environment, and you want to
install `xcube-gedi` into it.
With the existing environment activated, execute this command:

```bash
mamba install --channel conda-forge xcube-gedi
```

If xcube and any other necessary dependencies are not already installed, they
will be installed automatically.
