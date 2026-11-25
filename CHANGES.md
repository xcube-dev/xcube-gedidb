## Changes in 0.1.2

- Fix the GEDI database URL, changing `https://s3.gfz-potsdam.de` to 
  `https://s3.gfz.de`.
- Update the default database to the publicly available V3 database 
  (`dog-ext--gedidb--gedi-l2-l4-v003`), aligning with the changes 
  introduced in [gedidb v2026.11.19](https://github.com/simonbesnard1/gedidb/releases/tag/v2026.11.19).
- Improve data access performance by hardcoding metadata, eliminating the need 
  to retrieve it when opening the database.

## Changes in 0.1.1

Other changes:
* Added more tests
* Included third-party licensing information
* Added GitHub Actions workflow for publishing package to PyPI

## Changes in 0.1.0

Initial version of GediDB Data Store.