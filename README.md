# Étoilé
[![Build Status](https://travis-ci.org/andreoss/etoile.svg?branch=master)](https://travis-ci.org/andreoss/etoile)
[![Maintainability](https://api.codeclimate.com/v1/badges/45765fb306089171912c/maintainability)](https://codeclimate.com/github/andreoss/etoile/maintainability)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=andreoss_etoile&metric=alert_status)](https://sonarcloud.io/dashboard?id=andreoss_etoile)
[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=andreoss_etoile&metric=ncloc)](https://sonarcloud.io/dashboard?id=andreoss_etoile)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=andreoss_etoile&metric=coverage)](https://sonarcloud.io/dashboard?id=andreoss_etoile)
[![Hits-of-Code](https://hitsofcode.com/github/andreoss/etoile)](https://hitsofcode.com/view/github/andreoss/etoile)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/andreoss/etoile.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/andreoss/etoile/context:java)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fandreoss%2Fetoile.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fandreoss%2Fetoile?ref=badge_shield)

**Étoilé** is a multi-purpose Spark ETL application.

## Features

* Dump (`dump`)
  Dumps a dataset to a different format.

* Partition validation (`pv`)
  Validates partitioning schema according to a provided formula.
  It uses `input_file_name()` UDF and compares directory name to the formula.
  More than one expression can be passed in case of nested partitions.

* Compare (`compare`)
  Compare two datasets by joining them on primary keys.

## Build

```
./mvnw clean install -DskipTests
```

## Option format

Options are prefixed, after prefixes stripped they are primaraly passed to `DataFrameReader` or `DataFrameWriter`, thus each its option can be specified (such as `delimiter` for csv format, or `dbtable` for JDBC format).
Prefixes could be `input`, `output`, `left`, `right`, depending on command.


## Run

Submit with `spark2-submit`:

```
spark2-submit --master local étoilé.jar --command=....
```

## Example 1: Dumping Hive table to local CSV files

```
--command=dump
--input.table=FOO.BAR
--output.format=csv
--output.path=file://tmp/dump-result
```
## Example 2: Dumping Oracle JDBC table to Avro
Option `hive-names` indicates that Hive-incompatable symbols should be removed from column names.

```
--command=dump
--input.format=jdbc
--input.url=jdbc...
--input.dbtable=FOO.BAR
--output.format=avro
--output.hive-names=true
--output.path=file://tmp/dump-result
```

## Example 3: Validate partitioning structure

With these two expressions the valid directory structure should be `id_part=1/cn_part=1` and so on.
Values after `=` should match the results of the expressions.

```
--command=pv
--expression.1=pmod(id, 10) as id_part
--expression.2=pmod(cn, 10) as cn_part
--input.format=parquet
--input.path=/data/my/table
--input.dbtable=FOO.BAR
--output.format=avro
--output.hive-names=true
--output.path=file://tmp/dump-result
```

## Example 4: Compare two datasets
Output will contain rows which not match.
```
--command=compare
--keys=id
--left.format=parquet
--left.path=/data/my/table1
--right.format=parquet
--right.path=/data/my/table2
--output.format=json
--output.path=file://tmp/-result
```

## Misc options

* `--<prefix>.drop` can be used to exclude one or more columns.
* `--<prefix>.sort` reorders dataset by column or expression.
* `--<prefix>.cast` applies cast to a column.
Example: `--output.cast=id:string,date:timestamp`
* `--<prefix>.convert` same as cast, but casts type to type without specified columns.
Example: `--output.convert=timestamp:string,decimal:timestamp`
* `--<prefix>.hive-names` convert names to hive-comtable, i.e by removing non-alphanumeric characters
* `--<prefix>.rename` renames a column.
Example: `--output.rename=id as iden`


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fandreoss%2Fetoile.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fandreoss%2Fetoile?ref=badge_large)