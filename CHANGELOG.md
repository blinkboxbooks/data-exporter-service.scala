# Data Exporter Service Change Log

## 3.0.3 ([#24](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/24) 2014-10-17 16:09:46)

Fix memory usage and too long contributor URLs.

### Bug fixes:

- CP-1944: Enable streaming of query results from input databases, to avoid using a lot of memory when copying large tables.
- CP-1974: Truncate long generated contributor URLs to avoid potential failure when writing to contributors table.


## 3.0.2 ([#23](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/23) 2014-09-22 15:15:05)

CP-1795 Fixed leaking DB connections

### Bug fix:

- Stop leaking DB connections, which caused the connection pool to run out of connections after a number of jobs, causing the service to stop running further jobs.


## 3.0.1 ([#22](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/22) 2014-08-20 07:57:34)

Fixed incorrect column data-type

PATCH

## 3.0.0 ([#21](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/21) 2014-08-07 13:25:12)

Moved discount from publisher to book and amended tests accordingly

### Breaking Change

* Discount is now dynamic (per-book) rather than per-publisher.

## 2.0.3 ([#20](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/20) 2014-07-25 11:08:45)

Don't require DB on startup

### Improvements:

- Don't exit on startup if DB is not immediately available.


## 2.0.2 ([#19](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/19) 2014-07-24 12:40:10)

Simplified config

### Improvements:

* Moved parts of config into reference.conf


## 1.0.4 ([#18](https://git.mobcastdev.com/Mimir/data-exporter-service/pull/18) 2014-07-23 17:04:58)

Bump minor version number.

Bug fix: bump minor version number.

## 1.0.3 (2014-06-05 12:00)

Last manually versioned build.
