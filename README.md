# data-exporter-service

Service that performs scheduled batch jobs to export snapshots of shop data, for reporting purposes.

## Implementation

The service uses [RxJava/RxScala](https://github.com/ReactiveX/RxScala) to stream data from input databases to the output.

The schedule is configured in the `service.dataExporter.schedule` property in the configuration of the service.
This specifies a cron string that controls how often the service run. The default setting is to run at 3 am nightly.

## Developer install

To build the service, you need to have sbt version 0.13.6+ installed.

To run the service in development, use the command:

```
$ sbt run
```

By default, the service starts scheduling data export jobs at the given intervals. But there's also a command
line argument for running a one-off export, "--now". This is useful for testing and performing one-off update 
of reporting data. So, you can run it via sbt like this:

```
$ sbt run "--now"
```

To build a deployable version of the service, use the command:

```
$ sbt assembly
```

This builds a single JAR file that can be deployed without any other dependencies or application servers.
This is built using the sbt plugin [sbt-assembly](https://github.com/sbt/sbt-assembly), see 
its GitHub page for details on how it works.

## MySQL database creation

To set up the database tables, run the script db/schema.sql 
against the desired database server. For example:

```
$ mysql -u <username> -p <password> reporting < db/schema.sql
```

## Running the service

To run the service from a JAR file, you need to create a configuration file to specify how the service will connect to databases.
There is an example configuration file in [src/main/resources/application.conf]. 

This will not be included in the built fat JAR file, so to run it you need to specify the file to use using the `CONFIG_URL` environment
variable (see the Blinkbox Books common-config library for details).
