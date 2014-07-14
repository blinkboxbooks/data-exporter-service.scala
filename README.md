data-exporter-service
=====================

Service that exports snapshots of shop data, for reporting purposes.

## Requirements

The data export service will will periodically update snapshots of shop data such as books, publishers, clubcards etc
to the reporting database.

A detailed list of data that is required for reporting is 
given at [this page on Confluence](https://tools.mobcastdev.com/confluence/display/AN/Reporting+DB+requirements+-+Shop+Data), along with suggested priorities.

## Developer install

To build the service, you need to have [sbt](acceptance-test/data-export-service-test.properties.example) version 0.13 installed.

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

You also need to create a configuration file to specify how the service will connect to databases.
There is an example configuration file in the source directory src/main/resources/data-export-service.properties.example.
Edit this with the appropriate parameters for your environment, rename it so the file has a ".properties" extension only,
and put this file on the classpath that's specified when running the service.

If you want to work on the code in an IDE, you'll want to generate the project files for your preferred IDE.
For example, for Eclipse you would do the following steps:

First ensure that you have [the sbt-eclipse plugin](https://github.com/typesafehub/sbteclipse) installed. Typically, for the sbt 0.13 onwards, that involves adding the following line in a file ~/.sbt/0.13/plugins/plugins.sbt:

```
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.4.0")
```

Once you have configured the plugin, you can generate the Eclipse project files using this command:

```
$ sbt eclipse with-source=true
```

## MySQL database creation

To set up the database tables, run the script db/schema.sql 
against the desired database server. For example:

```
$ mysql -u <username> -p <password> reporting < db/schema.sql
```

## Running the service

To run the service, you need to provide it with configuration, typically as a properties file.
An example file is in [src/main/resources/data-exporter-service.properties.example](https://git.mobcastdev.com/Mimir/data-exporter-service/blob/master/src/main/resources/data-exporter-service.properties.example).
Edit this as necessary, then put it in the directory you'll be running the service from.  

To start the service, use a command like:

```
$ java -jar data-export-service-assembly-1.0.jar
```

Note that the current directory "." is included on the classpath of the built jar file, so
you can run the service by putting the properties file here.

