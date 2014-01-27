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

The service takes an optional command line parameter "--now". By default, the service starts scheduling 
data export jobs at the given intervals. But this parameter will make the service run a one-off export, then exit.
This is useful for testing and performing one-off dumps of reporting data. 

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

To set up the database tables use the active record migrations, first ensure that you have a 
properties file in the root directory of the project. You can use the same properties file that 
you use to run the service, for example by creating a symlink to this file like this:

```
$ ln -s src/main/resources/data-export-service.properties data-export-service.properties
```

Then to run the database scripts, run this command from the project root directory:

```
$ rake db:migrate
```

If you don't like running active record migrations and would rather run plain old SQL then 
set up a sacrificial database and use the `db:migrate_with_ddl` task instead, e.g.

```
$ rake db:migrate_with_ddl
```

This will output the SQL that was sent to the database in a file called "migration.sql" so you can run 
that on a different instance later. If you really want, you can set a different file name for the output, e.g.

```
$ rake db:migrate_with_ddl["my_file.sql"]
```

In case you need to roll back to the previous version, you can run:

```
$ rake db:rollback
```

## Running the server


To run the service, use a command like:

```
$ java -cp .:data-export-service-assembly-1.0.jar com.blinkboxbooks.mimir.export.DataExportService
```

Note that this example includes the current directory "." on the classpath, in which 
case properties files will be picked up from there.

