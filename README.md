# Schema migration for Cassandra

Simple python script for generate, execute and rollback [schema migrations](http://en.wikipedia.org/wiki/Schema_migration) in Cassandra.

## Installation

Download the script, give execution permission and install the python dependencies

```
$ chmod +x cassandra-migrations.py
$ pip install cassandra-driver
$ pip install blist
```

## Usage

### Help

Basic Help
```
./cassandra-migrations.py -h
```

Full Help
```bash
./cassandra-migrations.py help full
```

### Create Migration

```
./cassandra-migrations.py generate {keyspace} --name {name}
```
This creates a new file  /migrations/{keyspace}/20140914222010_{MigrationName}.xml
```xml
<?xml version="1.0" ?>
<migration>
<up>
    <!-- each CQL statment must be between <cql></cql> -->
    <cql><![CDATA[
Here cql up
    ]]></cql>
</up>
<down>
    <cql><![CDATA[
Here cql down
    ]]></cql>
</down>
</migration>
```

### Execute Migration

Apply migrations to a keyspace:
```
./cassandra-migrations.py migrate {keyspace}
```

for a remote server
```
./cassandra-migrations.py migrate {keyspace} --ip {serverIP}
```

for a remote server w/ authentication
```
./cassandra-migrations.py migrate {keyspace} --ip {serverIP} --username {username} --password {password}
```

### Rollback Migration
```
./cassandra-migrations.py rollback {keyspace}
```

## Get Latest Migration Version
```
./cassandra-migrations.py current {keyspace}
```
or
```
./cassandra-migrations.py current {keyspace} --ip {serverIP}
```

### Optional Settings
```
--ip {serverIp}
--port {C* Port}
--username {C* Username}
--password {C* Password}
--con {consistencyLevel} // ONE, EACH_QUARUM, ANY, LOCAL_QUARUM (default)
```

## Change Log

**[2015-12-29](https://github.com/ibspoof/cassandra-migrations/tree/2015-12-29)**
- Moved from using ordered params to named parameters
- Added support for passing username and password (--username, --password)
- Added support for setting C* port (--port)
- Added support for setting consistencyLevel (--con)
- Added check for C* driver version 2.7.1+ (older versions seem to have issues)
- Added method to return current version of migrations applied
- Added color output for all output

**[2015-01-02](https://github.com/ibspoof/cassandra-migrations/tree/2015-01-02)**
- Enables multiple CQL up/down executions in the same migration
- Ability to have migrations for multiple keyspaces with single installation
- Updated default migration XML template
- Added support for remote server migrations
- Better notifications and warnings/errors when running
- Help documentation
