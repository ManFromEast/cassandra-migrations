#!/usr/bin/env python

import argparse
import os
import re
import time
from xml.dom import minidom
import cassandra
from cassandra import (ConsistencyLevel, InvalidRequest)
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


class bcolors:
    def __init__(self):
        pass

    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


parser = argparse.ArgumentParser(description='Migrate cassandra schema',
                                 epilog="\nUse " + bcolors.OKGREEN + "./cassandra-migrations.py help full" +
                                        bcolors.ENDC + " for complete usage")
parser.add_argument('task', help='Task to run (generate|migrate|rollback|createKeyspace|help)')
parser.add_argument('keyspace', help="C* keyspace to use (global|local)")
parser.add_argument('--ip', default="127.0.0.1", help="Server IP address")
parser.add_argument('--port', default=9042, help="Server's Cassandra Port")
parser.add_argument('--con', default="LOCAL_QUARUM",
                    help="C* Insert Consistency, should be LOCAL_QUARUM for normal operations")
parser.add_argument('--username', help="C* Username")
parser.add_argument('--password', help="C* Password")
parser.add_argument('--name',
                    help="Name of schema migration to use when using 'generate' task. "
                         "Example output: 20150105110259_{name}.xml")
parser.add_argument('--timeout', default=60, help='Session default_timeout')

args = parser.parse_args()


def _versiontuple(v):
    return tuple(map(int, (v.split("."))))


base_driver_version = "2.7.1"
if _versiontuple(cassandra.__version__) < _versiontuple(base_driver_version):
    print "\n" + bcolors.WARNING + "WARNING: cassandra-driver is lower than " + base_driver_version + \
          " upgrade is highly recommended.\nUse: `pip install cassandra-driver --upgrade`" + bcolors.ENDC


def _app_help():
    print """
  Usage:

  Create new migration file
     """ + bcolors.OKGREEN + """./cassandra-migrations.py generate {keyspace} --name {name}""" + bcolors.ENDC + """

  Apply migrations to a keyspace:
     """ + bcolors.OKGREEN + """./cassandra-migrations.py migrate {keyspace}""" + bcolors.ENDC + """
  for a remote server
     """ + bcolors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --ip {serverIP}""" + bcolors.ENDC + """
  for a remote server w/ authentication
     """ + bcolors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --ip {serverIP} --username {username} --password {password}""" + bcolors.ENDC + """

  Setting port of C* to use:
     """ + bcolors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --port {port}""" + bcolors.ENDC + """

  Setting consistency of C* operations:
     """ + bcolors.OKGREEN + """./cassandra-migrations.py migrate {keyspace} --con {consistency}""" + bcolors.ENDC + """

  Rollback a migration:
     """ + bcolors.OKGREEN + """./cassandra-migrations.py rollback {keyspace}""" + bcolors.ENDC + """

  Create new keyspace (Note: only works on localhost)
     """ + bcolors.OKGREEN + """./cassandra-migrations.py createKeyspace {keyspace}""" + bcolors.ENDC + """

  Get latest migration for keyspace
     """ + bcolors.OKGREEN + """./cassandra-migrations.py current {keyspace}""" + bcolors.ENDC + """
     or
     """ + bcolors.OKGREEN + """./cassandra-migrations.py current {keyspace} --ip {serverIP}""" + bcolors.ENDC + """
  """


currentPath = os.path.dirname(os.path.abspath(__file__))
migrationPath = currentPath + '/migrations/' + args.keyspace + '/'

# set conLevel
if args.con == "ONE":
    conLevel = ConsistencyLevel.ONE
elif args.con == "EACH_QUORUM":
    conLevel = ConsistencyLevel.EACH_QUORUM
elif args.con == "ANY":
    conLevel = ConsistencyLevel.ANY
else:
    conLevel = ConsistencyLevel.LOCAL_QUORUM


def generate_migration():
    print args.name
    if args.name is None:
        _incorrect("Migration name must be provided (--name {name}).")

    file_name = time.strftime("%Y%m%d%H%M%S_") + _convert(args.name) + ".xml"

    default_text = """<?xml version="1.0" ?>
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
"""

    if not os.path.exists(migrationPath):
        os.makedirs(migrationPath)

    target = open(migrationPath + file_name, 'a')
    target.write(default_text)
    target.close()

    print bcolors.OKGREEN + "\nCreated migrations file: " + migrationPath + file_name + "\n" + bcolors.ENDC


def _convert(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def _connect():
    auth_provider = None

    if args.username is not None and args.password is not None:
        auth_provider = PlainTextAuthProvider(username=args.username, password=args.password)

    cluster = Cluster([args.ip], protocol_version=3, auth_provider=auth_provider, port=int(args.port))

    # connect to keyspace
    try:
        cluster = cluster.connect(args.keyspace)
    except InvalidRequest:
        _incorrect("Keyspace " + args.keyspace + "does not exist.")
    except Exception:
        _incorrect("Unable to connect to host " + args.ip + " using port " + str(args.port) +
                   " or username/password is incorrect.")

    return cluster


def migrate():
    session = _connect()

    session.default_timeout = args.timeout

    # check if schema migrations table exists
    query = SimpleStatement("SELECT * FROM schema_migrations LIMIT 1", consistency_level=conLevel)
    try:
        session.execute(query)
    except InvalidRequest:
        query = SimpleStatement("CREATE TABLE IF NOT EXISTS schema_migrations (version varchar PRIMARY KEY);",
                                consistency_level=conLevel)
        session.execute(query)

    f = []
    for (dirpath, dirnames, filenames) in os.walk(migrationPath):
        f.extend(filenames)
        break

    f = sorted(f)
    for filename in f:
        id_migration = filename.split('_')[0]

        query = SimpleStatement("SELECT COUNT(*) AS C FROM schema_migrations where version=%s",
                                consistency_level=conLevel)
        rows = session.execute(query, [id_migration])
        count = 0
        for c in rows:
            count = c[0]

        if count == 0:
            xmldoc = minidom.parse(migrationPath + filename)
            up = xmldoc.getElementsByTagName('up')[0]
            cqls = up.getElementsByTagName('cql')
            error = False

            for x in cqls:
                cql = x.firstChild.data
                try:
                    query = SimpleStatement(cql, consistency_level=ConsistencyLevel.LOCAL_ONE)
                    session.execute(query)
                    print "Executed up for (" + args.keyspace + '/' + filename + ")"
                    time.sleep(0.5)
                except InvalidRequest:
                    error = True
                    print bcolors.WARNING + "WARNING: Error occured while applying %s" % cql + bcolors.ENDC
            if error is True:
                print bcolors.OKBLUE + "NOTICE: Migration file " + args.keyspace + \
                      "/%s already applied" % filename + bcolors.ENDC
            else:
                query = SimpleStatement("INSERT INTO schema_migrations (version) VALUES (%s)",
                                        consistency_level=conLevel)
                session.execute(query, [id_migration])

    print bcolors.OKGREEN + "\nMigration complete.\n" + bcolors.ENDC


def current():
    session = _connect()
    versions = []

    query = SimpleStatement("SELECT version FROM schema_migrations",
                            consistency_level=conLevel)
    rows = session.execute(query)
    for c in rows:
        versions.append(c[0])
    versions = sorted(versions)

    print "\nCurrent Migration for '" + args.keyspace + "' keyspace: " + versions[-1] + "\n"
    return versions[-1]


def create():
    if args.ip is not None:
        _incorrect("Cannot create keyspace for remote server.")

    session = _connect()
    cql = "CREATE KEYSPACE IF NOT EXISTS " + args.keyspace + " WITH REPLICATION = " \
                                                             "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    query = SimpleStatement(cql, consistency_level=ConsistencyLevel.LOCAL_ONE)
    session.execute(query)
    print bcolors.OKGREEN + "\nKeyspace '" + args.keyspace + "' created with replication factor of 1\n" + bcolors.ENDC


def rollback():
    session = _connect()
    ff = []
    for (dirpath, dirnames, filenames) in os.walk(migrationPath + args.keyspace + '/'):
        ff.extend(filenames)
        break
    filename = ''
    id_migration = current()
    for f in ff:
        if f.find(id_migration) > -1:
            filename = f
    xmldoc = minidom.parse(migrationPath + filename)
    down = xmldoc.getElementsByTagName('down')[0]
    cqls = down.getElementsByTagName('cql')
    for x in cqls:
        cql = x.firstChild.data
        try:
            query = SimpleStatement(cql, consistency_level=ConsistencyLevel.LOCAL_ONE)
            session.execute(query)
            print bcolors.OKGREEN + "Executed down for (" + args.keyspace + '/' + filename + ")" + bcolors.ENDC
        except Exception:
            print bcolors.WARNING + "WARNING: Error occured while applying %s" % cql + bcolors.ENDC

    session.execute("DELETE FROM schema_migrations WHERE version=%s", [id_migration])
    # TODO: control first version


def _incorrect(message):
    if message is None:
        message = "Incorrect usage."
    print bcolors.FAIL + "\nERROR: " + message + bcolors.OKBLUE + \
                         "\n\nUse './cassandra-migrations.py -h' or " \
                         "'./cassandra-migrations.py help full' for commands\n" + bcolors.ENDC
    exit(1)


if args.task == "generate":
    generate_migration()
elif args.task == "createKeyspace":
    create()
elif args.task == "migrate":
    migrate()
elif args.task == "current":
    current()
elif args.task == "rollback":
    rollback()
elif args.task == "help":
    _app_help()
else:
    _incorrect(None)
