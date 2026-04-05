from cassandra.cluster import Cluster


print("hello app")

# Connects to the cassandra server
cluster = Cluster(['cassandra-server'])

session = cluster.connect()

# displays the available keyspaces
rows = session.execute('DESC keyspaces')
for row in rows:
    print(row)