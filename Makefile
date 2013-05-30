start:
	bin/cassandra; sleep 10; CASSANDRA_CONF=conf1 bin/cassandra

stop:
	kill `jps | grep Cassandra | cut -d' ' -f1`



deleteAll:
	sudo rm -rf /var/lib/cassandra/* /var/lib/cassandra1/*

showring:
	bin/nodetool -h 127.0.0.1 -p 7199 ring

