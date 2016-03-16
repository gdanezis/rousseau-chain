import os.path
import os
import re
import fnmatch

from paver.tasks import task, cmdopts
from paver.easy import sh, needs, pushd

@task
def startkafka(quiet=False):
    """ Start the Kafka deamon. """
    sh("""nohup ~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties > ~/kafka/kafka.log 2>&1 &""")

@task
def stopkafka(quiet=False):
    """ Stop the Kafka deamon. """
    sh("""~/kafka/bin/kafka-server-stop.sh""")

@task
def testkafka():
	sh("""echo "Hello, World" | ~/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic TutorialTopic > /dev/null""")
	x = sh("""~/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TutorialTopic --from-beginning """)
	print x

@task
def shards():
	""" Write out 8 shards and their bounds. """
	from binascii import hexlify
	from os import urandom

	limits = sorted([hexlify(urandom(32)) for _ in range(7)])
	limits = ["0" * 64] + limits + ["f" * 64]

	shards = []
	for i, (b0, b1) in enumerate(zip(limits[:-1],limits[1:])):
		name  = "n%s" % i 
		bounds = [b0, b1]
		shards.append([name, bounds])

	from json import dumps
	file("shards.json","w").write(dumps(shards))


