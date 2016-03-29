from fabric.api import run, env, cd, put, get, execute, require, sudo, local, lcd, settings
from fabric.decorators import runs_once, roles, parallel

import sys
import time
import re
import boto3
ec2 = boto3.resource('ec2')


running_filter = [{'Name': 'instance-state-name', 'Values': ['running']}, {"Name":"tag:deploy_name", "Values":["kafka"]}]

def get_aws_machines():
    instances = ec2.instances.filter(Filters=running_filter)
    return ['ubuntu@' + i.public_dns_name for i in instances]


def parse_machines(s):
    urls = re.findall("ec2-.*.compute.amazonaws.com", s)
    names = [('ubuntu@' + u) for u in urls ]
    return names

all_machines = sorted(get_aws_machines())
kafka_nodes = all_machines

env.roledefs.update({
    'kafka': kafka_nodes
})

try:
    from config import *
except:
    print "Cannot import config.py -- ensure all config parameters are set, by copying config.py-sample."
    sys.exit()

@runs_once
def ec2freshstart():
    NUM_MACHINES = 1
    if len(all_machines) < NUM_MACHINES:
        missing = NUM_MACHINES - len(all_machines)
        instances = ec2.create_instances(
            ImageId='ami-b0c379c3', # Trusty, amd64, ebs
            InstanceType='t2.micro',
            SecurityGroupIds= [ SECURITY_GROUP ],
            MinCount=missing, 
            MaxCount=missing,
            UserData="Kafka",
            KeyName=KEY_NAME)

        response = ec2.create_tags(
            Resources=[i.id for i in instances],
            Tags=[{"Key":"deploy_name", "Value":"kafka"}]
        )

@runs_once
def ec2start():
    if len(all_machines) < NUM_MACHINES:
        missing = NUM_MACHINES - len(all_machines)
        instances = ec2.create_instances(
            ImageId=LATEST_KAFKA_AMI, # Trusty, amd64, ebs
            InstanceType='t2.medium',
            SecurityGroupIds= [ SECURITY_GROUP ],
            MinCount=missing, 
            MaxCount=missing,
            UserData="Kafka",
            KeyName=KEY_NAME)

        response = ec2.create_tags(
            Resources=[i.id for i in instances],
            Tags=[{"Key":"deploy_name", "Value":"kafka"}]
        )

@runs_once
def ec2list():
    instances = ec2.instances.filter(Filters=running_filter)
    for instance in instances:
        print(instance.id, instance.state["Name"], instance.public_dns_name)


@runs_once
def ec2stop():
    instances = ec2.instances.filter(Filters=running_filter)
    ids = [i.id for i in instances]
    try:
        ec2.instances.filter(InstanceIds=ids).stop()
        ec2.instances.filter(InstanceIds=ids).terminate()
    except Exception as e:
        print e

    for v in ec2.volumes.filter():
        print v

    # Now delete all remaining volumes
    #for v in ec2.volumes.filter():
    #    if v.state == "available":
    #        v.delete()
    #    else:
    #        print v

@roles("kafka")
def host_type():
    run('uname -s')

@roles("kafka")
def configure_AMI():
    run("sudo apt-get update")
    run("sudo apt-get -y install default-jre")
    # run("sudo apt-get -y install zookeeperd")
    run("mkdir -p ~/Downloads")
    run('wget "http://packages.confluent.io/archive/2.1/confluent-2.1.0-alpha1-2.11.7.tar.gz" -O ~/Downloads/kafka.tgz')
    run("tar -xvzf ~/Downloads/kafka.tgz --strip 1")

@runs_once
def ec2makeami():
    instances = list(ec2.instances.filter(Filters=running_filter))
    assert len(instances) == 1
    inst = instances[0]

    import time
    import datetime
    t = int(time.time())

    # Store into an AMI
    inst.create_image(
        Name='kafka-%s' % t,
        Description='A kafka configured instance ready to run (%s)' % datetime.datetime.today().isoformat(),
        )

@roles("kafka")
def config_kafka():
    [_, host] = env.host_string.split("@")

    # CONFIGURATION
    # Set the external server address

    try:
        ret = run("grep '^advertised.host.name' etc/kafka/server.properties")
    except:
        run("echo 'advertised.host.name=%s\n' >>etc/kafka/server.properties" % host)


@roles("kafka")
def start_kafka():
    [_, host] = env.host_string.split("@")

    # CONFIGURATION
    # Set the external server address
    try:
        ret = run("grep '^advertised.host.name' etc/kafka/server.properties")
    except:
        run("echo 'advertised.host.name=%s\n' >>etc/kafka/server.properties" % host)

    # Run Services
    run("./bin/zookeeper-server-start -daemon ./etc/kafka/zookeeper.properties", pty=False)
    print "Going to sleep for 5 sec ..."
    time.sleep(5)

    run("./bin/kafka-server-start -daemon ./etc/kafka/server.properties", pty=False)
    print "Going to sleep for 5 sec ..."
    time.sleep(5)

    run("nohup ./bin/schema-registry-start -daemon ./etc/schema-registry/schema-registry.properties > ~/kafka/schema-registry.log 2>&1 &", pty=False)

@roles("kafka")
def stop_kafka():
    run("./bin/schema-registry-stop")
    run("./bin/kafka-server-stop")
    pid = run("ps ax | grep -i '\.zookeeper' | grep java | grep -v grep | awk '{print $1}'")
    run("./bin/zookeeper-server-stop %s" % pid)
