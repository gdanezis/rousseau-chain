from paver.tasks import task, cmdopts
from paver.easy import sh, needs, pushd

import popen2

from docker import Client
cli = Client(base_url='unix://var/run/docker.sock', version="1.6.2")

@task
def build():
	sh("sudo docker build -t my-python-app .")

@task
def start():
	container = cli.create_container(
		image='my-python-app',
		name ='checker-app')
	response = cli.start(container='checker-app')

@task
def do():
	stdo, stdi = popen2.popen2(["sudo", "docker", "exec", "-i", "checker-app", "python", "/checker/checker.py"])
	stdi.write("Hello\r\nWorld!\r\nexit\r\n")
	stdi.flush()
	stdi.close()
	print stdo.read()

@task
def stop():
	cli.stop('checker-app')
	cli.remove_container(container='checker-app', v=True)

