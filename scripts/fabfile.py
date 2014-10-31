from fabric.api import *
from fabric.contrib.files import *


@task
def deploy():
    sudo("apt-get update")
    sudo("apt-get install -y python-pip python-virtualenv")
    sudo("apt-get install -y redis-server")
    append("/etc/redis/redis.conf", "save 60 1", use_sudo=True)
    sudo("/etc/init.d/redis-server restart")

    run("virtualenv .env")
    run(".env/bin/pip install redis BeautifulSoup supervisor ipython")
    put("search.py")
    run("mkdir -p lib")
    put("lib/realtylink.py", "lib/")
    run("touch lib/__init__.py")
    put("supervisord.conf")


@task
def start():
    sudo("/etc/init.d/redis-server start")
    run(".env/bin/supervisord -c /home/ubuntu/supervisord.conf")


@task
def stop():
    run(".env/bin/supervisorctl stop all")


@task
def get_db():
    sudo('cp /var/lib/redis/dump.rdb /tmp')
    sudo('chmod a+r /tmp/dump.rdb')
    get('/tmp/dump.rdb')


@task
def put_db(db):
    sudo("/etc/init.d/redis-server stop")
    put(db, '/tmp')
    sudo('mv /tmp/dump.rdb /var/lib/redis/dump.rdb')
    sudo("/etc/init.d/redis-server start")
