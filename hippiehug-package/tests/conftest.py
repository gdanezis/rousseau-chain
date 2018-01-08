import pytest
from hippiehug.RedisStore import RedisStore

@pytest.fixture
def rstore():
    redis = pytest.importorskip("redis")

    # XXX we need to create our own Redis process/instance
    # XXX instead of using and flushing (!) the global one
    r = redis.StrictRedis()
    r.flushdb()
    return RedisStore(r)

