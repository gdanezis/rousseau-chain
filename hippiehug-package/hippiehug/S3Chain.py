# We implement a chain that lives on Amazon S3
# For tests it necessary to have a configured AWS account.

from .Chain import DocChain, Document, Block, ascii_hash

try:
    import boto3
except:
    print("Cannot install Boto3")

from binascii import hexlify
from os import urandom

from json import dumps, loads
try:
    from Queue import Queue as Queue
except:
    from queue import Queue as Queue

from threading import Thread

def worker(q, bucket):
    while True:
        (key, value) = q.get()
        
        try:
            if isinstance(value, Document):
                bucket.put_object(Key="/Objects/%s" % key, ContentType="text/plain",
                    Body=value.item, Metadata={"type":"Document"})

            if isinstance(value, Block):
                encoded = dumps({"fingers":value.fingers, "items":value.items, "sequence": value.sequence})
                bucket.put_object(Key="/Objects/%s" % key, ContentType="text/plain",
                    Body=encoded, Metadata={"type":"Block"})
        except Exception as e:
            q.put((key, value))

        finally:
            q.task_done()


class S3Chain():
    def __init__(self, chain_name):
        """ Initialize the S3 chain with an S3 bucket name. """
        self.name = chain_name
        self.cache = {}

        # Make a connection to AWS S3
        self.s3 = boto3.resource('s3')

        try:
            self.s3.create_bucket(Bucket=self.name, ACL='public-read',
                CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        except:
            pass
        finally:
            self.bucket = self.s3.Bucket(self.name)

        # Get the old root if possible
        new_root = None
        try:
            o = self.s3.Object(self.name, "/root")
            if o.metadata["type"] == "Root":
                new_root = o.get()["Body"].read()
        except Exception as e:
            pass

        # Initialize the chain
        self.chain = DocChain(store=self, root_hash=new_root)

        # Initialize the thread pool
        num_worker_threads = 10
        self.q = Queue()
        for i in range(num_worker_threads):
             t = Thread(args=(self.q,self.bucket), target=worker)
             t.daemon = True
             t.start()

    def root(self):
        """ Returns the root of the chain. """
        return self.chain.root()

    def __getitem__(self, key):
        if key in self.cache:
            return self.cache[key]
        
        if len(self.cache) > 10000:
            self.cache = {} 

        self.q.join()

        o = self.s3.Object(self.name, "/Objects/%s" % key)
        
        if o.metadata["type"] == "Document":
            obj = Document(o.get()["Body"].read())

        if o.metadata["type"] == "Block":
            obj = loads(o.get()["Body"].read())
            obj = Block(items=obj["items"], sequence=obj["sequence"], fingers=obj["fingers"])


        self.cache[key] = obj
        return obj

    def __setitem__(self, key, value):
        if key in self.cache:
            return
        else:
            self.cache[key] = value

        self.q.put((key, value))



    def add(self, items):
        """ Add a new block with the given items. """
        self.chain.multi_add(items)    
        self.q.join()

        # Only commit the new head after everything else.
        new_root = self.chain.root()
        self.bucket.put_object(Key="/root", ContentType="text/plain",
                Body=new_root, Metadata={"type":"Root"})


    def get(self, bid, sid, evidence = None):
        """ Get the item at the block bid, position sid. Optionally, gather
        evidence for the proof."""
        return self.chain.get(bid, sid, evidence)

def __del__(self):
    pass # self.q.join()

## ====================================================
## -------------------- TESTS -------------------------


def xtest_create_bucket():
    test1name = ascii_hash(urandom(16))
    print("Name: %s" % test1name)

    try:
        s3c = S3Chain(test1name+"test")
        s3c2 = S3Chain(test1name+"test")

    except Exception as e:
        raise e
    finally:
        bucket = s3c.bucket
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()

def xtest_create_add():
    test1name = "chainspaceiotest1"
    print("Name: %s" % test1name)

    s3c = S3Chain(test1name+"test")
    s3c.add(["Hello","World"])

    assert s3c.get(0,0) == "Hello"

    evidence = {}
    s3c.get(0,0, evidence)

    d = DocChain(evidence, s3c.root())
    assert d.get(0,0) == "Hello"