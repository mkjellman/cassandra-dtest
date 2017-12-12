Cassandra Distributed Tests (DTests)
====================================

Cassandra Distributed Tests (or better known as "DTests") are a set of Python-based 
tests for [Apache Cassandra](http://apache.cassandra.org) clusters. DTests aim to 
test functionality that requires multiple Cassandra instances. Functionality that
of code that can be tested in isolation should ideally be a unit test (which can be
found in the actual Cassandra repository). 

Setup and Prerequisites
------------

Some environmental setup is required before you can start running DTests.

### Native Dependencies
DTests requires the following native dependencies:
 * Python 3
 * PIP for Python 3 
 * libev
 * git
 * JDK 8 (Java)
 
#### Linux
1. ``apt-get install git-core python3 python3-pip python3-dev libev4 libev-dev``
2. Optional (solves warning: "jemalloc shared library could not be preloaded to speed up memory allocations"): 
``apt-get install -y --no-install-recommends libjemalloc1``
#### Mac
On Mac, the easiest path is to install the latest [Xcode and Command Line Utilities](https://developer.apple.com) to 
bootstrap your development environment and then use [Homebrew](https://brew.sh)

1. ``brew install python3 virtualenv libev``

### Python Dependencies
There are multiple external Python dependencies required to run DTests. 
The current Python depenendcy list is maintained in a file named 
[requirements.txt](https://github.com/apache/cassandra-dtest/blob/master/requirements.txt) 
in the root of the cassandra-dtest repository.

The easiest way to install these dependencies is with pip and virtualenv. 

**Note**: While virtualenv isn't strictly required, using virtualenv is almost always the quickest 
path to success as it provides common base setup across various configurations.``

1. Install virtualenv: ``pip install virtualenv``
2. Create a new virtualenv: ``virtualenv --python=python3 --no-site-packages ~/dtest``
3. Switch/Activate the new virtualenv: ``source ~/dtest/bin/activate``
4. Install remaining DTest Python dependencies: ``pip install -r /path/to/cassandra-dtest/requirements.txt``


Usage
-----

The tests are executed by the pytest framework. For convenience, a wrapper ``run_dtests.py`` 
is included with the intent to make starting execution of the dtests with sane defaults as easy 
as possible. Most users will most likely find that invoking the tests directly using ``pytest`` 
ultimately works the best and provides the most flexibility.

At minimum, 

  The only thing the framework needs to know is
the location of the (compiled) sources for Cassandra. There are two options:

Use existing sources:

    CASSANDRA_DIR=~/path/to/cassandra nosetests

Use ccm ability to download/compile released sources from archives.apache.org:

    CASSANDRA_VERSION=1.0.0 nosetests

A convenient option if tests are regularly run against the same existing
directory is to set a `default_dir` in `~/.cassandra-dtest`. Create the file and
set it to something like:

    [main]
    default_dir=~/path/to/cassandra

The tests will use this directory by default, avoiding the need for any
environment variable (that still will have precedence if given though).

Existing tests are probably the best place to start to look at how to write
tests.

Each test spawns a new fresh cluster and tears it down after the test. If a
test fails, the logs for the node are saved in a `logs/<timestamp>` directory
for analysis (it's not perfect but has been good enough so far, I'm open to
better suggestions).

To run the upgrade tests, you have must both JDK7 and JDK8 installed. Paths
to these installations should be defined in the environment variables
JAVA7_HOME and JAVA8_HOME, respectively.

Installation Instructions
-------------------------

See more detailed instructions in the included [INSTALL file](https://github.com/apache/cassandra-dtest/blob/master/INSTALL.md).

Writing Tests
-------------

- Most of the time when you start a cluster with `cluster.start()`, you'll want to pass in `wait_for_binary_proto=True` so the call blocks until the cluster is ready to accept CQL connections. We tried setting this to `True` by default once, but the problems caused there (e.g. when it waited the full timeout time on a node that was deliberately down) were more unpleasant and more difficult to debug than the problems caused by having it `False` by default.
- If you're using JMX via [the `tools.jmxutils` module](tools/jmxutils.py), make sure to call `remove_perf_disable_shared_mem` on the node or nodes you want to query with JMX _before starting the nodes_. `remove_perf_disable_shared_mem` disables a JVM option that's incompatible with JMX (see [this JMX ticket](https://github.com/rhuss/jolokia/issues/198)). It works by performing a string replacement in the node's Cassandra startup script, so changes will only propagate to the node at startup time.

If you'd like to know what to expect during a code review, please see the included [CONTRIBUTING file](CONTRIBUTING.md).

Links
-------
 * [ccm](https://github.com/pcmanus/ccm)
 * [pytest](https://docs.pytest.org/)
 * [Python Driver](http://datastax.github.io/python-driver/installation.html)
 * [CQL over Thrift Driver](http://code.google.com/a/apache-extras.org/p/cassandra-dbapi2/)