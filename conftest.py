import pytest
from datetime import datetime
from distutils.version import LooseVersion
import logging
import os
import shutil
import time
import re
import platform
import copy
import inspect
import sys

from netifaces import AF_INET
import netifaces as ni

from psutil import virtual_memory

from ccmlib.cluster import Cluster
from ccmlib.common import get_version_from_build, is_win

from dtest import find_libjemalloc, init_default_config, cleanup_cluster, maybe_setup_jacoco, set_log_levels
from dtest_setup import DTestSetup

logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger()


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.use_off_heap_memtables = False
        self.num_tokens = -1
        self.data_dir_count = -1
        self.force_execution_of_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False
        self.cassandra_dir = None
        self.cassandra_version = None
        self.delete_logs = False
        self.cluster_options = []
        self.execute_upgrade_tests = False
        self.disable_active_log_watching = False

    def setup(self, request):
        self.use_vnodes = request.config.getoption("--use-vnodes")
        self.use_off_heap_memtables = request.config.getoption("--use-off-heap-memtables")
        self.num_tokens = request.config.getoption("--num-tokens")
        self.data_dir_count = request.config.getoption("--data-dir-count-per-instance")
        self.force_execution_of_resource_intensive_tests = request.config.getoption("--force-resource-intensive-tests")
        self.skip_resource_intensive_tests = request.config.getoption("--skip-resource-intensive-tests")
        self.cassandra_dir = request.config.getoption("--cassandra-dir")
        self.cassandra_version = request.config.getoption("--cassandra-version")
        self.delete_logs = request.config.getoption("--delete-logs")
        self.execute_upgrade_tests = request.config.getoption("--execute-upgrade-tests")
        self.disable_active_log_watching = request.config.getoption("--disable-active-log-watching")


def check_required_loopback_interfaces_available():
    """
    We need at least 3 loopback interfaces configured to run almost all dtests. On Linux, loopback
    interfaces are automatically created as they are used, but on Mac they need to be explicitly
    created. Check if we're running on Mac (Darwin), and if so check we have at least 3 loopback
    interfaces available, otherwise bail out so we don't run the tests in a known bad config and
    give the user some helpful advice on how to get their machine into a good known config
    """
    if platform.system() == "Darwin":
        if len(ni.ifaddresses('lo0')[AF_INET]) < 3:
            raise Exception("At least 3 loopback interfaces are required to run dtests. "
                            "On Mac you can create the required loopback interfaces by running "
                            "'for i in {1..9}; do sudo ifconfig lo0 alias 127.0.0.$i up; done;'")


def pytest_addoption(parser):
    # if we're on mac, check that we have the required loopback interfaces before doing anything!
    check_required_loopback_interfaces_available()

    parser.addoption("--use-vnodes", action="store_true", default=False,
                     help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--use-off-heap-memtables", action="store_true", default=False,
                     help="Enable Off Heap Memtables when creating test clusters for tests")
    parser.addoption("--num-tokens", action="store", default=256,
                     help="Number of tokens to set num_tokens yaml setting to when creating instances "
                          "with vnodes enabled")
    parser.addoption("--data-dir-count-per-instance", action="store", default=3,
                     help="Control the number of data directories to create per instance")
    parser.addoption("--force-resource-intensive-tests", action="store_true", default=False,
                     help="Forces the execution of tests marked as resource_intensive")
    parser.addoption("--skip-resource-intensive-tests", action="store_true", default=False,
                     help="Skip all tests marked as resource_intensive")
    parser.addoption("--cassandra-dir", action="store", default=None)
    parser.addoption("--cassandra-version", action="store", default=None)
    parser.addoption("--delete-logs", action="store_true", default=False)
    parser.addoption("--execute-upgrade-tests", action="store_true", default=False,
                     help="Execute Cassandra Upgrade Tests (e.g. tests annotated with the upgrade_test mark)")
    parser.addoption("--disable-active-log-watching", action="store_true", default=False,
                     help="Disable ccm active log watching, which will cause dtests to check for errors in the "
                          "logs in a single operation instead of semi-realtime processing by consuming "
                          "ccm _log_error_handler callbacks")


def sufficient_system_resources_for_resource_intensive_tests():
    mem = virtual_memory()
    total_mem_gb = mem.total/1024/1024/1024
    logger.info("total available system memory is %dGB" % total_mem_gb)
    # todo kjkj: do not hard code our bound.. for now just do 9 instances at 3gb a piece
    return total_mem_gb >= 9*3


@pytest.fixture
def fixture_cmdopt(request):
    return request.config.getoption("--kj-is-cool")


@pytest.fixture
def fixture_dtest_config(request):
    dtest_config = DTestConfig()
    dtest_config.setup(request)
    return dtest_config


@pytest.fixture(scope='function', autouse=True)
def fixture_maybe_skip_tests_requiring_novnodes(request):
    """
    Fixture run before the start of every test function that checks if the test is marked with
    the no_vnodes annotation but the tests were started with a configuration that
    has vnodes enabled. This should always be a no-op as we explicitly deselect tests
    in pytest_collection_modifyitems that match this configuration -- but this is explicit :)
    """
    if request.node.get_marker('no_vnodes'):
        if request.config.getoption("--use-vnodes"):
            pytest.skip("Skipping test marked with no_vnodes as tests executed with vnodes enabled via the "
                        "--use-vnodes command line argument")


@pytest.fixture(scope='function', autouse=True)
def fixture_log_test_name_and_date(request):
    logger.info("Starting execution of %s at %s" % (request.node.name, str(datetime.now())))


def _filter_errors(dtest_setup, errors):
    """Filter errors, removing those that match ignore_log_patterns in the current DTestSetup"""
    for e in errors:
        for pattern in dtest_setup.ignore_log_patterns:
            if re.search(pattern, repr(e)):
                break
        else:
            yield e


def check_logs_for_errors(dtest_setup):
    for node in dtest_setup.cluster.nodelist():
        errors = list(_filter_errors(dtest_setup, ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
        if len(errors) is not 0:
            has_errors = False
            for error in errors:
                if isinstance(error, (bytes, bytearray)):
                    error_str = error.decode("utf-8").strip()
                else:
                    error_str = error.strip()

                if error_str:
                    logger.error("Unexpected error in {node_name} log, error: \n{error}".format(node_name=node.name, error=error_str))
                    has_errors = True
            return has_errors


def copy_logs(request, cluster, directory=None, name=None):
    """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
    log_saved_dir = "logs"
    try:
        os.mkdir(log_saved_dir)
    except OSError:
        pass

    if directory is None:
        directory = log_saved_dir
    if name is None:
        name = os.path.join(log_saved_dir, "last")
    else:
        name = os.path.join(directory, name)
    if not os.path.exists(directory):
        os.mkdir(directory)
    logs = [(node.name, node.logfilename(), node.debuglogfilename(), node.gclogfilename(), node.compactionlogfilename())
            for node in list(cluster.nodes.values())]
    if len(logs) is not 0:
        basedir = str(int(time.time() * 1000)) + '_' + request.node.name
        logdir = os.path.join(directory, basedir)
        os.mkdir(logdir)
        for n, log, debuglog, gclog, compactionlog in logs:
            if os.path.exists(log):
                assert os.path.getsize(log) >= 0
                shutil.copyfile(log, os.path.join(logdir, n + ".log"))
            if os.path.exists(debuglog):
                assert os.path.getsize(debuglog) >= 0
                shutil.copyfile(debuglog, os.path.join(logdir, n + "_debug.log"))
            if os.path.exists(gclog):
                assert os.path.getsize(gclog) >= 0
                shutil.copyfile(gclog, os.path.join(logdir, n + "_gc.log"))
            if os.path.exists(compactionlog):
                assert os.path.getsize(compactionlog) >= 0
                shutil.copyfile(compactionlog, os.path.join(logdir, n + "_compaction.log"))
        if os.path.exists(name):
            os.unlink(name)
        if not is_win():
            os.symlink(basedir, name)


def reset_environment_vars(initial_environment):
    pytest_current_test = os.environ.get('PYTEST_CURRENT_TEST')
    os.environ.clear()
    os.environ.update(initial_environment)
    os.environ['PYTEST_CURRENT_TEST'] = pytest_current_test


#@pytest.fixture(scope='function', autouse=True)
@pytest.fixture(scope='function', autouse=False)
def fixture_dtest_setup(request, parse_dtest_config):
    logger.info("function yield fixture is doing setup")

    initial_environment = copy.deepcopy(os.environ)
    dtest_setup = DTestSetup()
    dtest_setup.cluster = initialize_cluster(parse_dtest_config, dtest_setup)

    if not parse_dtest_config.disable_active_log_watching:
        dtest_setup.log_watch_thread = dtest_setup.begin_active_log_watch()

    yield dtest_setup

    logger.info("function yield fixture is starting teardown")
    # test_is_ending prevents active log watching from being able to interrupt the test
    # which we don't want to happen once tearDown begins
    #self.test_is_ending = True

    reset_environment_vars(initial_environment)

    for con in dtest_setup.connections:
        con.cluster.shutdown()

    failed = False
    try:
        if not dtest_setup.allow_log_errors and check_logs_for_errors(dtest_setup):
            failed = True
            raise AssertionError('Unexpected error in log, see stdout')
    finally:
        try:
            # save the logs for inspection
            if failed or not parse_dtest_config.delete_logs:
                copy_logs(request, dtest_setup.cluster)
        except Exception as e:
            logger.error("Error saving log:", str(e))
        finally:
            #log_watch_thread = getattr(self, '_log_watch_thread', None)
            #cleanup_cluster(self.cluster, self.test_path, log_watch_thread)
            cleanup_cluster(dtest_setup)
            #if failed:
            #    cleanup_cluster(request.cls.cluster, request.cls.test_path, None)
            #    initialize_cluster(request, dtest_config)


def _skip_msg(current_running_version, since_version, max_version):
    if current_running_version < since_version:
        return "%s < %s" % (current_running_version, since_version)
    if max_version and current_running_version > max_version:
        return "%s > %s" % (current_running_version, max_version)


@pytest.fixture(autouse=True)
def fixture_since(request, fixture_dtest_setup):
    if request.node.get_marker('since'):
        max_version_str = request.node.get_marker('since').kwargs.get('max_version', None)
        max_version = None
        if max_version_str:
            max_version = LooseVersion(max_version_str)

        since_str = request.node.get_marker('since').args[0]
        since = LooseVersion(since_str)
        current_running_version = fixture_dtest_setup.cluster.version()
        skip_msg = _skip_msg(current_running_version, since, max_version)
        if skip_msg:
            pytest.skip(skip_msg)


#@pytest.yield_fixture(scope='class')
#def fixture_class_setup_and_teardown(request):
#    dtest_config = DTestConfig()
#    dtest_config.setup(request)
#    initialize_cluster(request, dtest_config)
#
#    yield
#
#    ##cleanup_cluster(request.cls.cluster, request.cls.test_path)


def create_ccm_cluster(test_path, name, config):
    log = logging.getLogger('create_ccm_cluster')
    log.debug("cluster ccm directory: %s", test_path)

    if config.cassandra_version:
        cluster = Cluster(test_path, name, cassandra_version=config.cassandra_version)
    else:
        cluster = Cluster(test_path, name, cassandra_dir=config.cassandra_dir)

    if config.use_vnodes:
        cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': config.num_tokens})
    else:
        cluster.set_configuration_options(values={'num_tokens': None})

    if config.use_off_heap_memtables:
        cluster.set_configuration_options(values={'memtable_allocation_type': 'offheap_objects'})

    cluster.set_datadir_count(config.data_dir_count)
    cluster.set_environment_variable('CASSANDRA_LIBJEMALLOC', find_libjemalloc())

    return cluster


@pytest.fixture(scope='function')
def parse_dtest_config(request):
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    yield dtest_config


def initialize_cluster(parse_dtest_config, dtest_setup):
    """
    This method is responsible for initializing and configuring a ccm
    cluster for the next set of tests.  This can be called for two
    different reasons:
     * A class of tests is starting
     * A test method failed/errored, so the cluster has been wiped

    Subclasses that require custom initialization should generally
    do so by overriding post_initialize_cluster().
    """
    #connections = []
    #cluster_options = []
    cluster = create_ccm_cluster(dtest_setup.test_path, name='test', config=parse_dtest_config)
    init_default_config(cluster, parse_dtest_config.cluster_options)

    maybe_setup_jacoco(parse_dtest_config, dtest_setup)
    set_log_levels(cluster)

    return cluster

    #cls.init_config()
    #write_last_test_file(cls.test_path, cls.cluster)

    #cls.post_initialize_cluster()


def pytest_collection_modifyitems(items, config):
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
    if not config.getoption("--collect-only") and config.getoption("--cassandra-dir") is None:
        if config.getoption("--cassandra-version") is None:
            raise Exception("Required dtest arguments were missing! You must provide either --cassandra-dir "
                            "or --cassandra-version. Refer to the documentation or invoke the help with --help.")

    selected_items = []
    deselected_items = []

    sufficient_system_resources_resource_intensive = sufficient_system_resources_for_resource_intensive_tests()
    logger.debug("has sufficient resources? %s" % sufficient_system_resources_resource_intensive)

    for item in items:
        #  set a timeout for all tests, it may be overwritten at the test level with an additional marker
        if not item.get_marker("timeout"):
            item.add_marker(pytest.mark.timeout(60*15))

        deselect_test = False

        if item.get_marker("resource_intensive"):
            if config.getoption("--force-resource-intensive-tests"):
                pass
            if config.getoption("--skip-resource-intensive-tests"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as test marked resource_intensive. To force execution of "
                      "this test re-run with the --force-resource-intensive-tests command line argument" % item.name)
            if not sufficient_system_resources_resource_intensive:
                deselect_test = True
                logger.info("SKIP: Deselecting resource_intensive test %s due to insufficient system resources" % item.name)

        if item.get_marker("no_vnodes"):
            if config.getoption("--use-vnodes"):
                deselect_test = True
                logger.info("SKIP: Deselecting test %s as the test requires vnodes to be disabled. To run this test, "
                      "re-run without the --use-vnodes command line argument" % item.name)

        for test_item_class in inspect.getmembers(item.module, inspect.isclass):
            if not hasattr(test_item_class[1], "pytestmark"):
                continue

            for module_pytest_mark in test_item_class[1].pytestmark:
                if module_pytest_mark.name == "upgrade_test":
                    if not config.getoption("--execute-upgrade-tests"):
                        deselect_test = True

        if item.get_marker("upgrade_test"):
            if not config.getoption("--execute-upgrade-tests"):
                deselect_test = True

        # todo kjkj: deal with no_offheap_memtables mark

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items
