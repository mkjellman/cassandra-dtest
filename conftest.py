import pytest
from datetime import datetime

from psutil import virtual_memory


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.use_off_heap_memtables = False
        self.num_tokens = -1
        self.data_dir_count = -1
        self.force_execution_of_resource_intensive_tests = False
        self.skip_resource_intensive_tests = False

    def setup(self, request):
        self.use_vnodes = request.config.getoption("--use-vnodes")
        self.use_off_heap_memtables = request.config.getoption("--use-off-heap-memtables")
        self.num_tokens = request.config.getoption("--num-tokens")
        self.data_dir_count = request.config.getoption("--data-dir-count-per-instance")
        self.force_execution_of_resource_intensive_tests = request.config.getoption("--force-resource-intensive-tests")
        self.skip_resource_intensive_tests = request.config.getoption("--skip-resource-intensive-tests")


def pytest_addoption(parser):
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


def sufficient_system_resources_for_resource_intensive_tests():
    mem = virtual_memory()
    total_mem_gb = mem.total/1024/1024/1024
    print("total available system memory is %dGB" % total_mem_gb)
    # todo kjkj: do not hard code our bound.. for now just do 9 instances at 3gb a piece
    return total_mem_gb >= 9*3


@pytest.fixture
def fixture_cmdopt(request):
    return request.config.getoption("--kj-is-cool")


@pytest.fixture
def fixture_dtest_config(request):
    request.cls.dtest_config = DTestConfig()
    request.cls.dtest_config.setup(request)


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
    print("Starting execution of %s at %s" % (request.node.name, str(datetime.now())))


def pytest_collection_modifyitems(items, config):
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
    selected_items = []
    deselected_items = []

    sufficient_system_resources_resource_intensive = sufficient_system_resources_for_resource_intensive_tests()
    print("has sufficient resources? %s" % sufficient_system_resources_resource_intensive)

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
                print("SKIP: Deselecting test %s as test marked resource_intensive. To force execution of "
                      "this test re-run with the --force-resource-intensive-tests command line argument" % item.name)
            if not sufficient_system_resources_resource_intensive:
                deselect_test = True
                print("SKIP: Deselecting resource_intensive test %s due to insufficient system resources" % item.name)

        if item.get_marker("no_vnodes"):
            if config.getoption("--use-vnodes"):
                deselect_test = True
                print("SKIP: Deselecting test %s as the test requires vnodes to be disabled. To run this test, "
                      "re-run without the --use-vnodes command line argument" % item.name)

        # todo kjkj: deal with no_offheap_memtables mark

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items
