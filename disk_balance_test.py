import os
import os.path
import pytest
import logging

from dtest import Tester, create_ks
from tools.assertions import assert_almost_equal
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.misc import new_node

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('3.2')
class TestDiskBalance(Tester):
    """
    @jira_ticket CASSANDRA-6696
    """

    def test_disk_balance_stress(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        # make sure the data directories are balanced:
        for node in cluster.nodelist():
            self.assert_balanced(node)

    def test_disk_balance_bootstrap(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        # apparently we have legitimate errors in the log when bootstrapping (see bootstrap_test.py)
        self.fixture_dtest_setup.allow_log_errors = True
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        node5 = new_node(cluster)
        node5.start(wait_for_binary_proto=True)
        self.assert_balanced(node5)

    def test_disk_balance_decommission(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1 = cluster.nodes['node1']
        node4 = cluster.nodes['node4']
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=2)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        node4.decommission()

        for node in cluster.nodelist():
            node.nodetool('relocatesstables')

        for node in cluster.nodelist():
            self.assert_balanced(node)

    def test_blacklisted_directory(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(1)
        [node] = cluster.nodelist()
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)
        insert_c1c2(session, n=10000)
        node.flush()
        for k in range(0, 10000):
            query_c1c2(session, k)

        node.compact()
        mbean = make_mbean('db', type='BlacklistedDirectories')
        with JolokiaAgent(node) as jmx:
            jmx.execute_method(mbean, 'markUnwritable', [os.path.join(node.get_path(), 'data0')])

        for k in range(0, 10000):
            query_c1c2(session, k)

        node.nodetool('relocatesstables')

        for k in range(0, 10000):
            query_c1c2(session, k)

    def test_alter_replication_factor(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=1', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=1)'])
        cluster.flush()
        session = self.patient_cql_connection(node1)
        session.execute("ALTER KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=100'])
        cluster.flush()
        for node in cluster.nodelist():
            self.assert_balanced(node)

    def assert_balanced(self, node):
        sums = []
        for sstabledir in node.get_sstables_per_data_directory('keyspace1', 'standard1'):
            sum = 0
            for sstable in sstabledir:
                sum = sum + os.path.getsize(sstable)
            sums.append(sum)
        assert_almost_equal(*sums, error=0.1, error_message=node.name)
