import pytest
import glob
import os
import shutil
import time
import subprocess
import logging
import re
import tempfile
import subprocess
import sys

import cassandra
import ccmlib.repository
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster as PyCluster
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RetryPolicy, WhiteListRoundRobinPolicy, RoundRobinPolicy
from ccmlib.cluster import Cluster
from ccmlib.cluster_factory import ClusterFactory
from ccmlib.common import get_version_from_build, is_win

from dtest import (get_ip_from_node, make_execution_profile, get_auth_provider, get_port_from_node, get_eager_protocol_version)
from distutils.version import LooseVersion

from tools.context import log_filter

logger = logging.getLogger("DTestSetup")


def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)


class DTestSetup:
    def __init__(self):
        self.ignore_log_patterns = []
        self.cluster = None
        self.allow_log_errors = False
        self.connections = []
        self.log_saved_dir = None
        self.last_log = None
        self.test_path = self.get_test_path()
        self.enable_for_jolokia = False
        self.subprocs = []

    def get_test_path(self):
        test_path = tempfile.mkdtemp(prefix='dtest-')

        # ccm on cygwin needs absolute path to directory - it crosses from cygwin space into
        # regular Windows space on wmic calls which will otherwise break pathing
        if sys.platform == "cygwin":
            process = subprocess.Popen(["cygpath", "-m", test_path], stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            test_path = process.communicate()[0].rstrip()

        return test_path

    def glob_data_dirs(self, path, ks="ks"):
        result = []
        for node in self.cluster.nodelist():
            for data_dir in node.data_directories():
                ks_dir = os.path.join(data_dir, ks, path)
                result.extend(glob.glob(ks_dir))
        return result

    def _catch_interrupt(self, signal, frame):
        """
        Signal handler for registering on SIGINT.

        If called will look for a stored exception and raise it to abort test.
        If a stored exception is not present, this handler has likely caught a
        user interrupt via CTRL-C, and will raise a KeyboardInterrupt.
        """
        try:
            # check if we have a persisted exception to fail with
            raise self.exit_with_exception
        except AttributeError:
            # looks like this was just a plain CTRL-C event
            raise KeyboardInterrupt()

    def copy_logs(self, directory=None, name=None):
        """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
        if directory is None:
            directory = self.log_saved_dir
        if name is None:
            name = self.last_log
        else:
            name = os.path.join(directory, name)
        if not os.path.exists(directory):
            os.mkdir(directory)
        logs = [(node.name, node.logfilename(), node.debuglogfilename(), node.gclogfilename(),
                 node.compactionlogfilename())
                for node in list(self.cluster.nodes.values())]
        if len(logs) is not 0:
            basedir = str(int(time.time() * 1000)) + '_' + self.id()
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

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None, port=None, ssl_opts=None, **kwargs):

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def exclusive_cql_connection(self, node, keyspace=None, user=None,
                                 password=None, compression=True, protocol_version=None, port=None, ssl_opts=None,
                                 **kwargs):

        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression,
                                    protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr,
                                    **kwargs)

    def _create_session(self, node, keyspace, user, password, compression, protocol_version,
                        port=None, ssl_opts=None, execution_profiles=None, **kwargs):
        node_ip = get_ip_from_node(node)
        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = get_eager_protocol_version(node.cluster.version())

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(**kwargs)
                    } if not execution_profiles else execution_profiles

        cluster = PyCluster([node_ip],
                            auth_provider=auth_provider,
                            compression=compression,
                            protocol_version=protocol_version,
                            port=port,
                            ssl_options=ssl_opts,
                            connect_timeout=10,
                            allow_beta_protocol_version=True,
                            execution_profiles=profiles)
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        self.connections.append(session)
        return session

    def patient_cql_connection(self, node, keyspace=None,
                               user=None, password=None, timeout=30, compression=True,
                               protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        expected_log_lines = ('Control connection failed to connect, shutting down Cluster:',
                              '[control connection] Error connecting to ')
        with log_filter('cassandra.cluster', expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs
            )

        return session

    def patient_exclusive_cql_connection(self, node, keyspace=None,
                                         user=None, password=None, timeout=30, compression=True,
                                         protocol_version=None, port=None, ssl_opts=None, **kwargs):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        if is_win():
            timeout *= 2

        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs
        )

    def check_logs_for_errors(self):
        pytest.set_trace()
        for node in self.cluster.nodelist():
            errors = list(self.__filter_errors(
                ['\n'.join(msg) for msg in node.grep_log_for_errors()]))
            if len(errors) is not 0:
                for error in errors:
                    print("Unexpected error in {node_name} log, error: \n{error}".format(node_name=node.name, error=error))
                return True

    def __filter_errors(self, errors):
        """Filter errors, removing those that match self.ignore_log_patterns"""
        if not hasattr(self, 'ignore_log_patterns'):
            self.ignore_log_patterns = []
        for e in errors:
            for pattern in self.ignore_log_patterns:
                if re.search(pattern, e):
                    break
            else:
                yield e

    def get_jfr_jvm_args(self):
        """
        @return The JVM arguments required for attaching flight recorder to a Java process.
        """
        return ["-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder"]

    def start_jfr_recording(self, nodes):
        """
        Start Java flight recorder provided the cluster was started with the correct jvm arguments.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.start'],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def dump_jfr_recording(self, nodes):
        """
        Save Java flight recorder results to file for analyzing with mission control.
        """
        for node in nodes:
            p = subprocess.Popen(['jcmd', str(node.pid), 'JFR.dump',
                                  'recording=1', 'filename=recording_{}.jfr'.format(node.address())],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            logger.debug(stdout)
            logger.debug(stderr)

    def supports_v5_protocol(self, cluster_version):
        return cluster_version >= LooseVersion('4.0')