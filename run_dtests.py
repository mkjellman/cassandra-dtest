#!/usr/bin/env python
"""
Usage: run_dtests.py [--nose-options NOSE_OPTIONS] [TESTS...] [--vnodes VNODES_OPTIONS...]
                 [--runner-debug | --runner-quiet] [--dry-run]

nosetests options:
    --nose-options NOSE_OPTIONS  specify options to pass to `nosetests`.
    TESTS                        space-separated list of tests to pass to `nosetests`

script configuration options:
    --runner-debug -d            print debug statements in this script
    --runner-quiet -q            quiet all output from this script

cluster configuration options:
    --vnodes VNODES_OPTIONS...   specify whether to run with or without vnodes.
                                 valid values: 'true' and 'false'

example:
    The following command will execute nosetests with the '-v' (verbose) option, vnodes disabled, and run a single test:
    ./run_dtests.py --nose-options -v --vnodes false repair_tests/repair_test.py:TestRepair.token_range_repair_test_with_cf

"""


import subprocess
import sys
import os
from collections import namedtuple
from os import getcwd, environ
from tempfile import NamedTemporaryFile

from _pytest.config import Parser
import argparse

from conftest import pytest_addoption


class ValidationResult(namedtuple('_ValidationResult', ['serialized', 'error_messages'])):
    """
    A value to be returned from validation functions. If serialization works,
    return one with 'serialized' set, otherwise return a list of string on the
    'error_messages' attribute.
    """
    __slots__ = ()

    def __new__(cls, serialized=None, error_messages=None):
        if error_messages is None:
            error_messages = []

        success_result = serialized is not None
        failure_result = bool(error_messages)

        if success_result + failure_result != 1:
            msg = ('attempted to instantiate a {cls_name} with serialized='
                   '{serialized} and error_messages={error_messages}. {cls_name} '
                   'objects must be instantiated with either a serialized or '
                   'error_messages argument, but not both.')
            msg = msg.format(cls_name=cls.__name__,
                             serialized=serialized,
                             error_messages=error_messages)
            raise ValueError(msg)

        return super(ValidationResult, cls).__new__(cls, serialized=serialized, error_messages=error_messages)


def _validate_and_serialize_vnodes(vnodes_value):
    """
    Validate the values received for vnodes configuration. Returns a
    ValidationResult.

    If the values validate, return a ValidationResult with 'serialized' set to
    the equivalent of:

        tuple(set({'true': True, 'false':False}[v.lower()] for v in vnodes_value))

    If the values don't validate, return a ValidationResult with 'messages' set
    to a list of strings, each of which points out an invalid value.
    """
    messages = []
    vnodes_value = set(v.lower() for v in vnodes_value)
    value_map = {'true': True, 'false': False}

    for v in vnodes_value:
        if v not in value_map:
            messages.append('{} not a valid value for --vnodes option. '
                            'valid values are {} (case-insensitive)'.format(v, ', '.join(list(value_map))))

    if messages:
        return ValidationResult(error_messages=messages)

    serialized = tuple({value_map[v] for v in vnodes_value})
    return ValidationResult(serialized=serialized)


class RunDTests():
    def log_debug(self, msg):
        if self.verbosity_level < 2:
            return
        print(msg)

    def log_info(self, msg):
        if self.verbosity_level < 1:
            return
        print (msg)

    def run(self, *kwargs):
        parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog,
                                                                                                             max_help_position=100,
                                                                                                             width=200))

        # this is a bit ugly: all of our command line arguments are added and configured as part
        # of pytest. however, we also have this wrapper script to make it easier for those who
        # aren't comfortable calling pytest directly. To avoid duplicating code (e.g. have the options
        # in two separate places) we directly use the pytest_addoption fixture from conftest.py. Unfortunately,
        # pytest wraps ArgumentParser, so, first we add the options to a pytest Parser, and then we pull
        # all of those custom options out and add them to the unwrapped ArgumentParser we want to use
        # here inside of run_dtests.py.
        #
        # So NOTE: to add a command line argument, if you're trying to do so by adding it here, you're doing it wrong!
        # add it to conftest.py:pytest_addoption
        pytest_parser = Parser()
        pytest_addoption(pytest_parser)

        # add all of the options from the pytest Parser we created, and add them into our ArgumentParser instance
        pytest_custom_opts = pytest_parser._anonymous
        for opt in pytest_custom_opts.options:
            parser.add_argument(opt._long_opts[0], action=opt._attrs['action'], default=opt._attrs['default'], help=opt._attrs['help'])

        args = parser.parse_args()

        nose_options = args['--nose-options'] or ''
        nose_option_list = nose_options.split()
        test_list = args['TESTS']
        nose_argv = nose_option_list + test_list

        self.verbosity_level = 1  # default verbosity level
        if args['--runner-debug']:
            self.verbosity_level = 2
        if args['--runner-quiet']:  # --debug and --quiet are mutually exclusive, enforced by docopt
            self.verbosity_level = 0

        # Get dictionaries corresponding to each point in the configuration matrix
        # we want to run, then generate a config object for each of them.
        self.log_debug('Generating configurations from the following matrix:\n\t{}'.format(args))

        all_configs = []
        results = []
        for config in all_configs:
            self.log_info('Running dtests with config object {}'.format(config))

            # Generate a file that runs nose, passing in config as the
            # configuration object.
            #
            # Yes, this is icky. The reason we do it is because we're dealing with
            # global configuration. We've decided global, nosetests-run-level
            # configuration is the way to go. This means we don't want to call
            # nose.main() multiple times in the same Python interpreter -- I have
            # not yet found a way to re-execute modules (thus getting new
            # module-level configuration) for each call. This didn't even work for
            # me with exec(script, {}, {}). So, here we are.
            #
            # How do we execute code in a new interpreter each time? Generate the
            # code as text, then shell out to a new interpreter.
            to_execute = (
                    "import nose\n" +
                    "from plugins.dtestconfig import DtestConfigPlugin, GlobalConfigObject\n" +
                    "from plugins.dtestxunit import DTestXunit\n" +
                    "from plugins.dtesttag import DTestTag\n" +
                    "from plugins.dtestcollect import DTestCollect\n" +
                    "import sys\n" +
                    "print sys.getrecursionlimit()\n" +
                    "print sys.setrecursionlimit(8000)\n" +
                    (
                    "nose.main(addplugins=[DtestConfigPlugin({config}), DTestXunit(), DTestCollect(), DTestTag()])\n" if "TEST_TAG" in environ else "nose.main(addplugins=[DtestConfigPlugin({config}), DTestCollect(), DTestXunit()])\n")
            ).format(config=repr(config))
            temp = NamedTemporaryFile(dir=getcwd())
            self.log_debug('Writing the following to {}:'.format(temp.name))

            self.log_debug('```\n{to_execute}```\n'.format(to_execute=to_execute))
            temp.write(to_execute)
            temp.flush()

            # We pass nose_argv as options to the python call to maintain
            # compatibility with the nosetests command. Arguments passed in via the
            # command line are treated one way, args passed in as
            # nose.main(argv=...) are treated another. Compare with the options
            # -xsv for an example.
            cmd_list = [sys.executable, temp.name] + nose_argv
            self.log_debug('subprocess.call-ing {cmd_list}'.format(cmd_list=cmd_list))

            if args['--dry-run']:
                print('Would run the following command:\n\t{}'.format(cmd_list))
                with open(temp.name, 'r') as f:
                    contents = f.read()
                print('{temp_name} contains:\n```\n{contents}```\n'.format(
                    temp_name=temp.name,
                    contents=contents
                ))
            else:
                results.append(subprocess.call(cmd_list, env=os.environ.copy()))
            # separate the end of the last subprocess.call output from the
            # beginning of the next by printing a newline.
            print()

        # If this answer:
        # http://stackoverflow.com/a/21788998/3408454
        # is to be believed, nosetests will exit with 0 on success, 1 on test or
        # other failure, and 2 on printing usage. We'll just grab the max of the
        # runs we saw -- if one printed usage, the whole run "printed usage", if
        # none printed usage, and one or more failed, we failed, else success.
        if not results:
            results = [0]
        exit(max(results))


if __name__ == '__main__':
    RunDTests().run(sys.argv[1:])
