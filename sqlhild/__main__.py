#!/usr/bin/env python
"""sqlhild.

Usage:
  sqlhild [-q -a -v --csv -m=<MODULES> -c=<CONFIG> --sqlite -O=<level>] <query>
  sqlhild [-q -a -v --csv -m=<MODULES> -c=<CONFIG> -O=<level>] --file=<sqlfile>
  sqlhild --server HOST [-a -v -m=<MODULES> --show-ra]
  sqlhild --help
  sqlhild --version

Options:
  -f --file=<sqlfile>        SQL file to parse.
  --csv                      Output CSV.
  -q --queryplan             Output queryplan.
  -a --dumpast               Output AST.
  -m --modules=<MODULES>     Import these modules.
  -O=<level>                 Optimization level [default: 5].
  -c --config=<CONFIG>       Load config.
  -s --server HOST           Run as a server.
  -l --log-level=<LOGLEVEL>  Set default log level.
  -v --verbose               Debug mode.
  -h --help                  Show this screen.
  --version                  Show version.
"""

# TODO: add --tsv (tab separation)
# TODO: add error if graphviz not installed and --queryplan used

import addict
import docopt
import importlib
import logging
import logging.config
import os
import sys
import yaml

from .query import go
from . import logger


def load_modules(modules):
    """
    The user can specify the modules sqlhild needs to load before any
    processing is done.
    """
    for module_name in modules.split():
        # TODO: detect if it's a module name or pat
        spec = importlib.util.spec_from_file_location("x", module_name)
        if spec:
            foo = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(foo)
        else:
            importlib.import_module(modules)


def load_config(config_filename):
    config = yaml.load(open(config_filename, 'r').read())
    try:
        logging.config.dictConfig(config['logging'])
    except KeyError:
        logger.addHandler(logging.StreamHandler(sys.stdout))


def main():
    try:
        args = docopt.docopt(__doc__, version='sqlhild 0.1')
    except docopt.DocoptExit:
        # TODO: add warning? shell's globbing on "*" might mess things up?
        # Try to an "echo" style command line without quotes
        # eg. sqlhild select * from Table
        args = addict.Dict({'<query>': ' '.join(sys.argv[1:])})

    if args['--config']:
        load_config(args['--config'])
    else:
        try:
            load_config('sqlhild.yaml')
        except FileNotFoundError:
            from . import logger
            logger.addHandler(logging.StreamHandler(sys.stderr))
            logger.setLevel(level=logging.INFO)

    logger = logging.getLogger('root')

    if args['--verbose']:
        logger.setLevel(level=logging.DEBUG)

    if args['--modules']:
        load_modules(args['--modules'])

    if args['--server']:
        from sqlhild.postgres.server import start_server
        start_server(args['--server'])
        exit()

    if args['--file']:
        sql_text = open(args['--file'], 'r').read()
    else:
        sql_text = args['<query>']

    if args['-O']:
        os.environ['SQLHILD_OPTIMIZATION_LEVEL'] = args['-O']

    go(
        sql_text,
        pretty_print=True,
        queryplan=args['--queryplan'],
        dumpast=args['--dumpast'],
        output_csv=args['--csv'],
        sqlite_run=args['--sqlite'],
    )


if __name__ == '__main__':
    main()
