# -*- coding: utf-8 -*-
"""


@author: jamin, zhiruiwang, aawiegel
"""

from __future__ import print_function
from Pandleau.PandleauTable import PandleauTable
from tableausdk.HyperExtract import Extract
from tableausdk.HyperExtract import ExtractAPI
from tableausdk import *
import config
logger = config.logger


class Pandleau(object):
    def __init__(self, name, *args, tableau_logging=False):
        """
        object to publish can be Single-Table or Multi-table Tableau Hyper extracts.
        :param name:
        :param args:
        """
        if tableau_logging:
            ExtractAPI.initialize()
        self._pandleau_tables = []  # just a list of PandleauTable objects
        self._name = name
        self._hyper_output_path = config.TABLEAU_HYPER_OUTPUT_DIR + self._name + '.hyper'
        self._extract = None
        for pandleau_table in args:
            self.add_table(pandleau_table)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        self._hyper_output_path = config.TABLEAU_HYPER_OUTPUT_DIR + self._name + '.hyper'

    def add_table(self, table):
        if not isinstance(table, PandleauTable):
            raise Exception('Error: argument to Pandleau must be a PandleauTable.')
        self._pandleau_tables.append(table)

    def validate_state(self):
        if len(self._pandleau_tables) == 0:
            raise ValueError("can't create an extract without any PandleauTables")
        elif len(self._pandleau_tables) == 1:
            logger.info("publishing single-table hyper extract")
        else:
            logger.info("publishing multi-table hyper extract")

    def to_hyper(self):
        """
        Converts a Pandas DataFrame to a Tableau .tde file
        @param path = path to write file
        @param table_name = name of the table in the extract
        @param add_index = adds incrementing integer index before dataframe columns

        """
        self.validate_state()

        # Delete debug log if already exists
        for file in [os.path.dirname(self._hyper_output_path) + '/debug.log',
                     './DataExtract.log', './debug.log']:
            if os.path.isfile(file):
                os.remove(file)

        self._extract = Extract(self._hyper_output_path)

        n = len(self._pandleau_tables)
        for pt in self._pandleau_tables:
            logger.info(f"processing PandleauTable {pt.name}")
            if not self._extract.hasTable(pt.name):
                logger.info(f"Table '{pt.name}' does not exist in extract {self._hyper_output_path}, creating it.")
                self._extract.addTable(pt.name, pt.extract_schema_definition)
            logger.info("populating extract...")
            extract_table = self._extract.openTable(pt.name)
            pt.populate_extract(extract_table=extract_table)

        self._extract.close()
        ExtractAPI.cleanup()
