# -*- coding: utf-8 -*-
"""
@author: jamin, zhiruiwang, aawiegel
"""

from __future__ import print_function
import config
import pandas
from tableausdk.HyperExtract import Extract
from tableausdk.HyperExtract import ExtractAPI
from tableausdk.HyperExtract import Table
from tableausdk.HyperExtract import Row
from tableausdk.HyperExtract import TableDefinition
from tableausdk import *
from Pandleau import TypeConversions
from tqdm import tqdm

logger = config.logger


class PandleauTable(object):
    def __init__(self, dataframe, name=None, add_index=False, tableau_logging=False):
        """
        Modification to the a Pandas dataframe object
        :param dataframe: pandas Dataframe
        :param name: Table name for this Extract table.
        :param add_index: Add an integer index column?
        :param tableau_logging: enable tableausdk logging
        """
        if tableau_logging:
            ExtractAPI.initialize()
        self._add_index = add_index
        self._added_index = False
        self._column_names = None
        self._column_static_types = None
        self._column_conversion_functions = []
        self._df = None
        self._hyper_output_path = None
        self._name = None
        self._extract = None
        self._extract_schema_definition = None
        # use the property setter logic
        self.df = dataframe
        self.name = name

    def init_df(self):
        if not self._name:
            r, c = self._df.shape
            self.name = f'pandleau_{r}rows_x_{c}columns'
        if self._add_index and not self._added_index:
            self.add_index_column()
            return False
        else:
            self._column_names = [self._df.columns]
            self._column_static_types = self._df.apply(lambda x: TypeConversions.data_static_type(x), axis=0)
            self.set_extract_schema_definition()
            self.set_conversion_functions()
            return True

    @property
    def df(self):
        return self._df

    @df.setter
    def df(self, df):
        """
        Assign a dataframe for this PandleauTable.

        If dataframe changes, we need to run PandleauTable.setup() again.r
        :param df:
        :return:
        """
        if not isinstance(df, pandas.DataFrame):
            logger.error("PandleauTable.dataframe must be a pandas DataFrame.")
            raise TypeError("PandleauTable.dataframe must be a pandas DataFrame.")
        self._df = df
        self.init_df()

    @property
    def extract_schema_definition(self):
        return self._extract_schema_definition

    def set_extract_schema_definition(self):
        """
        create a schema for the Tableau Extract table
        :return:
        """
        table_definition = TableDefinition()

        # add column definitions for dataframe columns
        for col_index, col_name in enumerate(self._df):
            table_definition.addColumn(col_name, self._column_static_types[col_index])
        # Create table
        self._extract_schema_definition = table_definition

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value: str):
        if not isinstance(value, str):
            raise TypeError("PandleauTable name must be a str")
        if not value:
            r, c = self._df.shape
            self.name = f'pandleau_{r}rows_x_{c}columns'
        else:
            self._name = value
        self._hyper_output_path = config.PANDLEAU_HOME + self._name + '.hyper'

    def add_index_column(self):
        """
        Add an integer index column to our dataframe as the left-most column
        :return:
        """
        if 'index' in self._df.columns:
            raise ValueError("The supplied dataframe already contains a column named 'index'")
        old_columns = self._df.columns.to_list()
        # add index column
        self._df['index'] = self._df.index
        # shift 'index' column to left-most position
        new_columns = ['index']
        new_columns.extend(old_columns)
        self._added_index = True
        self.df = self._df[new_columns]

    def populate_extract(self, extract_table=None):
        """
        Populate Extract
        :param extract_table: provide an extract table to use, otherwise a default is created.
        :return:
        """

        if not extract_table:
            logger.debug(f"{self.name}: Using default extract...")
            extract_table = self._extract.openTable(self._name)
        else:
            assert isinstance(extract_table, Table)
            logger.debug(f"PandleauTable '{self.name}' using {extract_table} extract...")
        for df_row in tqdm(self._df.itertuples(index=False), desc='processing table...'):
            tableau_row = Row(extract_table.getTableDefinition())
            for col_idx, col_name in enumerate(self._df.columns.to_list()):
                try:
                    if pandas.isnull(col_name):
                        tableau_row.setNull(col_idx)
                    else:
                        self._column_conversion_functions[col_idx](tableau_row, col_idx, df_row[col_idx])
                except Exception:
                    tableau_row.setNull(col_idx)
            extract_table.insert(tableau_row)

    def set_conversion_functions(self):
        """
        create an (ordered) list of lambda functions, corresponding to self._df.columns.
        These lambda functions will translate pandas types to Tableau types.
        :return:
        """
        column_conversion_functions = []
        default_function = lambda row, entry_index, _: row.setNull(entry_index)
        for col_type in self._column_static_types:
            fn = TypeConversions.entry_writer.get(col_type, default_function)
            column_conversion_functions.append(fn)
        self._column_conversion_functions = column_conversion_functions

    def set_spatial(self, column_index, indicator=True):
        """
        Allows the user to define a spatial column
        @param column_index = index of spatial column,
                             either number or name
        @param indicator = change spatial characteristic

        """

        if indicator:
            if column_index.__class__.__name__ == 'int':
                self._column_static_types[column_index] = Type.SPATIAL
            elif column_index.__class__.__name__ == 'str':
                self._column_static_types[self._column_names.index(column_index)] = Type.SPATIAL
            else:
                logger.error('could not find column in dataframe.')
                raise LookupError('Error: could not find column in dataframe.')
        else:
            if column_index.__class__.__name__ == 'int':
                self._column_static_types[column_index] = \
                    TypeConversions.data_static_type(self._df.iloc[:, column_index])
            elif column_index.__class__.__name__ == 'str':
                self._column_static_types[self._column_names.index(column_index)] = \
                    TypeConversions.data_static_type(self._df.loc[:, column_index])
            else:
                logger.error('Error: could not find column in dataframe.')
                raise LookupError('Error: could not find column in dataframe.')

    def set_extract_schema(self, new_extract, table_name):
        """
        Checks if the provided table name exists in the extract, and if not, creates it.
        @param new_extract = extract that's been created
        @param table_name = name of the table in the extract        @param add_index = adds incrementing integer index before dataframe columns
        """

        table_def = TableDefinition()
        # Set columns in Tableau
        for col_index, col_name in enumerate(self._df):
            table_def.addColumn(col_name, self._column_static_types[col_index])
        # Create table
        new_extract.addTable(table_name, table_def)

    def to_hyper(self):
        """
        Publish this as a single-table Tableau Hyper extract

        """

        # Delete debug log if already exists
        for file in [os.path.dirname(self._hyper_output_path) + '/debug.log',
                     './DataExtract.log', './debug.log']:
            if os.path.isfile(file):
                os.remove(file)

        self._extract = Extract(self._hyper_output_path)

        if not self._extract.hasTable(self._name):
            logger.info(f"Table '{self._name}' does not exist in extract {self._hyper_output_path}, creating it.")
            self._extract.addTable(self._name, self.extract_schema_definition)

        # Set Column values
        self.populate_extract()

        # Close extract
        self._extract.close()
        ExtractAPI.cleanup()

