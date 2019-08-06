# -*- coding: utf-8 -*-
"""


@author: jamin, zhiruiwang, aawiegel
"""

from __future__ import print_function
import pandas
import importlib
from tableausdk import *
from Pandleau import Pandleau
from tqdm import tqdm



def determine_extract_api_version():
    extract_version = None
    sdk_spec = importlib.util.find_spec("tableaudsk.Extract")

    try:
        from tableausdk.Extract import ExtractAPI
        from tableausdk.Extract import Extract
        extract_version = 1
    except ModuleNotFoundError:
        pass
    try:
        from tableausdk.HyperExtract import ExtractAPI
        from tableausdk.HyperExtract import Extract
        extract_version = 2
    except ModuleNotFoundError:
        pass

    if extract_version & extract_version == 1:
        print("You are using the Tableau SDK, please save the output as .tde format")
    elif extract_version & extract_version == 2:
        print("You are using the Extract API 2.0, please save the output as .hyper format")
    else:
        raise ModuleNotFoundError


determine_extract_api_version()


class PandleauTable(object):
    def __init__(self, name=None, dataframe=None):
        self._name = name
        self._dataframe = None
        self._column_names = None
        self._column_static_types = None

        if dataframe:
            # use the dataframe.setter method to assign dataframe
            self.dataframe = dataframe

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def dataframe(self):
        return self._dataframe

    @dataframe.setter
    def dataframe(self, value):
        if isinstance(value, pandas.DataFrame):
            self._dataframe = value
            self._column_names = [self._dataframe.columns]
            self._column_static_type = self._dataframe.apply(lambda x: Pandleau.data_static_type(x), axis=0)
        else:
            raise Exception('Error: PandleauTable.dataframe must be a pandas DataFrame.')

    def set_spatial(self, column_index, indicator=True):
        """
        Allows the user to define a spatial column
        @param column_index = index of spatial column,
                             either number or name
        @param indicator = change spatial characteristic

        """

        if indicator:
            if column_index.__class__.__name__ == 'int':
                self._column_static_type[column_index] = Type.SPATIAL
            elif column_index.__class__.__name__ == 'str':
                self._column_static_type[self._column_names.index(column_index)] = Type.SPATIAL
            else:
                raise Exception('Error: could not find column in dataframe.')
        else:
            if column_index.__class__.__name__ == 'int':
                self._column_static_type[column_index] = Pandleau.data_static_type(
                    self._dataframe.iloc[:, column_index])
            elif column_index.__class__.__name__ == 'str':
                self._column_static_type[self._column_names.index(column_index)] = Pandleau.data_static_type(
                    self._dataframe.loc[:, column_index])
            else:
                raise Exception('Error: could not find column in dataframe.')

    def set_table_structure(self, new_extract, table_name, add_index=False):
        """
        Checks if the provided table name exists in the extract, and if not, creates it.
        @param new_extract = extract that's been created
        @param table_name = name of the table in the extract
        @param add_index = adds incrementing integer index before dataframe columns
        """

        table_def = TableDefinition()
        # Set columns in Tableau
        if add_index:
            index_col = 'index'
            suffix = 1
            while index_col in self._dataframe.columns:
                index_col = '%s_%s' % (index_col, suffix)
                suffix += 1
            table_def.addColumn(index_col, Type.INTEGER)
        for col_index, col_name in enumerate(self._dataframe):
            table_def.addColumn(col_name, self._column_static_type[col_index])
        # Create table
        new_extract.addTable(table_name, table_def)

    def set_column_values(self, tableau_table, extract_table, add_index):
        """
        Translates pandas datatypes to tableau datatypes
        """
        # Create new row
        new_row = Row(extract_table)
        cols_range = range(len(self._dataframe.columns))
        column_set_functions = [
            Pandleau.entry_writer.get(col_type, lambda row, entry_index, _: row.setNull(entry_index))
            for col_type in
            self._column_static_type]
        i = 0

        for row_index in tqdm(self._dataframe.itertuples(index=False), desc='processing table'):

            if add_index:
                new_row.setInteger(0, i)
                i += 1

            for col_index in cols_range:
                col_index_corrected = col_index+add_index
                col_entry = row_index[col_index]
                column_set_function = column_set_functions[col_index]

                try:
                    if pandas.isnull(col_entry):
                        new_row.setNull(col_index_corrected)
                    else:
                        column_set_function(new_row, col_index_corrected, col_entry)
                except:
                    new_row.setNull(col_index_corrected)

            tableau_table.insert(new_row)

    def to_tableau(self, path, add_index=False):
        """
        Converts a Pandas DataFrame to a Tableau .tde file
        @param path = path to write file
        @param add_index = adds incrementing integer index before dataframe columns

        """

        # Delete debug log if already exists
        for file in [os.path.dirname(path) + '/debug.log',
                     './DataExtract.log', './debug.log']:
            if os.path.isfile(file):
                os.remove(file)

        # Create Extract and Table
        ExtractAPI.initialize()
        new_extract = Extract(path)

        if not new_extract.hasTable(self.name):
            print('Table \'{}\' does not exist in extract {}, creating.'.format(self.name, path))
            self.set_table_structure(new_extract, self.name, add_index)

        new_table = new_extract.openTable(self.name)
        table_def = new_table.getTableDefinition()

        # Set Column values
        self.set_column_values(new_table, table_def, add_index)

        # Close extract
        new_extract.close()
        ExtractAPI.cleanup()

