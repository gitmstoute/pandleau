import pandas
from tableausdk import Type
mapper = {'string': Type.UNICODE_STRING,
          'bytes': Type.BOOLEAN,
          'floating': Type.DOUBLE,
          'integer': Type.INTEGER,
          'mixed-integer': Type.DOUBLE,  # integers with non-integers
          'mixed-integer-float': Type.DOUBLE,  # floats and integers
          'decimal': Type.DOUBLE,
          'complex': Type.UNICODE_STRING,  # No complex set type
          'categorical': Type.CHAR_STRING,
          'boolean': Type.BOOLEAN,
          'datetime64': Type.DATETIME,
          'datetime': Type.DATETIME,
          'date': Type.DATE,
          'timedelta64': Type.DATETIME,
          'timedelta': Type.DATETIME,
          'time': Type.DATETIME,
          'period': Type.DURATION,
          'mixed': Type.UNICODE_STRING}

entry_writer = {Type.SPATIAL: lambda row, entry_index, entry: row.setSpatial(entry_index, entry.encode('utf-8')),
                Type.UNICODE_STRING: lambda row, entry_index, entry: row.setString(entry_index, str(entry)),
                Type.BOOLEAN: lambda row, entry_index, entry: row.setBoolean(entry_index, entry),
                Type.DOUBLE: lambda row, entry_index, entry: row.setDouble(entry_index, entry),
                Type.INTEGER: lambda row, entry_index, entry: row.setInteger(entry_index, int(entry)),
                Type.CHAR_STRING: lambda row, entry_index, entry: row.setCharString(entry_index, str(entry)),
                Type.DATETIME: lambda row, entry_index, entry: row.setDateTime(entry_index, entry.year,
                                                                               entry.month, entry.day, entry.hour,
                                                                               entry.minute, entry.second,
                                                                               entry.microsecond),
                Type.DATE: lambda row, entry_index, entry: row.setDate(entry_index, entry.year, entry.month,
                                                                       entry.day),
                Type.DURATION: lambda row, entry_index, entry: row.setDuration(entry_index, entry.day, entry.hour,
                                                                               entry.minute,
                                                                               entry.second, entry.microsecond)}


def data_static_type(column):
    """
        Translates pandas datatypes to static datatypes (for initial columns)
        @param column is dataframe column
        """

    # Use pandas api for inferring types for latest versions of pandas, lib method for earlier versions
    if pandas.__version__ >= '0.21.0':
        return mapper[pandas.api.types.infer_dtype(column.dropna())]
    else:
        return mapper[pandas.lib.infer_dtype(column.dropna())]
