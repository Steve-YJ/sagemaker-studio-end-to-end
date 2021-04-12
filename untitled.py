from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
#  You may want to configure the Spark Context with the right credentials provider.
spark = SparkSession.builder.master('local').getOrCreate()

mode = None

def capture_stdout(func, *args, **kwargs):
    """Capture standard output to a string buffer"""

    from contextlib import redirect_stdout
    import io

    stdout_string = io.StringIO()
    with redirect_stdout(stdout_string):
        func(*args, **kwargs)
    return stdout_string.getvalue()


def convert_or_coerce(pandas_df, spark):
    """Convert pandas df to pyspark df and coerces the mixed cols to string"""
    import re

    try:
        return spark.createDataFrame(pandas_df)
    except TypeError as e:
        match = re.search(r".*field (\w+).*Can not merge type.*", str(e))
        if match is None:
            raise e
        mixed_col_name = match.group(1)
        # Coercing the col to string
        pandas_df[mixed_col_name] = pandas_df[mixed_col_name].astype("str")
        return pandas_df


def default_spark(value):
    return {"default": value}


def default_spark_with_stdout(df, stdout):
    return {
        "default": df,
        "stdout": stdout,
    }


def default_spark_with_trained_parameters(value, trained_parameters):
    return {"default": value, "trained_parameters": trained_parameters}


def default_spark_with_trained_parameters_and_state(df, trained_parameters, state):
    return {"default": df, "trained_parameters": trained_parameters, "state": state}


def dispatch(key_name, args, kwargs, funcs):
    """
    Dispatches to another operator based on a key in the passed parameters.
    This also slices out any parameters using the parameter_name passed in,
    and will reassemble the trained_parameters correctly after invocation.

    Args:
        key_name: name of the key in kwargs used to identify the function to use.
        args: dataframe that will be passed as the first set of parameters to the function.
        kwargs: keyword arguments that key_name will be found in; also where args will be passed to parameters.
                These are also expected to include trained_parameters if there are any.
        funcs: dictionary mapping from value of key_name to (function, parameter_name)

    """
    if key_name not in kwargs:
        raise OperatorCustomerError(f"Missing required parameter {key_name}")

    operator = kwargs[key_name]

    if operator not in funcs:
        raise OperatorCustomerError(f"Invalid choice selected for {key_name}. {operator} is not supported.")

    func, parameter_name = funcs[operator]

    # Extract out the parameters that should be available.
    func_params = kwargs.get(parameter_name, {})
    if func_params is None:
        func_params = {}

    # Extract out any trained parameters.
    specific_trained_parameters = None
    if "trained_parameters" in kwargs:
        trained_parameters = kwargs["trained_parameters"]
        if trained_parameters is not None and parameter_name in trained_parameters:
            specific_trained_parameters = trained_parameters[parameter_name]
    func_params["trained_parameters"] = specific_trained_parameters

    # Execute the function (should return a dict).
    result = func(*args, **func_params)

    # Check if the result contains any trained parameters and remap them to the proper structure.
    if result is not None and "trained_parameters" in result:
        existing_trained_parameters = kwargs.get("trained_parameters")
        updated_trained_parameters = result["trained_parameters"]

        if existing_trained_parameters is not None or updated_trained_parameters is not None:
            existing_trained_parameters = existing_trained_parameters if existing_trained_parameters is not None else {}
            existing_trained_parameters[parameter_name] = result["trained_parameters"]

            # Update the result trained_parameters so they are part of the original structure.
            result["trained_parameters"] = existing_trained_parameters
        else:
            # If the given trained parameters were None and the returned trained parameters were None, don't return anything.
            del result["trained_parameters"]

    return result


def get_dataframe_with_sequence_ids(df: DataFrame):
    df_cols = df.columns
    rdd_with_seq = df.rdd.zipWithIndex()
    df_with_seq = rdd_with_seq.toDF()
    df_with_seq = df_with_seq.withColumnRenamed("_2", "_seq_id_")
    for col_name in df_cols:
        df_with_seq = df_with_seq.withColumn(col_name, df_with_seq["_1"].getItem(col_name))
    df_with_seq = df_with_seq.drop("_1")
    return df_with_seq


def get_execution_state(status: str, message=None):
    return {"status": status, "message": message}


class OperatorCustomerError(Exception):
    """Error type for Customer Errors in Spark Operators"""




def encode_categorical_ordinal_encode(
    df, input_column=None, output_column=None, invalid_handling_strategy=None, trained_parameters=None
):
    INVALID_HANDLING_STRATEGY_SKIP = "Skip"
    INVALID_HANDLING_STRATEGY_ERROR = "Error"
    INVALID_HANDLING_STRATEGY_KEEP = "Keep"
    INVALID_HANDLING_STRATEGY_REPLACE_WITH_NAN = "Replace with NaN"

    from pyspark.ml.feature import StringIndexer, StringIndexerModel
    from pyspark.sql.functions import when

    expects_column(df, input_column, "Input column")

    invalid_handling_map = {
        INVALID_HANDLING_STRATEGY_SKIP: "skip",
        INVALID_HANDLING_STRATEGY_ERROR: "error",
        INVALID_HANDLING_STRATEGY_KEEP: "keep",
        INVALID_HANDLING_STRATEGY_REPLACE_WITH_NAN: "keep",
    }

    output_column, output_is_temp = get_temp_col_if_not_set(df, output_column)

    # process inputs
    handle_invalid = (
        invalid_handling_strategy
        if invalid_handling_strategy in invalid_handling_map
        else INVALID_HANDLING_STRATEGY_ERROR
    )

    trained_parameters = load_trained_parameters(
        trained_parameters, {"invalid_handling_strategy": invalid_handling_strategy}
    )

    input_handle_invalid = invalid_handling_map.get(handle_invalid)
    index_model, index_model_loaded = load_pyspark_model_from_trained_parameters(
        trained_parameters, StringIndexerModel, "string_indexer_model"
    )

    if index_model is None:
        indexer = StringIndexer(inputCol=input_column, outputCol=output_column, handleInvalid=input_handle_invalid)
        # fit the model and transform
        try:
            index_model = fit_and_save_model(trained_parameters, "string_indexer_model", indexer, df)
        except Exception as e:
            if input_handle_invalid == "error":
                raise OperatorSparkOperatorCustomerError(
                    f"Encountered error calculating string indexes. Halting because error handling is set to 'Error'. Please check your data and try again: {e}"
                )
            else:
                raise e

    output_df = transform_using_trained_model(index_model, df, index_model_loaded)

    # finally, if missing should be nan, convert them
    if handle_invalid == INVALID_HANDLING_STRATEGY_REPLACE_WITH_NAN:
        new_val = float("nan")
        # convert all numLabels indices to new_val
        num_labels = len(index_model.labels)
        output_df = output_df.withColumn(
            output_column, when(output_df[output_column] == num_labels, new_val).otherwise(output_df[output_column])
        )

    # finally handle the output column name appropriately.
    output_df = replace_input_if_output_is_temp(output_df, input_column, output_column, output_is_temp)

    return default_spark_with_trained_parameters(output_df, trained_parameters)


def encode_categorical_one_hot_encode(
    df,
    input_column=None,
    input_already_ordinal_encoded=None,
    invalid_handling_strategy=None,
    drop_last=None,
    output_style=None,
    output_column=None,
    trained_parameters=None,
):

    INVALID_HANDLING_STRATEGY_SKIP = "Skip"
    INVALID_HANDLING_STRATEGY_ERROR = "Error"
    INVALID_HANDLING_STRATEGY_KEEP = "Keep"

    OUTPUT_STYLE_VECTOR = "Vector"
    OUTPUT_STYLE_COLUMNS = "Columns"

    invalid_handling_map = {
        INVALID_HANDLING_STRATEGY_SKIP: "skip",
        INVALID_HANDLING_STRATEGY_ERROR: "error",
        INVALID_HANDLING_STRATEGY_KEEP: "keep",
    }

    handle_invalid = invalid_handling_map.get(invalid_handling_strategy, "error")
    expects_column(df, input_column, "Input column")
    output_format = output_style if output_style in [OUTPUT_STYLE_VECTOR, OUTPUT_STYLE_COLUMNS] else OUTPUT_STYLE_VECTOR
    drop_last = parse_parameter(bool, drop_last, "Drop Last", True)
    input_ordinal_encoded = parse_parameter(bool, input_already_ordinal_encoded, "Input already ordinal encoded", False)

    output_column = output_column if output_column else input_column

    trained_parameters = load_trained_parameters(
        trained_parameters, {"invalid_handling_strategy": invalid_handling_strategy, "drop_last": drop_last}
    )

    from pyspark.ml.feature import (
        StringIndexer,
        StringIndexerModel,
        OneHotEncoder,
        OneHotEncoderModel,
    )
    from pyspark.ml.functions import vector_to_array
    import pyspark.sql.functions as sf
    from pyspark.sql.types import DoubleType

    # first step, ordinal encoding. Not required if input_ordinal_encoded==True
    # get temp name for ordinal encoding
    ordinal_name = temp_col_name(df, output_column)
    if input_ordinal_encoded:
        df_ordinal = df.withColumn(ordinal_name, df[input_column].cast("int"))
        labels = None
    else:
        index_model, index_model_loaded = load_pyspark_model_from_trained_parameters(
            trained_parameters, StringIndexerModel, "string_indexer_model"
        )
        if index_model is None:
            # apply ordinal encoding
            indexer = StringIndexer(inputCol=input_column, outputCol=ordinal_name, handleInvalid=handle_invalid)
            try:
                index_model = fit_and_save_model(trained_parameters, "string_indexer_model", indexer, df)
            except Exception as e:
                if handle_invalid == "error":
                    raise OperatorSparkOperatorCustomerError(
                        f"Encountered error calculating string indexes. Halting because error handling is set to 'Error'. Please check your data and try again: {e}"
                    )
                else:
                    raise e

        try:
            df_ordinal = transform_using_trained_model(index_model, df, index_model_loaded)
        except Exception as e:
            if handle_invalid == "error":
                raise OperatorSparkOperatorCustomerError(
                    f"Encountered error transforming string indexes. Halting because error handling is set to 'Error'. Please check your data and try again: {e}"
                )
            else:
                raise e

        labels = index_model.labels

    # drop the input column if required from the ordinal encoded dataset
    if output_column == input_column:
        df_ordinal = df_ordinal.drop(input_column)

    temp_output_col = temp_col_name(df_ordinal, output_column)

    # apply onehot encoding on the ordinal
    cur_handle_invalid = handle_invalid if input_ordinal_encoded else "error"
    cur_handle_invalid = "keep" if cur_handle_invalid == "skip" else cur_handle_invalid

    ohe_model, ohe_model_loaded = load_pyspark_model_from_trained_parameters(
        trained_parameters, OneHotEncoderModel, "one_hot_encoder_model"
    )
    if ohe_model is None:
        ohe = OneHotEncoder(
            dropLast=drop_last, handleInvalid=cur_handle_invalid, inputCol=ordinal_name, outputCol=temp_output_col
        )
        try:
            ohe_model = fit_and_save_model(trained_parameters, "one_hot_encoder_model", ohe, df_ordinal)
        except Exception as e:
            if handle_invalid == "error":
                raise OperatorSparkOperatorCustomerError(
                    f"Encountered error calculating encoding categories. Halting because error handling is set to 'Error'. Please check your data and try again: {e}"
                )
            else:
                raise e

    output_df = transform_using_trained_model(ohe_model, df_ordinal, ohe_model_loaded)

    if output_format == OUTPUT_STYLE_COLUMNS:
        if labels is None:
            labels = list(range(ohe_model.categorySizes[0]))

        current_output_cols = set(list(output_df.columns))
        old_cols = [sf.col(name) for name in df.columns if name in current_output_cols]
        arr_col = vector_to_array(output_df[temp_output_col])
        new_cols = [(arr_col[i]).alias(f"{output_column}_{name}") for i, name in enumerate(labels)]
        output_df = output_df.select(*(old_cols + new_cols))
    else:
        # remove the temporary ordinal encoding
        output_df = output_df.drop(ordinal_name)
        output_df = output_df.withColumn(output_column, sf.col(temp_output_col))
        output_df = output_df.drop(temp_output_col)
        final_ordering = [col for col in df.columns]
        if output_column not in final_ordering:
            final_ordering.append(output_column)
        output_df = output_df.select(final_ordering)

    return default_spark_with_trained_parameters(output_df, trained_parameters)


import re
from pyspark.sql import functions as sf, types


def format_string_lower_case(df, input_column=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column, sf.lower(df[input_column].cast(types.StringType()))
        )
    )


def format_string_upper_case(df, input_column=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column, sf.upper(df[input_column].cast(types.StringType()))
        )
    )


def format_string_title_case(df, input_column=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.title(), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_capitalize(df, input_column=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.capitalize(), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_swap_case(df, input_column=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.swapcase(), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_left_pad(
    df, input_column=None, width=None, fill_character=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    width = parse_parameter(int, width, "Width")
    fill_character = parse_parameter(str, fill_character, "Fill character", " ")

    MAX_WIDTH = 1000
    if width > MAX_WIDTH:
        raise OperatorSparkOperatorCustomerError(f"Width must be less than {MAX_WIDTH}. Received: {width}")

    if len(fill_character) > 1:
        raise OperatorSparkOperatorCustomerError(f"Fill character can only be a single character. Received: {fill_character}")

    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.lpad(df[input_column].cast(types.StringType()), len=width, pad=fill_character),
        )
    )


def format_string_right_pad(
    df, input_column=None, width=None, fill_character=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    width = parse_parameter(int, width, "Width")
    fill_character = parse_parameter(str, fill_character, "Fill character", " ")

    MAX_WIDTH = 1000
    if width > MAX_WIDTH:
        raise OperatorSparkOperatorCustomerError(f"Width must be less than {MAX_WIDTH}. Received: {width}")

    if len(fill_character) > 1:
        raise OperatorSparkOperatorCustomerError(f"Fill character can only be a single character. Received: {fill_character}")

    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.rpad(df[input_column].cast(types.StringType()), len=width, pad=fill_character),
        )
    )


def format_string_center_pad_on_either_side(
    df, input_column=None, width=None, fill_character=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    width = parse_parameter(int, width, "Width")
    fill_character = parse_parameter(str, fill_character, "Fill character", " ")

    MAX_WIDTH = 1000
    if width > MAX_WIDTH:
        raise OperatorSparkOperatorCustomerError(f"Width must be less than {MAX_WIDTH}. Received: {width}")

    if len(fill_character) > 1:
        raise OperatorSparkOperatorCustomerError(f"Fill character can only be a single character. Received: {fill_character}")

    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.center(width=width, fillchar=fill_character), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_strip_characters_from_left(
    df, input_column=None, characters_to_remove=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.lstrip(to_strip=characters_to_remove), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_strip_characters_from_right(
    df, input_column=None, characters_to_remove=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.rstrip(to_strip=characters_to_remove), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_strip_left_and_right(
    df, input_column=None, characters_to_remove=None, output_column=None, trained_parameters=None
):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.pandas_udf(lambda s: s.str.strip(to_strip=characters_to_remove), returnType=types.StringType())(
                df[input_column].cast(types.StringType())
            ),
        )
    )


def format_string_prepend_zeros(df, input_column=None, width=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    width = parse_parameter(int, width, "Width")

    MAX_WIDTH = 1000
    if width > MAX_WIDTH:
        raise OperatorSparkOperatorCustomerError(f"Width must be less than {MAX_WIDTH}. Received: {width}")

    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.lpad(df[input_column].cast(types.StringType()), len=width, pad="0"),
        )
    )


def format_string_add_prefix(df, input_column=None, prefix=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.concat(sf.lit(prefix), df[input_column].cast(types.StringType())),
        )
    )


def format_string_add_suffix(df, input_column=None, suffix=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.concat(df[input_column].cast(types.StringType()), sf.lit(suffix)),
        )
    )


def format_string_remove_symbols(df, input_column=None, symbols=None, output_column=None, trained_parameters=None):
    expects_column(df, input_column, "Input column")
    symbols = "!@#$%^&*()_+=-/\\`~{}|<>?" if not symbols else symbols
    regex = "|".join([re.escape(symbol) for symbol in symbols])
    return default_spark(
        df.withColumn(
            output_column if output_column else input_column,
            sf.regexp_replace(df[input_column].cast(types.StringType()), f"({regex})", ""),
        )
    )


from enum import Enum

from pyspark.sql.types import BooleanType, DateType, DoubleType, LongType, StringType
from pyspark.sql import functions as f


class NonCastableDataHandlingMethod(Enum):
    REPLACE_WITH_NULL = "replace_null"
    REPLACE_WITH_NULL_AND_PUT_NON_CASTABLE_DATA_IN_NEW_COLUMN = "replace_null_with_new_col"
    REPLACE_WITH_FIXED_VALUE = "replace_value"
    REPLACE_WITH_FIXED_VALUE_AND_PUT_NON_CASTABLE_DATA_IN_NEW_COLUMN = "replace_value_with_new_col"
    DROP_NON_CASTABLE_ROW = "drop"

    @staticmethod
    def get_names():
        return [item.name for item in NonCastableDataHandlingMethod]

    @staticmethod
    def get_values():
        return [item.value for item in NonCastableDataHandlingMethod]


class MohaveDataType(Enum):
    BOOL = "bool"
    DATE = "date"
    FLOAT = "float"
    LONG = "long"
    STRING = "string"
    OBJECT = "object"

    @staticmethod
    def get_names():
        return [item.name for item in MohaveDataType]

    @staticmethod
    def get_values():
        return [item.value for item in MohaveDataType]


PYTHON_TYPE_MAPPING = {
    MohaveDataType.BOOL: bool,
    MohaveDataType.DATE: str,
    MohaveDataType.FLOAT: float,
    MohaveDataType.LONG: int,
    MohaveDataType.STRING: str,
}

MOHAVE_TO_SPARK_TYPE_MAPPING = {
    MohaveDataType.BOOL: BooleanType,
    MohaveDataType.DATE: DateType,
    MohaveDataType.FLOAT: DoubleType,
    MohaveDataType.LONG: LongType,
    MohaveDataType.STRING: StringType,
}

SPARK_TYPE_MAPPING_TO_SQL_TYPE = {
    BooleanType: "BOOLEAN",
    LongType: "BIGINT",
    DoubleType: "DOUBLE",
    StringType: "STRING",
    DateType: "DATE",
}

SPARK_TO_MOHAVE_TYPE_MAPPING = {value: key for (key, value) in MOHAVE_TO_SPARK_TYPE_MAPPING.items()}


def cast_single_column_type_helper(df, column_name_to_cast, column_name_to_add, mohave_data_type, date_formatting):
    if mohave_data_type == MohaveDataType.DATE:
        df = df.withColumn(column_name_to_add, f.to_date(df[column_name_to_cast], date_formatting))
    else:
        df = df.withColumn(
            column_name_to_add, df[column_name_to_cast].cast(MOHAVE_TO_SPARK_TYPE_MAPPING[mohave_data_type]())
        )
    return df


def cast_single_column_type(
    df, column, mohave_data_type, invalid_data_handling_method, replace_value=None, date_formatting="dd-MM-yyyy"
):
    """Cast single column to a new type

    Args:
        df (DataFrame): spark dataframe
        column (Column): target column for type casting
        mohave_data_type (Enum): Enum MohaveDataType
        invalid_data_handling_method (Enum): Enum NonCastableDataHandlingMethod
        replace_value (str): value to replace for invalid data when "replace_value" is specified
        date_formatting (str): format for date. Default format is "dd-MM-yyyy"

    Returns:
        df (DataFrame): casted spark dataframe
    """
    cast_to_date = f.to_date(df[column], date_formatting)
    cast_to_non_date = df[column].cast(MOHAVE_TO_SPARK_TYPE_MAPPING[mohave_data_type]())
    non_castable_column = f"{column}_typecast_error"
    temp_column = "temp_column"

    if invalid_data_handling_method == NonCastableDataHandlingMethod.REPLACE_WITH_NULL:
        # Replace non-castable data to None in the same column. pyspark's default behaviour
        # Original dataframe
        # +---+------+
        # | id | txt |
        # +---+---+--+
        # | 1 | foo  |
        # | 2 | bar  |
        # | 3 | 1    |
        # +---+------+
        # cast txt column to long
        # +---+------+
        # | id | txt |
        # +---+------+
        # | 1 | None |
        # | 2 | None |
        # | 3 | 1    |
        # +---+------+
        return df.withColumn(column, cast_to_date if (mohave_data_type == MohaveDataType.DATE) else cast_to_non_date)
    if invalid_data_handling_method == NonCastableDataHandlingMethod.DROP_NON_CASTABLE_ROW:
        # Drop non-castable row
        # Original dataframe
        # +---+------+
        # | id | txt |
        # +---+---+--+
        # | 1 | foo  |
        # | 2 | bar  |
        # | 3 | 1    |
        # +---+------+
        # cast txt column to long, _ non-castable row
        # +---+----+
        # | id|txt |
        # +---+----+
        # |  3|  1 |
        # +---+----+
        df = df.withColumn(column, cast_to_date if (mohave_data_type == MohaveDataType.DATE) else cast_to_non_date)
        return df.where(df[column].isNotNull())

    if (
        invalid_data_handling_method
        == NonCastableDataHandlingMethod.REPLACE_WITH_NULL_AND_PUT_NON_CASTABLE_DATA_IN_NEW_COLUMN
    ):
        # Replace non-castable data to None in the same column and put non-castable data to a new column
        # Original dataframe
        # +---+------+
        # | id | txt |
        # +---+------+
        # | 1 | foo  |
        # | 2 | bar  |
        # | 3 | 1    |
        # +---+------+
        # cast txt column to long
        # +---+----+------------------+
        # | id|txt |txt_typecast_error|
        # +---+----+------------------+
        # |  1|None|      foo         |
        # |  2|None|      bar         |
        # |  3|  1 |                  |
        # +---+----+------------------+
        df = df.withColumn(temp_column, cast_to_date if (mohave_data_type == MohaveDataType.DATE) else cast_to_non_date)
        df = df.withColumn(non_castable_column, f.when(df[temp_column].isNotNull(), "").otherwise(df[column]),)
    elif invalid_data_handling_method == NonCastableDataHandlingMethod.REPLACE_WITH_FIXED_VALUE:
        # Replace non-castable data to a value in the same column
        # Original dataframe
        # +---+------+
        # | id | txt |
        # +---+------+
        # | 1 | foo  |
        # | 2 | bar  |
        # | 3 | 1    |
        # +---+------+
        # cast txt column to long, replace non-castable value to 0
        # +---+-----+
        # | id| txt |
        # +---+-----+
        # |  1|  0  |
        # |  2|  0  |
        # |  3|  1  |
        # +---+----+
        value = _validate_and_cast_value(value=replace_value, mohave_data_type=mohave_data_type)

        df = df.withColumn(temp_column, cast_to_date if (mohave_data_type == MohaveDataType.DATE) else cast_to_non_date)

        replace_date_value = f.when(df[temp_column].isNotNull(), df[temp_column]).otherwise(
            f.to_date(f.lit(value), date_formatting)
        )
        replace_non_date_value = f.when(df[temp_column].isNotNull(), df[temp_column]).otherwise(value)

        df = df.withColumn(
            temp_column, replace_date_value if (mohave_data_type == MohaveDataType.DATE) else replace_non_date_value
        )
    elif (
        invalid_data_handling_method
        == NonCastableDataHandlingMethod.REPLACE_WITH_FIXED_VALUE_AND_PUT_NON_CASTABLE_DATA_IN_NEW_COLUMN
    ):
        # Replace non-castable data to a value in the same column and put non-castable data to a new column
        # Original dataframe
        # +---+------+
        # | id | txt |
        # +---+---+--+
        # | 1 | foo  |
        # | 2 | bar  |
        # | 3 | 1    |
        # +---+------+
        # cast txt column to long, replace non-castable value to 0
        # +---+----+------------------+
        # | id|txt |txt_typecast_error|
        # +---+----+------------------+
        # |  1|  0  |   foo           |
        # |  2|  0  |   bar           |
        # |  3|  1  |                 |
        # +---+----+------------------+
        value = _validate_and_cast_value(value=replace_value, mohave_data_type=mohave_data_type)

        df = df.withColumn(temp_column, cast_to_date if (mohave_data_type == MohaveDataType.DATE) else cast_to_non_date)
        df = df.withColumn(non_castable_column, f.when(df[temp_column].isNotNull(), "").otherwise(df[column]),)

        replace_date_value = f.when(df[temp_column].isNotNull(), df[temp_column]).otherwise(
            f.to_date(f.lit(value), date_formatting)
        )
        replace_non_date_value = f.when(df[temp_column].isNotNull(), df[temp_column]).otherwise(value)

        df = df.withColumn(
            temp_column, replace_date_value if (mohave_data_type == MohaveDataType.DATE) else replace_non_date_value
        )
    # drop temporary column
    df = df.withColumn(column, df[temp_column]).drop(temp_column)

    df_cols = df.columns
    if non_castable_column in df_cols:
        # Arrange columns so that non_castable_column col is next to casted column
        df_cols.remove(non_castable_column)
        column_index = df_cols.index(column)
        arranged_cols = df_cols[: column_index + 1] + [non_castable_column] + df_cols[column_index + 1 :]
        df = df.select(*arranged_cols)
    return df


def _validate_and_cast_value(value, mohave_data_type):
    if value is None:
        return value
    try:
        return PYTHON_TYPE_MAPPING[mohave_data_type](value)
    except ValueError as e:
        raise ValueError(
            f"Invalid value to replace non-castable data. "
            f"{mohave_data_type} is not in mohave supported date type: {MohaveDataType.get_values()}. "
            f"Please use a supported type",
            e,
        )


import os
import collections
import tempfile
import zipfile
import base64
import logging
from io import BytesIO
import numpy as np


class OperatorSparkOperatorCustomerError(Exception):
    """Error type for Customer Errors in Spark Operators"""


def temp_col_name(df, *illegal_names):
    """Generates a temporary column name that is unused.
    """
    name = "temp_col"
    idx = 0
    name_set = set(list(df.columns) + list(illegal_names))
    while name in name_set:
        name = f"_temp_col_{idx}"
        idx += 1

    return name


def get_temp_col_if_not_set(df, col_name):
    """Extracts the column name from the parameters if it exists, otherwise generates a temporary column name.
    """
    if col_name:
        return col_name, False
    else:
        return temp_col_name(df), True


def replace_input_if_output_is_temp(df, input_column, output_column, output_is_temp):
    """Replaces the input column in the dataframe if the output was not set

    This is used with get_temp_col_if_not_set to enable the behavior where a 
    transformer will replace its input column if an output is not specified.
    """
    if output_is_temp:
        df = df.withColumn(input_column, df[output_column])
        df = df.drop(output_column)
        return df
    else:
        return df


def parse_parameter(typ, value, key, default=None, nullable=False):
    if value is None:
        if default is not None or nullable:
            return default
        else:
            raise OperatorSparkOperatorCustomerError(f"Missing required input: '{key}'")
    else:
        try:
            value = typ(value)
            if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
                if np.isnan(value) or np.isinf(value):
                    raise OperatorSparkOperatorCustomerError(
                        f"Invalid value provided for '{key}'. Expected {typ.__name__} but received: {value}"
                    )
                else:
                    return value
            else:
                return value
        except (ValueError, TypeError):
            raise OperatorSparkOperatorCustomerError(
                f"Invalid value provided for '{key}'. Expected {typ.__name__} but received: {value}"
            )
        except OverflowError:
            raise OperatorSparkOperatorCustomerError(
                f"Overflow Error: Invalid value provided for '{key}'. Given value '{value}' exceeds the range of type "
                f"'{typ.__name__}' for this input. Insert a valid value for type '{typ.__name__}' and try your request "
                f"again."
            )


def expects_valid_column_name(value, key, nullable=False):
    if nullable and value is None:
        return

    if value is None or len(str(value).strip()) == 0:
        raise OperatorSparkOperatorCustomerError(f"Column name cannot be null, empty, or whitespace for parameter '{key}': {value}")


def expects_parameter(value, key, condition=None):
    if value is None:
        raise OperatorSparkOperatorCustomerError(f"Missing required input: '{key}'")
    elif condition is not None and not condition:
        raise OperatorSparkOperatorCustomerError(f"Invalid value provided for '{key}': {value}")


def expects_column(df, value, key):
    if not value or value not in df.columns:
        raise OperatorSparkOperatorCustomerError(f"Expected column in dataframe for '{key}' however received '{value}'")


def expects_parameter_value_in_list(key, value, items):
    if value not in items:
        raise OperatorSparkOperatorCustomerError(f"Illegal parameter value. {key} expected to be in {items}, but given {value}")


def encode_pyspark_model(model):
    with tempfile.TemporaryDirectory() as dirpath:
        dirpath = os.path.join(dirpath, "model")
        # Save the model
        model.save(dirpath)

        # Create the temporary zip-file.
        mem_zip = BytesIO()
        with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            # Zip the directory.
            for root, dirs, files in os.walk(dirpath):
                for file in files:
                    rel_dir = os.path.relpath(root, dirpath)
                    zf.write(os.path.join(root, file), os.path.join(rel_dir, file))

        zipped = mem_zip.getvalue()
        encoded = base64.b85encode(zipped)
        return str(encoded, "utf-8")


def decode_pyspark_model(model_factory, encoded):
    with tempfile.TemporaryDirectory() as dirpath:
        zip_bytes = base64.b85decode(encoded)
        mem_zip = BytesIO(zip_bytes)
        mem_zip.seek(0)

        with zipfile.ZipFile(mem_zip, "r") as zf:
            zf.extractall(dirpath)

        model = model_factory.load(dirpath)
        return model


def hash_parameters(value):
    # pylint: disable=W0702
    try:
        if isinstance(value, collections.Hashable):
            return hash(value)
        if isinstance(value, dict):
            return hash(frozenset([hash((hash_parameters(k), hash_parameters(v))) for k, v in value.items()]))
        if isinstance(value, list):
            return hash(frozenset([hash_parameters(v) for v in value]))
        raise RuntimeError("Object not supported for serialization")
    except:  # noqa: E722
        raise RuntimeError("Object not supported for serialization")


def load_trained_parameters(trained_parameters, operator_parameters):
    trained_parameters = trained_parameters if trained_parameters else {}
    parameters_hash = hash_parameters(operator_parameters)
    stored_hash = trained_parameters.get("_hash")
    if stored_hash != parameters_hash:
        trained_parameters = {"_hash": parameters_hash}
    return trained_parameters


def load_pyspark_model_from_trained_parameters(trained_parameters, model_factory, name):
    if trained_parameters is None or name not in trained_parameters:
        return None, False

    try:
        model = decode_pyspark_model(model_factory, trained_parameters[name])
        return model, True
    except Exception as e:
        logging.error(f"Could not decode PySpark model {name} from trained_parameters: {e}")
        del trained_parameters[name]
        return None, False


def fit_and_save_model(trained_parameters, name, algorithm, df):
    model = algorithm.fit(df)
    trained_parameters[name] = encode_pyspark_model(model)
    return model


def transform_using_trained_model(model, df, loaded):
    try:
        return model.transform(df)
    except Exception as e:
        if loaded:
            raise OperatorSparkOperatorCustomerError(
                f"Encountered error while using stored model. Please delete the operator and try again. {e}"
            )
        else:
            raise e


import re
from datetime import date

import numpy as np
import pandas as pd
from pyspark.sql.types import (
    BooleanType,
    IntegralType,
    FractionalType,
    StringType,
)



def type_inference(df):  # noqa: C901 # pylint: disable=R0912
    """Core type inference logic

    Args:
        df: spark dataframe

    Returns: dict a schema that maps from column name to mohave datatype

    """
    columns_to_infer = [col for (col, col_type) in df.dtypes if col_type == "string"]

    pandas_df = df.toPandas()
    report = {}
    for (columnName, _) in pandas_df.iteritems():
        if columnName in columns_to_infer:
            column = pandas_df[columnName].values
            report[columnName] = {
                "sum_string": len(column),
                "sum_numeric": sum_is_numeric(column),
                "sum_integer": sum_is_integer(column),
                "sum_boolean": sum_is_boolean(column),
                "sum_date": sum_is_date(column),
                "sum_null_like": sum_is_null_like(column),
                "sum_null": sum_is_null(column),
            }

    # Analyze
    numeric_threshold = 0.8
    integer_threshold = 0.8
    date_threshold = 0.8
    bool_threshold = 0.8

    column_types = {}

    for col, insights in report.items():
        # Convert all columns to floats to make thresholds easy to calculate.
        proposed = MohaveDataType.STRING.value
        if (insights["sum_numeric"] / insights["sum_string"]) > numeric_threshold:
            proposed = MohaveDataType.FLOAT.value
            if (insights["sum_integer"] / insights["sum_numeric"]) > integer_threshold:
                proposed = MohaveDataType.LONG.value
        elif (insights["sum_boolean"] / insights["sum_string"]) > bool_threshold:
            proposed = MohaveDataType.BOOL.value
        elif (insights["sum_date"] / insights["sum_string"]) > date_threshold:
            proposed = MohaveDataType.DATE.value
        column_types[col] = proposed

    for f in df.schema.fields:
        if f.name not in columns_to_infer:
            if isinstance(f.dataType, IntegralType):
                column_types[f.name] = MohaveDataType.LONG.value
            elif isinstance(f.dataType, FractionalType):
                column_types[f.name] = MohaveDataType.FLOAT.value
            elif isinstance(f.dataType, StringType):
                column_types[f.name] = MohaveDataType.STRING.value
            elif isinstance(f.dataType, BooleanType):
                column_types[f.name] = MohaveDataType.BOOL.value
            else:
                # unsupported types in mohave
                column_types[f.name] = MohaveDataType.OBJECT.value

    return column_types


def _is_numeric_single(x):
    try:
        x_float = float(x)
        return np.isfinite(x_float)
    except ValueError:
        return False
    except TypeError:  # if x = None
        return False


def sum_is_numeric(x):
    """count number of numeric element

    Args:
        x: numpy array

    Returns: int

    """
    castables = np.vectorize(_is_numeric_single)(x)
    return np.count_nonzero(castables)


def _is_integer_single(x):
    try:
        if not _is_numeric_single(x):
            return False
        return float(x) == int(x)
    except ValueError:
        return False
    except TypeError:  # if x = None
        return False


def sum_is_integer(x):
    castables = np.vectorize(_is_integer_single)(x)
    return np.count_nonzero(castables)


def _is_boolean_single(x):
    boolean_list = ["true", "false"]
    try:
        is_boolean = x.lower() in boolean_list
        return is_boolean
    except ValueError:
        return False
    except TypeError:  # if x = None
        return False
    except AttributeError:
        return False


def sum_is_boolean(x):
    castables = np.vectorize(_is_boolean_single)(x)
    return np.count_nonzero(castables)


def sum_is_null_like(x):  # noqa: C901
    def _is_empty_single(x):
        try:
            return bool(len(x) == 0)
        except TypeError:
            return False

    def _is_null_like_single(x):
        try:
            return bool(null_like_regex.match(x))
        except TypeError:
            return False

    def _is_whitespace_like_single(x):
        try:
            return bool(whitespace_regex.match(x))
        except TypeError:
            return False

    null_like_regex = re.compile(r"(?i)(null|none|nil|na|nan)")  # (?i) = case insensitive
    whitespace_regex = re.compile(r"^\s+$")  # only whitespace

    empty_checker = np.vectorize(_is_empty_single)(x)
    num_is_null_like = np.count_nonzero(empty_checker)

    null_like_checker = np.vectorize(_is_null_like_single)(x)
    num_is_null_like += np.count_nonzero(null_like_checker)

    whitespace_checker = np.vectorize(_is_whitespace_like_single)(x)
    num_is_null_like += np.count_nonzero(whitespace_checker)
    return num_is_null_like


def sum_is_null(x):
    return np.count_nonzero(pd.isnull(x))


def _is_date_single(x):
    try:
        return bool(date.fromisoformat(x))  # YYYY-MM-DD
    except ValueError:
        return False
    except TypeError:
        return False


def sum_is_date(x):
    return np.count_nonzero(np.vectorize(_is_date_single)(x))


def cast_df(df, schema):
    """Cast datafram from given schema

    Args:
        df: spark dataframe
        schema: schema to cast to. It map from df's col_name to mohave datatype

    Returns: casted dataframe

    """
    # col name to spark data type mapping
    col_to_spark_data_type_map = {}

    # get spark dataframe's actual datatype
    fields = df.schema.fields
    for f in fields:
        col_to_spark_data_type_map[f.name] = f.dataType
    cast_expr = []
    # iterate given schema and cast spark dataframe datatype
    for col_name in schema:
        mohave_data_type_from_schema = MohaveDataType(schema.get(col_name, MohaveDataType.OBJECT.value))
        if mohave_data_type_from_schema != MohaveDataType.OBJECT:
            spark_data_type_from_schema = MOHAVE_TO_SPARK_TYPE_MAPPING[mohave_data_type_from_schema]
            # Only cast column when the data type in schema doesn't match the actual data type
            if not isinstance(col_to_spark_data_type_map[col_name], spark_data_type_from_schema):
                # use spark-sql expression instead of spark.withColumn to improve performance
                expr = f"CAST (`{col_name}` as {SPARK_TYPE_MAPPING_TO_SQL_TYPE[spark_data_type_from_schema]})"
            else:
                # include column that has same dataType as it is
                expr = f"`{col_name}`"
        else:
            # include column that has same mohave object dataType as it is
            expr = f"`{col_name}`"
        cast_expr.append(expr)
    if len(cast_expr) != 0:
        df = df.selectExpr(*cast_expr)
    return df, schema


def validate_schema(df, schema):
    """Validate if every column is covered in the schema

    Args:
        schema ():
    """
    columns_in_df = df.columns
    columns_in_schema = schema.keys()

    if len(columns_in_df) != len(columns_in_schema):
        raise ValueError(
            f"Invalid schema column size. "
            f"Number of columns in schema should be equal as number of columns in dataframe. "
            f"schema columns size: {len(columns_in_schema)}, dataframe column size: {len(columns_in_df)}"
        )

    for col in columns_in_schema:
        if col not in columns_in_df:
            raise ValueError(
                f"Invalid column name in schema. "
                f"Column in schema does not exist in dataframe. "
                f"Non-existed columns: {col}"
            )


def s3_source(spark, mode, dataset_definition):
    """Represents a source that handles sampling, etc."""

    content_type = dataset_definition["s3ExecutionContext"]["s3ContentType"].upper()
    has_header = dataset_definition["s3ExecutionContext"]["s3HasHeader"]
    path = dataset_definition["s3ExecutionContext"]["s3Uri"].replace("s3://", "s3a://")

    try:
        if content_type == "CSV":
            df = spark.read.csv(path=path, header=has_header, escape='"', quote='"')
        elif content_type == "PARQUET":
            df = spark.read.parquet(path)

        return default_spark(df)
    except Exception as e:
        raise RuntimeError("An error occurred while reading files from S3") from e


def infer_and_cast_type(df, spark, inference_data_sample_size=1000, trained_parameters=None):
    """Infer column types for spark dataframe and cast to inferred data type.

    Args:
        df: spark dataframe
        spark: spark session
        inference_data_sample_size: number of row data used for type inference
        trained_parameters: trained_parameters to determine if we need infer data types

    Returns: a dict of pyspark df with column data type casted and trained parameters

    """
    from pyspark.sql.utils import AnalysisException

    # if trained_parameters is none or doesn't contain schema key, then type inference is needed
    if trained_parameters is None or not trained_parameters.get("schema", None):
        # limit first 1000 rows to do type inference

        limit_df = df.limit(inference_data_sample_size)
        schema = type_inference(limit_df)
    else:
        schema = trained_parameters["schema"]
        try:
            validate_schema(df, schema)
        except ValueError as e:
            raise OperatorCustomerError(e)
    try:
        df, schema = cast_df(df, schema)
    except (AnalysisException, ValueError) as e:
        raise OperatorCustomerError(e)
    trained_parameters = {"schema": schema}
    return default_spark_with_trained_parameters(df, trained_parameters)


def custom_pandas(df, spark, code):
    """ Apply custom pandas code written by the user on the input dataframe.

    Right now only pyspark dataframe is supported as input, so the pyspark df is
    converted to pandas df before the custom pandas code is being executed.

    The output df is converted back to pyspark df before getting returned.

    Example:
        The custom code expects the user to provide an output df.
        code = \"""
        import pandas as pd
        df = pd.get_dummies(df['country'], prefix='country')
        \"""

    Notes:
        This operation expects the user code to store the output in df variable.

    Args:
        spark: Spark Session
        params (dict): dictionary that has various params. Required param for this operation is "code"
        df: pyspark dataframe

    Returns:
        df: pyspark dataframe with the custom pandas code executed on the input df.
    """
    import ast

    exec_block = ast.parse(code, mode="exec")
    if len(exec_block.body) == 0:
        return default_spark(df)

    pandas_df = df.toPandas()

    _globals, _locals = {}, {"df": pandas_df}

    stdout = capture_stdout(exec, compile(exec_block, "<string>", mode="exec"), _locals)  # pylint: disable=W0122

    pandas_df = eval("df", _globals, _locals)  # pylint: disable=W0123

    # find list of columns with all None values and fill with empty str.
    null_columns = pandas_df.columns[pandas_df.isnull().all()].tolist()
    pandas_df[null_columns] = pandas_df[null_columns].fillna("")

    # convert the mixed cols to str, since pyspark df does not support mixed col.
    df = convert_or_coerce(pandas_df, spark)

    # while statement is to recurse over all fields that have mixed type and cannot be converted
    while not isinstance(df, DataFrame):
        df = convert_or_coerce(df, spark)

    return default_spark_with_stdout(df, stdout)


def format_string(df, spark, **kwargs):

    return dispatch(
        "operator",
        [df],
        kwargs,
        {
            "Lower case": (format_string_lower_case, "lower_case_parameters"),
            "Upper case": (format_string_upper_case, "upper_case_parameters"),
            "Title case": (format_string_title_case, "title_case_parameters"),
            "Capitalize": (format_string_capitalize, "capitalize_parameters"),
            "Swap case": (format_string_swap_case, "swap_case_parameters"),
            "Left pad": (format_string_left_pad, "left_pad_parameters"),
            "Right pad": (format_string_right_pad, "right_pad_parameters"),
            "Center (pad on either side)": (
                format_string_center_pad_on_either_side,
                "center_pad_on_either_side_parameters",
            ),
            "Strip characters from left": (
                format_string_strip_characters_from_left,
                "strip_characters_from_left_parameters",
            ),
            "Strip characters from right": (
                format_string_strip_characters_from_right,
                "strip_characters_from_right_parameters",
            ),
            "Strip left and right": (format_string_strip_left_and_right, "strip_left_and_right_parameters"),
            "Prepend zeros": (format_string_prepend_zeros, "prepend_zeros_parameters"),
            "Add prefix": (format_string_add_prefix, "add_prefix_parameters"),
            "Add suffix": (format_string_add_suffix, "add_suffix_parameters"),
            "Remove symbols": (format_string_remove_symbols, "remove_symbols_parameters"),
        },
    )


def encode_categorical(df, spark, **kwargs):

    return dispatch(
        "operator",
        [df],
        kwargs,
        {
            "Ordinal encode": (encode_categorical_ordinal_encode, "ordinal_encode_parameters"),
            "One-hot encode": (encode_categorical_one_hot_encode, "one_hot_encode_parameters"),
        },
    )


def cast_single_data_type(  # noqa: C901
    df,
    spark,
    column,
    data_type,
    non_castable_data_handling_method="replace_null",
    replace_value=None,
    date_formatting="dd-MM-yyyy",
):
    """Cast pyspark dataframe column type

    Args:
        column: column name e.g.: "col_1"
        data_type: data type to cast to
        non_castable_data_handling_method:
            supported method:
                ("replace_null","replace_null_with_new_col", "replace_value","replace_value_with_new_col","drop")
            If not specified, it will use the default method replace_null.
            see casting.NonCastableDataHandlingMethod
        replace_value: value to replace non-castable data
        date_formatting: date format to cast to
    Returns: df: pyspark df with column data type casted
    """
    from pyspark.sql.utils import AnalysisException

    supported_type = MohaveDataType.get_values()
    df_cols = df.columns
    # Validate input params
    if column not in df_cols:
        raise OperatorCustomerError(
            f"Invalid column name. {column} is not in current columns {df_cols}. Please use a valid column name."
        )
    if data_type not in supported_type:
        raise OperatorCustomerError(
            f"Invalid data_type. {data_type} is not in {supported_type}. Please use a supported data type."
        )

    support_invalid_data_handling_method = NonCastableDataHandlingMethod.get_values()
    if non_castable_data_handling_method not in support_invalid_data_handling_method:
        raise OperatorCustomerError(
            f"Invalid data handling method. "
            f"{non_castable_data_handling_method} is not in {support_invalid_data_handling_method}. "
            f"Please use a supported method."
        )

    mohave_data_type = MohaveDataType(data_type)

    spark_data_type = [f.dataType for f in df.schema.fields if f.name == column]

    if isinstance(spark_data_type[0], MOHAVE_TO_SPARK_TYPE_MAPPING[mohave_data_type]):
        return default_spark(df)

    try:
        df = cast_single_column_type(
            df,
            column=column,
            mohave_data_type=MohaveDataType(data_type),
            invalid_data_handling_method=NonCastableDataHandlingMethod(non_castable_data_handling_method),
            replace_value=replace_value,
            date_formatting=date_formatting,
        )
    except (AnalysisException, ValueError) as e:
        raise OperatorCustomerError(e)

    return default_spark(df)


op_1_output = s3_source(spark=spark, mode=mode, **{'dataset_definition': {'__typename': 'S3CreateDatasetDefinitionOutput', 'datasetSourceType': 'S3', 'name': 'claims.csv', 'description': None, 's3ExecutionContext': {'__typename': 'S3ExecutionContext', 's3Uri': 's3://sagemaker-us-east-1-870180618679/fraud-detect-demo/data/raw/claims.csv', 's3ContentType': 'csv', 's3HasHeader': True}}})
op_2_output = infer_and_cast_type(op_1_output['default'], spark=spark, **{})
op_3_output = custom_pandas(op_2_output['default'], spark=spark, **{'code': '# Table is available as variable `df`\ncat_cols = df.dtypes[df.dtypes == object].index\ndf[cat_cols] = df[cat_cols].apply(lambda x: x.str.lower())\n'})
op_4_output = format_string(op_3_output['default'], spark=spark, **{'operator': 'Remove symbols', 'remove_symbols_parameters': {'symbols': '!@#$%^&*()_+=-/\\`~{}|<>?', 'input_column': 'driver_relationship'}, 'lower_case_parameters': {}})
op_5_output = format_string(op_4_output['default'], spark=spark, **{'operator': 'Remove symbols', 'remove_symbols_parameters': {'symbols': '!@#$%^&*()_+=-/\\`~{}|<>?', 'input_column': 'collision_type'}, 'lower_case_parameters': {}})
op_6_output = format_string(op_5_output['default'], spark=spark, **{'operator': 'Remove symbols', 'remove_symbols_parameters': {'symbols': '!@#$%^&*()_+=-/\\`~{}|<>?', 'input_column': 'incident_type'}, 'lower_case_parameters': {}})
op_7_output = encode_categorical(op_6_output['default'], spark=spark, **{'operator': 'One-hot encode', 'one_hot_encode_parameters': {'invalid_handling_strategy': 'Keep', 'drop_last': False, 'output_style': 'Columns', 'input_column': 'driver_relationship'}, 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN'}})
op_8_output = encode_categorical(op_7_output['default'], spark=spark, **{'operator': 'One-hot encode', 'one_hot_encode_parameters': {'invalid_handling_strategy': 'Keep', 'drop_last': False, 'output_style': 'Columns', 'input_column': 'incident_type'}, 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN'}})
op_9_output = encode_categorical(op_8_output['default'], spark=spark, **{'operator': 'One-hot encode', 'one_hot_encode_parameters': {'invalid_handling_strategy': 'Keep', 'drop_last': False, 'output_style': 'Columns', 'input_column': 'collision_type'}, 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN'}})
op_10_output = encode_categorical(op_9_output['default'], spark=spark, **{'operator': 'One-hot encode', 'one_hot_encode_parameters': {'invalid_handling_strategy': 'Keep', 'drop_last': False, 'output_style': 'Columns', 'input_column': 'authorities_contacted'}, 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN'}})
op_11_output = encode_categorical(op_10_output['default'], spark=spark, **{'operator': 'Ordinal encode', 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN', 'input_column': 'incident_severity'}})
op_12_output = encode_categorical(op_11_output['default'], spark=spark, **{'operator': 'Ordinal encode', 'ordinal_encode_parameters': {'invalid_handling_strategy': 'Replace with NaN', 'input_column': 'police_report_available'}})
op_13_output = custom_pandas(op_12_output['default'], spark=spark, **{'code': "# Table is available as variable `df`\nimport pandas as pd\ndf['event_time'] = pd.to_datetime('now').timestamp()"})
op_14_output = cast_single_data_type(op_13_output['default'], spark=spark, **{'column': 'police_report_available', 'original_data_type': 'Float', 'data_type': 'long'})
op_15_output = cast_single_data_type(op_14_output['default'], spark=spark, **{'column': 'authorities_contacted_fire', 'original_data_type': 'Float', 'data_type': 'long'})
op_16_output = cast_single_data_type(op_15_output['default'], spark=spark, **{'column': 'authorities_contacted_ambulance', 'original_data_type': 'Float', 'data_type': 'long'})
op_17_output = cast_single_data_type(op_16_output['default'], spark=spark, **{'column': 'authorities_contacted_none', 'original_data_type': 'Float', 'data_type': 'long'})
op_18_output = cast_single_data_type(op_17_output['default'], spark=spark, **{'column': 'authorities_contacted_police', 'original_data_type': 'Float', 'data_type': 'long'})
op_19_output = cast_single_data_type(op_18_output['default'], spark=spark, **{'column': 'collision_type_na', 'original_data_type': 'Float', 'data_type': 'long'})
op_20_output = cast_single_data_type(op_19_output['default'], spark=spark, **{'column': 'collision_type_side', 'original_data_type': 'Float', 'data_type': 'long'})
op_21_output = cast_single_data_type(op_20_output['default'], spark=spark, **{'column': 'incident_type_theft', 'original_data_type': 'Float', 'data_type': 'long'})
op_22_output = cast_single_data_type(op_21_output['default'], spark=spark, **{'column': 'incident_type_breakin', 'original_data_type': 'Float', 'data_type': 'long'})
op_23_output = cast_single_data_type(op_22_output['default'], spark=spark, **{'column': 'incident_type_collision', 'original_data_type': 'Float', 'data_type': 'long'})
op_24_output = cast_single_data_type(op_23_output['default'], spark=spark, **{'column': 'driver_relationship_other', 'original_data_type': 'Float', 'data_type': 'long'})
op_25_output = cast_single_data_type(op_24_output['default'], spark=spark, **{'column': 'driver_relationship_child', 'original_data_type': 'Float', 'data_type': 'long'})
op_26_output = cast_single_data_type(op_25_output['default'], spark=spark, **{'column': 'driver_relationship_spouse', 'original_data_type': 'Float', 'data_type': 'long'})
op_27_output = cast_single_data_type(op_26_output['default'], spark=spark, **{'column': 'driver_relationship_na', 'original_data_type': 'Float', 'data_type': 'long'})
op_28_output = cast_single_data_type(op_27_output['default'], spark=spark, **{'column': 'driver_relationship_self', 'original_data_type': 'Float', 'data_type': 'long'})
op_29_output = cast_single_data_type(op_28_output['default'], spark=spark, **{'column': 'total_claim_amount', 'original_data_type': 'Long', 'data_type': 'float'})
op_30_output = cast_single_data_type(op_29_output['default'], spark=spark, **{'column': 'vehicle_claim', 'original_data_type': 'Long', 'data_type': 'float'})
op_31_output = cast_single_data_type(op_30_output['default'], spark=spark, **{'column': 'injury_claim', 'original_data_type': 'Long', 'data_type': 'float'})
op_32_output = cast_single_data_type(op_31_output['default'], spark=spark, **{'column': 'incident_severity', 'original_data_type': 'Float', 'data_type': 'long'})
op_33_output = cast_single_data_type(op_32_output['default'], spark=spark, **{'column': 'collision_type_rear', 'original_data_type': 'Float', 'data_type': 'long'})
op_34_output = cast_single_data_type(op_33_output['default'], spark=spark, **{'column': 'collision_type_front', 'original_data_type': 'Float', 'data_type': 'long'})

#  Glossary: variable name to node_id
#
#  op_1_output: e5d60d4f-6284-4a68-a788-39787909ebc9
#  op_2_output: 1626aef2-cad0-4922-a496-34586d3def90
#  op_3_output: 921ad4e5-3812-4651-b3cd-fecc38e0bba6
#  op_4_output: e90bfd12-d702-4c79-8e11-5e4011600fda
#  op_5_output: 10ca6bec-5d89-43bd-ba3e-7b5e839e9d23
#  op_6_output: 09a3e1c7-29c5-46e8-8690-4cdaf905628a
#  op_7_output: a5333162-b98e-41f4-8b18-5bb68b98a615
#  op_8_output: de96bceb-ac8c-40d4-a9e8-5a777b44437c
#  op_9_output: 2e5cb36f-b0a5-4763-9740-2ef60f3c6376
#  op_10_output: e20b22bc-12aa-469e-9d6c-c4083ddedf08
#  op_11_output: cd48da2b-c648-41e2-b828-38b518fc795d
#  op_12_output: 9ff4c2a4-e1fe-4a66-819f-f7d8df7a5fed
#  op_13_output: 67fdfb06-278a-4360-8454-3ec1abf1ddd3
#  op_14_output: f324323b-8d6e-4e6f-a362-eb9405c2aabf
#  op_15_output: c3e12fe4-0792-4627-a728-43bb51312196
#  op_16_output: 0e239308-1ad0-421b-9e8f-36011d83a6b6
#  op_17_output: 93d33455-1cf2-485d-aaf7-5ae5c7d5197e
#  op_18_output: 0eff4390-66cb-423a-bad4-40fc368aa6d4
#  op_19_output: 488aa71e-881e-451e-be8b-5d960530715c
#  op_20_output: 38ede4e0-ac3c-4e73-9b5e-ad5216997520
#  op_21_output: dc6b5e18-0423-4940-afbe-57e24560400e
#  op_22_output: acb3f245-c50b-4174-8295-5b933500568b
#  op_23_output: 9dfb1906-6d9a-45d3-ac92-d5f3f535ff8a
#  op_24_output: a65a0721-7696-4e64-9e65-2423dc65163e
#  op_25_output: 9b23353f-c840-4bed-9368-636a249038b7
#  op_26_output: 1c9c8473-9ccb-4ce4-a2a0-db15c648565e
#  op_27_output: a00c44fb-3704-4e1c-bda7-887f0feae6a6
#  op_28_output: 7bd2e323-2ef9-4329-844a-0f2406ff980c
#  op_29_output: 7132ae96-da8f-4a02-8708-0bc233a5ecd2
#  op_30_output: 10a9d871-4660-44fb-aa9f-14f14a4f5a44
#  op_31_output: 01b14795-ad16-4e33-af09-5cedf7a3b806
#  op_32_output: 8328f4cc-9adf-47e7-9752-9ab1cf83e7b9
#  op_33_output: 6d59ff64-c095-4f20-a7ba-c4a49e842c7c
#  op_34_output: 62d710d9-a288-4004-b960-6cf452c0380c