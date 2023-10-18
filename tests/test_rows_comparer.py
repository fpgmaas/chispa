import pytest

from .spark import *
from chispa import *
from chispa.rows_comparer import assert_basic_rows_equality
from chispa import DataFramesNotEqualError
import math


class TestDescribeAssertBasicRowsEquality:
    def test_it_throws_with_row_mismatches(self):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [("bob", "jose"), ("li", "li"), ("luisa", "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        with pytest.raises(DataFramesNotEqualError) as e_info:
            assert_basic_rows_equality(df1.collect(), df2.collect())

    def test_it_works_when_rows_are_the_same(self):
        data1 = [(1, "jose"), (2, "li"), (3, "laura")]
        df1 = spark.createDataFrame(data1, ["num", "expected_name"])
        data2 = [(1, "jose"), (2, "li"), (3, "laura")]
        df2 = spark.createDataFrame(data2, ["name", "expected_name"])
        assert_basic_rows_equality(df1.collect(), df2.collect())

