import pytest

from .spark import spark
from chispa import *


class TestDescribeAssertColumnEquality:
    def test_it_throws_error_with_data_mismatch(self):
        data = [("jose", "jose"), ("li", "li"), ("luisa", "laura")]
        df = spark.createDataFrame(data, ["name", "expected_name"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_column_equality(df, "name", "expected_name")

    def test_it_doesnt_throw_without_mismatch(self):
        data = [("jose", "jose"), ("li", "li"), ("luisa", "luisa"), (None, None)]
        df = spark.createDataFrame(data, ["name", "expected_name"])
        assert_column_equality(df, "name", "expected_name")


    def test_it_works_with_integer_values(self):
        data = [(1, 1), (10, 10), (8, 8), (None, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        assert_column_equality(df, "num1", "num2")


class TestDescribeAssertApproxColumnEquality:
    def test_it_works_with_no_mismatches(self):
        data = [(1.1, 1.1), (1.0004, 1.0005), (.4, .45), (None, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        assert_approx_column_equality(df, "num1", "num2", 0.1)


    def test_it_throws_when_difference_is_bigger_than_precision(self):
        data = [(1.5, 1.1), (1.0004, 1.0005), (.4, .45)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_approx_column_equality(df, "num1", "num2", 0.1)


    def test_it_throws_when_comparing_floats_with_none(self):
        data = [(1.1, 1.1), (2.2, 2.2), (3.3, None)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_approx_column_equality(df, "num1", "num2", 0.1)


    def test_it_throws_when_comparing_none_with_floats(self):
        data = [(1.1, 1.1), (2.2, 2.2), (None, 3.3)]
        df = spark.createDataFrame(data, ["num1", "num2"])
        with pytest.raises(ColumnsNotEqualError) as e_info:
            assert_approx_column_equality(df, "num1", "num2", 0.1)

