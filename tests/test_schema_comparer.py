import pytest

from pyspark.sql.types import *
from chispa.schema_comparer import *


class TestDescribeAssertSchemaEquality:
    def test_it_does_nothing_when_equal(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        assert_schema_equality(s1, s2)


    def test_it_throws_when_column_names_differ(self):
        s1 = StructType([
           StructField("HAHA", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality(s1, s2)


    def test_it_throws_when_schema_lengths_differ(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True),
           StructField("fav_number", IntegerType(), True)])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality(s1, s2)


class TestDescribeAssertSchemaEqualityIgnoreNullable:
    def test_it_has_good_error_messages_for_different_sized_schemas(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), False),
           StructField("age", IntegerType(), True),
           StructField("something", IntegerType(), True),
           StructField("else", IntegerType(), True)
        ])
        with pytest.raises(SchemasNotEqualError) as e_info:
            assert_schema_equality_ignore_nullable(s1, s2)


    def test_it_does_nothing_when_equal(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        assert_schema_equality_ignore_nullable(s1, s2)


    def test_it_does_nothing_when_only_nullable_flag_is_different(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert_schema_equality_ignore_nullable(s1, s2)


class TestDescribeAreSchemasEqualIgnoreNullable:
    def test_it_returns_true_when_only_nullable_flag_is_different(self):
        s1 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), True),
           StructField("coords", ArrayType(DoubleType(), True), True),
        ])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False),
           StructField("coords", ArrayType(DoubleType(), True), False),
        ])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True


    def test_it_returns_true_when_only_nullable_flag_is_different_within_array_element(self):
        s1 = StructType([StructField("coords", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("coords", ArrayType(DoubleType(), False), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True

    def test_it_returns_true_when_only_nullable_flag_is_different_within_nested_array_element(self):
        s1 = StructType([StructField("coords", ArrayType(ArrayType(DoubleType(), True), True), True)])
        s2 = StructType([StructField("coords", ArrayType(ArrayType(DoubleType(), False), True), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == True


    def test_it_returns_false_when_the_element_type_is_different_within_array(self):
        s1 = StructType([StructField("coords", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("coords", ArrayType(IntegerType(), True), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False


    def test_it_returns_false_when_column_names_differ(self):
        s1 = StructType([
           StructField("blah", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("name", StringType(), True),
           StructField("age", IntegerType(), False)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False

    def test_it_returns_false_when_columns_have_different_order(self):
        s1 = StructType([
           StructField("blah", StringType(), True),
           StructField("age", IntegerType(), True)])
        s2 = StructType([
           StructField("age", IntegerType(), False),
           StructField("blah", StringType(), True)])
        assert are_schemas_equal_ignore_nullable(s1, s2) == False


class TestDescribeAreStructfieldsEqual:
    def test_it_returns_true_when_only_nullable_flag_is_different_within_array_element(self):
        s1 = StructField("coords", ArrayType(DoubleType(), True), True)
        s2 = StructField("coords", ArrayType(DoubleType(), False), True)
        assert are_structfields_equal(s1, s2, True) == True


    def test_it_returns_false_when_the_element_type_is_different_within_array(self):
        s1 = StructField("coords", ArrayType(DoubleType(), True), True)
        s2 = StructField("coords", ArrayType(IntegerType(), True), True)
        assert are_structfields_equal(s1, s2, True) == False


    def test_it_returns_true_when_the_element_type_is_same_within_struct(self):
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == True


    def test_it_returns_false_when_the_element_type_is_different_within_struct(self):
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", IntegerType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == False


    def test_it_returns_false_when_the_element_name_is_different_within_struct(self):
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("world", DoubleType(), True)]), True)
        assert are_structfields_equal(s1, s2, True) == False
       
        
    def test_it_returns_true_when_different_nullability_within_struct(self):
        s1 = StructField("coords", StructType([StructField("hello", DoubleType(), True)]), True)
        s2 = StructField("coords", StructType([StructField("hello", DoubleType(), False)]), True)
        assert are_structfields_equal(s1, s2, True) == True
    def test_it_returns_false_when_metadata_differs(self):
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=False) is False

    def test_it_allows_metadata_to_be_ignored(self):
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=False, ignore_metadata=True) is True

    def test_it_allows_nullability_and_metadata_to_be_ignored(self):
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), False, {"hi": "no"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=True) is True

    def test_it_returns_true_when_metadata_is_the_same(self):
        s1 = StructField("coords", StringType(), True, {"hi": "whatever"})
        s2 = StructField("coords", StringType(), True, {"hi": "whatever"})
        assert are_structfields_equal(s1, s2, ignore_nullability=True, ignore_metadata=False) is True
