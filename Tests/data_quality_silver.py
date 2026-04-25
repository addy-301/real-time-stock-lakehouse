import great_expectations as gx
from great_expectations.data_context import FileDataContext

context=gx.get_context()
datasource=context.data_sources.add_pandas("silver_datasource")
validator=context.get_validator(
    batch_request=datasource.get_batch_request()
)

validator.expect_column_values_to_not_be_null("symbol")
validator.expect_column_values_to_not_be_null("data_quality_score")
validator.expect_column_values_to_not_be_null("load_timestamp")
validator.expect_column_values_to_not_be_null("transformation_logic")
validator.expect_column_values_to_not_be_null("source_system")
validator.expect_column_values_to_not_be_null("validation_status")
validator.expect_column_values_to_not_be_null("validation_timestamp")
validator.expect_column_values_to_not_be_null("validation_details")

validator.expect_column_values_to_be_of_type("price", "double")
validator.expect_column_values_to_be_of_type("volume", "long")
validator.expect_column_values_to_be_of_type("timestamp", "timestamp")
validator.expect_column_values_to_be_of_type("data_quality_score", "double")
validator.expect_column_values_to_be_of_type("load_timestamp", "timestamp")
validator.expect_column_values_to_be_of_type("transformation_logic", "string")
validator.expect_column_values_to_be_of_type("source_system", "string")
validator.expect_column_values_to_be_of_type("validation_status", "string")
validator.expect_column_values_to_be_of_type("validation_timestamp", "timestamp")
validator.expect_column_values_to_be_of_type("validation_details", "string")

validator.expect_column_values_to_be_between("price", min_value=0.01, max_value=10000)
validator.expect_column_values_to_be_between("volume", min_value=0, max_value=100000000)
validator.expect_column_values_to_be_between("data_quality_score", min_value=0.0, max_value=1.0)

validator.expect_column_values_to_match_regex("symbol", r"^[A-Z]+$")
validator.expect_column_values_to_be_in_set("market_cap_category", ["Large", "Mid", "Small"])
validator.expect_column_values_to_be_unique(["symbol", "timestamp"])

results=validator.validate()
print("Data Quality Results:")
print(f"Success: {results.success}")
print(results.results)