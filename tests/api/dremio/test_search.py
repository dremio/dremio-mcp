#
#  Copyright (C) 2017-2025 Dremio Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import pytest
from pydantic import ValidationError
from dremioai.api.dremio.search import (
    Category,
    EnterpriseSearchResultsObject,
)


def assert_category_validation_error(category_value):
    """Helper function to assert that a category value raises ValidationError"""
    with pytest.raises(ValidationError) as exc_info:
        EnterpriseSearchResultsObject(category=category_value)

    # Check that the error is related to the category field
    errors = exc_info.value.errors()
    assert len(errors) == 1
    assert errors[0]["loc"] == ("category",)


class TestEnterpriseSearchResultsObjectCategoryValidation:
    """Test case-insensitive category validation in EnterpriseSearchResultsObject"""

    def test_category_none_value(self):
        """Test that None category value is handled correctly"""
        obj = EnterpriseSearchResultsObject(category=None)
        assert obj.category is None

    def test_category_valid_enum_value(self):
        """Test that valid Category enum values work correctly"""
        obj = EnterpriseSearchResultsObject(category=Category.JOB)
        assert obj.category == Category.JOB

    @pytest.mark.parametrize(
        "category_str,expected_enum",
        [
            # Generate test cases dynamically for each Category enum value
            (category.value.lower(), category)
            for category in Category
        ]
        + [(category.value.upper(), category) for category in Category]
        + [(category.value.capitalize(), category) for category in Category]
        + [
            # Add some mixed case variations for a few examples
            ("JoB", Category.JOB),
            ("TaBlE", Category.TABLE),
            ("ScRiPt", Category.SCRIPT),
        ],
    )
    def test_category_case_insensitive_strings(self, category_str, expected_enum):
        """Test that category strings are converted case-insensitively"""
        obj = EnterpriseSearchResultsObject(category=category_str)
        assert obj.category == expected_enum
        assert isinstance(obj.category, Category)

    @pytest.mark.parametrize(
        "invalid_category_value",
        [
            pytest.param("invalid_category", id="invalid_string"),
            pytest.param("", id="empty_string"),
            pytest.param("   ", id="whitespace_string"),
            pytest.param(123, id="numeric_value"),
            pytest.param(["job"], id="list_value"),
            pytest.param({"type": "job"}, id="dict_value"),
        ],
    )
    def test_category_invalid_values(self, invalid_category_value):
        """Test that invalid category values raise ValidationError"""
        assert_category_validation_error(invalid_category_value)


class TestEnterpriseSearchResultsObjectFullModel:
    """Test the complete EnterpriseSearchResultsObject model with category validation"""

    def test_model_with_valid_category_string(self):
        """Test creating model with valid category string"""
        data = {
            "category": "job",
            "jobObject": None,
            "scriptObject": None,
            "reflectionObject": None,
            "catalogObject": None,
        }
        obj = EnterpriseSearchResultsObject.model_validate(data)
        assert obj.category == Category.JOB

    def test_model_with_mixed_case_category(self):
        """Test creating model with mixed case category"""
        data = {
            "category": "TaBlE",
            "jobObject": None,
            "scriptObject": None,
            "reflectionObject": None,
            "catalogObject": None,
        }
        obj = EnterpriseSearchResultsObject.model_validate(data)
        assert obj.category == Category.TABLE

    def test_model_serialization_with_category(self):
        """Test that model serialization works correctly with category"""
        obj = EnterpriseSearchResultsObject(category="view")
        data = obj.model_dump()
        assert data["category"] == "VIEW"  # Should be the enum value

    def test_model_json_serialization_with_category(self):
        """Test JSON serialization with category"""
        obj = EnterpriseSearchResultsObject(category="script")
        json_str = obj.model_dump_json()
        assert '"category":"SCRIPT"' in json_str

    def test_model_validation_from_json_with_category(self):
        """Test model validation from JSON with category"""
        json_data = '{"category": "reflection"}'
        obj = EnterpriseSearchResultsObject.model_validate_json(json_data)
        assert obj.category == Category.REFLECTION


class TestCategoryValidationIntegration:
    """Integration tests for category validation with real-world scenarios"""

    def test_json_deserialization_with_lowercase_category(self):
        """Test JSON deserialization with lowercase category from API response"""
        json_response = """
        {
            "category": "table",
            "catalogObject": {
                "path": ["sample", "table"],
                "type": "TABLE"
            }
        }
        """
        obj = EnterpriseSearchResultsObject.model_validate_json(json_response)
        assert obj.category == Category.TABLE
        assert obj.catalog.path == ["sample", "table"]

    def test_json_deserialization_with_mixed_case_category(self):
        """Test JSON deserialization with mixed case category"""
        json_response = """
        {
            "category": "Script",
            "scriptObject": {
                "id": "script123",
                "name": "My Script"
            }
        }
        """
        obj = EnterpriseSearchResultsObject.model_validate_json(json_response)
        assert obj.category == Category.SCRIPT
        assert obj.script.id == "script123"

    def test_batch_validation_with_different_cases(self):
        """Test validating multiple objects with different category cases"""
        # Create test data dynamically using first 5 categories with different cases
        categories_list = list(Category)[:5]  # Take first 5 categories
        case_variations = ["lower", "upper", "capitalize", "mixed", "original"]

        test_data = []
        expected_categories = []

        for i, category in enumerate(categories_list):
            case_type = case_variations[i % len(case_variations)]

            if case_type == "lower":
                category_str = category.value.lower()
            elif case_type == "upper":
                category_str = category.value.upper()
            elif case_type == "capitalize":
                category_str = category.value.capitalize()
            elif case_type == "mixed":
                # Create mixed case by alternating upper/lower
                category_str = "".join(
                    c.upper() if i % 2 == 0 else c.lower()
                    for i, c in enumerate(category.value)
                )
            else:  # original
                category_str = category.value

            test_data.append({"category": category_str})
            expected_categories.append(category)

        objects = [
            EnterpriseSearchResultsObject.model_validate(data) for data in test_data
        ]
        actual_categories = [obj.category for obj in objects]

        assert actual_categories == expected_categories

    def test_model_dump_preserves_enum_values(self):
        """Test that model_dump returns the correct enum values"""
        obj = EnterpriseSearchResultsObject(category="view")
        dumped = obj.model_dump()

        # The dumped value should be the enum's string value (uppercase)
        assert dumped["category"] == "VIEW"

        # Re-validating the dumped data should work
        obj2 = EnterpriseSearchResultsObject.model_validate(dumped)
        assert obj2.category == Category.VIEW

    def test_category_validation_with_all_other_fields_none(self):
        """Test category validation when all other fields are None"""
        obj = EnterpriseSearchResultsObject(
            category="reflection", job=None, script=None, reflection=None, catalog=None
        )
        assert obj.category == Category.REFLECTION
        assert obj.job is None
        assert obj.script is None
        assert obj.reflection is None
        assert obj.catalog is None
