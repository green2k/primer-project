from unittest import TestCase
from unittest.mock import MagicMock

from primer_project.utils import sql_check_entity_name, table_exists


class SqlCheckEntityName(TestCase):
    """
    An example of some trivial tests.
    """
    def test_valid(self):
        sql_check_entity_name("common_metrics")

    def test_invalid_doublequote(self):
        self.assertRaises(
            ValueError,
            sql_check_entity_name,
            ("common_\"metrics"),
        )

    def test_invalid_space(self):
        self.assertRaises(
            ValueError,
            sql_check_entity_name,
            ("common metrics"),
        )

    def test_invalid_backslash(self):
        self.assertRaises(
            ValueError,
            sql_check_entity_name,
            ("common\\metrics"),
        )


class TableExists(TestCase):
    """
    An example of using MagicMock in tests.
    """
    def test_exists(self):
        mock_cursor = MagicMock()
        mock_cursor_result = MagicMock()
        mock_table = MagicMock()

        mock_cursor.execute.return_value = mock_cursor_result
        mock_cursor_result.fetchall.return_value = ["table1"]

        actual = table_exists(mock_cursor, mock_table)
        expected = True

        assert actual == expected

    def test_not_exists(self):
        mock_cursor = MagicMock()
        mock_cursor_result = MagicMock()
        mock_table = MagicMock()

        mock_cursor.execute.return_value = mock_cursor_result
        mock_cursor_result.fetchall.return_value = []

        actual = table_exists(mock_cursor, mock_table)
        expected = False

        assert actual == expected
