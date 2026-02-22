"""Shared test fixtures for filemaker-mcp tests.

Populates EXPOSED_TABLES and TABLES with sample data so tests can run
without a live FileMaker connection. These mirror a generic FM database
structure for testing purposes.
"""

import pytest


@pytest.fixture(autouse=True)
def _populate_test_tables():
    """Ensure EXPOSED_TABLES and TABLES have sample data for all tests."""
    from filemaker_mcp.ddl import TABLES
    from filemaker_mcp.tools.query import EXPOSED_TABLES

    # Sample tables for testing
    test_tables = {
        "Customers": "Customer records.",
        "Invoices": "Service invoices.",
        "LineItems": "Invoice line items.",
        "Orders": "Orders.",
        "Drivers": "Service drivers.",
    }

    # Sample DDL for testing
    test_ddl = {
        "Customers": {
            "CustomerID": {"type": "number", "tier": "key", "pk": True},
            "Company Name": {"type": "text", "tier": "key"},
            "City": {"type": "text", "tier": "key"},
            "State": {"type": "text", "tier": "standard"},
            "Phone": {"type": "text", "tier": "key"},
            "Email": {"type": "text", "tier": "standard"},
        },
        "Invoices": {
            "PrimaryKey": {"type": "text", "tier": "key", "pk": True},
            "Amount": {"type": "number", "tier": "key"},
            "ServiceDate": {"type": "datetime", "tier": "key"},
            "Region": {"type": "text", "tier": "key"},
            "Technician": {"type": "text", "tier": "standard"},
            "City": {"type": "text", "tier": "key"},
            "Name": {"type": "text", "tier": "key"},
        },
        "LineItems": {
            "PrimaryKey": {"type": "text", "tier": "key", "pk": True},
        },
        "Orders": {
            "PrimaryKey": {"type": "text", "tier": "key", "pk": True},
        },
        "Drivers": {
            "Driver_ID": {"type": "number", "tier": "key", "pk": True},
            "Driver_Name": {"type": "text", "tier": "key"},
        },
    }

    # Populate and restore
    old_exposed = dict(EXPOSED_TABLES)
    old_tables = dict(TABLES)
    EXPOSED_TABLES.update(test_tables)
    TABLES.update(test_ddl)
    yield
    EXPOSED_TABLES.clear()
    EXPOSED_TABLES.update(old_exposed)
    TABLES.clear()
    TABLES.update(old_tables)
