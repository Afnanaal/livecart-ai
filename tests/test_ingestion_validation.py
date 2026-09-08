from src.ingestion.consumer import validate_event


def test_invalid_price_is_rejected():
    invalid_event = """
    {
        "event_id": "BAD001",
        "event_type": "product_updated",
        "product_id": "P999",
        "product_name": "Laptop",
        "price": "not_a_number",
        "quantity": 2,
        "region": "Riyadh",
        "event_time": "2026-09-08T12:00:00"
    }
    """

    result = validate_event(invalid_event)

    assert result is None


def test_unknown_field_is_rejected():
    invalid_event = """
    {
        "event_id": "BAD002",
        "event_type": "product_updated",
        "product_id": "P999",
        "product_name": "Laptop",
        "price": 3500,
        "quantity": 2,
        "region": "Riyadh",
        "event_time": "2026-09-08T12:00:00",
        "discount_hack": 90
    }
    """

    result = validate_event(invalid_event)

    assert result is None
    
    
if __name__ == "__main__":
    test_invalid_price_is_rejected()
    test_unknown_field_is_rejected()
    print("🎯 All validation tests passed.")    
    