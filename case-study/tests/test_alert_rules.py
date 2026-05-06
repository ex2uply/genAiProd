"""Test alert rule logic matching pipeline main.py ordering."""


def alert_for_reading(temperature: float, moisture: float, high_temp_threshold: float = 35,
                     low_moisture_threshold: float = 40) -> str:
    """Mirrors main.py alert ordering - temperature checked first."""
    if temperature > high_temp_threshold:
        return "High Temp"
    if moisture < low_moisture_threshold:
        return "Low Moisture"
    return "Normal"


def test_high_temperature_alert_wins_before_low_moisture():
    """When both conditions are met, High Temp should be reported first."""
    assert alert_for_reading(36, 10) == "High Temp"


def test_low_moisture_alert_when_temperature_not_high():
    """When only moisture is low, report Low Moisture."""
    assert alert_for_reading(30, 10) == "Low Moisture"


def test_normal_reading():
    """When neither threshold is breached, report Normal."""
    assert alert_for_reading(30, 50) == "Normal"


def test_custom_thresholds():
    """Test with custom AI-driven thresholds."""
    # Stricter thresholds
    assert alert_for_reading(32, 10, high_temp_threshold=30, low_moisture_threshold=15) == "High Temp"
    assert alert_for_reading(28, 12, high_temp_threshold=30, low_moisture_threshold=15) == "Low Moisture"
    assert alert_for_reading(28, 20, high_temp_threshold=30, low_moisture_threshold=15) == "Normal"
