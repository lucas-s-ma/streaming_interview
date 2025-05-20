from . import weather

def test_snapshot():
    input_events = [
        {
            "type": "sample",
            "stationName": "Foster Weather Station",
            "timestamp": 1672531200000,
            "temperature": 37.1
        },
        {
            "type": "sample",
            "stationName": "Duke University",
            "timestamp": 1672531200001,
            "temperature": 36.5
        },
        {
            "type": "sample",
            "stationName": "Foster Weather Station",
            "timestamp": 1672531200002,
            "temperature": 37.5
        },
        {
            "type": "sample",
            "stationName": "Duke University",
            "timestamp": 1672531200004,
            "temperature": 38.5
        },
        {
            "type": "sample",
            "stationName": "Duke University",
            "timestamp": 1672531200009,
            "temperature": 90.0
        },
        {"type": "control", "command": "snapshot"}
    ]
    output = list(weather.process_events(input_events))
    assert output == [{
        "type": "snapshot",
        "asOf": 1672531200009,
        "stations": {
            "Foster Weather Station": {"high": 37.5, "low": 37.1},
            "Duke University": {"high": 90.0, "low": 36.5}
        }
    }]
