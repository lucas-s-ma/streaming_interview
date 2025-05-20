from typing import Any, Iterable, Generator, Dict


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    stations: Dict[str, Dict[str, float]] = {}
    latest_timestamp = None

    for line in events:
        type = line['type']
        if type == 'sample':
            try:
                station_name = line['stationName']
                timestamp = line['timestamp']
                temperature = line['temperature']
            except KeyError as e:
                raise ValueError(f"Missing key in the sample message: {e}")

            if station_name not in stations:
                stations[station_name] = {'min': temperature, 'max': temperature}
            else:
                stations[station_name]['min'] = min(stations[station_name]['min'], temperature)
                stations[station_name]['max'] = max(stations[station_name]['max'], temperature)

            latest_timestamp = timestamp

        elif type == 'control':
            try:
                command = line['command']
            except KeyError:
                raise ValueError("Control message missing 'command' key")

            if command == 'snapshot':
                if latest_timestamp is None:
                    continue  

                stations_output = {
                    name: {'high': data['max'], 'low': data['min']} for name, data in stations.items()
                }
                yield {
                    'type': 'snapshot',
                    'asOf': latest_timestamp,
                    'stations': stations_output
                }

            elif command == 'reset':
                if latest_timestamp is None:
                    continue  

                yield {
                    'type': 'reset',
                    'asOf': latest_timestamp
                }

                stations.clear()
                latest_timestamp = None

            else:
                raise ValueError(f"Unknown control command: {command}")

        else:
            raise ValueError(f"Unknown message type: {type}")
