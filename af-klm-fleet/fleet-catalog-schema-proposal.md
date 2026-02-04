# Open Source Airline Fleet Catalog - Schema Proposal

> **Author:** Clément Wehrung  
> **Date:** February 4, 2026  
> **Status:** Draft for Discussion  
> **Implementation:** See `fleet-catalog/` directory

## Overview

This document proposes a standardized JSON schema for an open source catalog of airline fleets. The goal is to track aircraft properties (WiFi, cabin configuration, IFE, etc.) across multiple airlines with a consistent format and change history.

## Design Principles

1. **One JSON file per airline** - Easy to maintain, review PRs, and avoid merge conflicts
2. **Standardized enums** - Consistent values across all airlines (e.g., WiFi types)
3. **History tracking** - Record property changes over time with timestamps
4. **Extensible** - Room for airline-specific fields without breaking the schema
5. **Machine-readable** - JSON Schema validation for data quality

## Current Implementation

The schema has been implemented with Air France data exported from the fleet database:
- **220 aircraft** with full property data
- **History tracking** for WiFi upgrades, seat config changes, etc.
- **ICAO24 hex codes** for ADS-B tracking correlation

---

## Proposed Directory Structure

```
fleet-catalog/
├── schema/
│   └── aircraft.schema.json      # JSON Schema for validation
├── airlines/
│   ├── AF.json                   # Air France
│   ├── BA.json                   # British Airways
│   ├── DL.json                   # Delta
│   ├── LH.json                   # Lufthansa
│   └── ...
├── reference/
│   ├── aircraft-types.json       # ICAO/IATA aircraft type codes
│   ├── wifi-providers.json       # Known WiFi providers & capabilities
│   └── cabin-class-codes.json    # Cabin class code mappings
└── README.md
```

---

## Schema Definition

### Root Object (Airline File)

```json
{
  "schema_version": "1.0.0",
  "airline": {
    "iata_code": "AF",
    "icao_code": "AFR",
    "name": "Air France",
    "country": "FR"
  },
  "generated_at": "2026-02-04T18:32:20.803Z",
  "aircraft": [...]
}
```

### Aircraft Object

```json
{
  "registration": "FHPND",
  "icao24": "39bda3",
  
  "aircraft_type": {
    "iata_code": "223",
    "icao_code": "A223",
    "manufacturer": "Airbus",
    "model": "A220",
    "variant": "300",
    "full_name": "AIRBUS A220-300 PASSENGER"
  },

  "operator": {
    "sub_fleet_code": "CA",
    "cabin_crew_employer": "AF",
    "cockpit_crew_employer": "AF"
  },

  "cabin": {
    "physical_configuration": "Y148",
    "operational_configuration": "C008Y135",
    "saleable_configuration": null,
    "total_seats": 148,
    "classes": {
      "first": 0,
      "business": 0,
      "premium_economy": 0,
      "economy": 148
    },
    "freight_configuration": "PP000LL000"
  },

  "connectivity": {
    "wifi": "high-speed",
    "wifi_provider": "Starlink",
    "satellite": true,
    "live_tv": false,
    "power_outlets": true,
    "usb_ports": true
  },

  "ife": {
    "type": "streaming",
    "personal_screens": false
  },

  "status": "active",

  "tracking": {
    "first_seen": "2025-12-20",
    "last_seen": "2026-02-05",
    "total_flights": 3214
  },

  "metadata": {
    "delivery_date": null,
    "msn": null,
    "line_number": null,
    "production_site": null,
    "engine_type": null,
    "aircraft_name": null,
    "livery": null,
    "comments": null
  },

  "history": [...]
}
```

---

## Standardized Enums

### `connectivity.wifi`

| Value | Description | Examples |
|-------|-------------|----------|
| `"none"` | No WiFi available | — |
| `"low-speed"` | Basic WiFi, typically < 10 Mbps | Gogo ATG, old Ku-band systems |
| `"high-speed"` | Fast WiFi, typically > 50 Mbps | Starlink, Viasat Ka-band, Gogo 2Ku |

### `connectivity.wifi_provider`

Suggested standardized provider names:

| Provider | Notes |
|----------|-------|
| `"Starlink"` | SpaceX LEO constellation |
| `"Viasat"` | Ka-band GEO satellites |
| `"Gogo 2Ku"` | Dual Ku-band antennas |
| `"Gogo ATG"` | Air-to-ground (US only) |
| `"Panasonic Ku"` | Ku-band system |
| `"Inmarsat GX"` | Global Xpress Ka-band |
| `"Anuvu"` | Formerly Global Eagle |

### `ife.type`

| Value | Description |
|-------|-------------|
| `"none"` | No IFE system |
| `"overhead"` | Shared overhead screens only |
| `"seatback"` | Personal seatback screens |
| `"streaming"` | BYOD streaming to personal devices |
| `"hybrid"` | Both seatback screens and streaming |

### `status`

| Value | Description |
|-------|-------------|
| `"active"` | Currently in service |
| `"stored"` | Temporarily stored/parked |
| `"maintenance"` | In heavy maintenance |
| `"retired"` | Permanently removed from fleet |

### Cabin Class Codes

Standard codes used in `configuration_raw`:

| Code | Class | Notes |
|------|-------|-------|
| `F` | First Class | Traditional first |
| `P` | First Class | Premium first (e.g., La Première) |
| `J` | Business Cla ss | Standard code |
| `C` | Business Class | Alternative code |
| `W` | Premium Economy | |
| `Y` | Economy | |

---

## History Tracking

Each time a property changes, append an entry to the `history` array:

```json
{
  "history": [
    {
      "timestamp": "2026-01-15T14:30:00.000Z",
      "property": "connectivity.wifi",
      "old_value": "low-speed",
      "new_value": "high-speed",
      "source": "flight_api"
    },
    {
      "timestamp": "2026-01-15T14:30:00.000Z",
      "property": "connectivity.wifi_provider",
      "old_value": "Gogo",
      "new_value": "Starlink",
      "source": "flight_api"
    },
    {
      "timestamp": "2025-06-01T00:00:00.000Z",
      "property": "cabin.configuration_raw",
      "old_value": "Y146",
      "new_value": "Y148",
      "source": "manual"
    }
  ]
}
```

### History Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | ISO 8601 | When the change was detected |
| `property` | string | Dot-notation path to the changed field |
| `old_value` | any | Previous value (or `null` if new) |
| `new_value` | any | New value |
| `source` | string | How the change was detected |

### Source Values

| Value | Description |
|-------|-------------|
| `"flight_api"` | Detected via flight data API |
| `"airline_api"` | From airline's official API |
| `"manual"` | Manual update/correction |
| `"seatguru"` | SeatGuru or similar source |
| `"community"` | Community contribution |

---

## Example: Air France A220-300

```json
{
  "registration": "FHPND",
  
  "aircraft_type": {
    "icao_code": "A223",
    "iata_code": "223",
    "manufacturer": "Airbus",
    "model": "A220-300",
    "variant": null
  },

  "cabin": {
    "configuration_raw": "Y148",
    "total_seats": 148,
    "classes": {
      "first": 0,
      "business": 0,
      "premium_economy": 0,
      "economy": 148
    }
  },

  "connectivity": {
    "wifi": "high-speed",
    "wifi_provider": "Starlink",
    "live_tv": false,
    "power_outlets": true,
    "usb_ports": true
  },

  "ife": {
    "type": "streaming",
    "personal_screens": false
  },

  "status": "active",

  "tracking": {
    "first_seen": "2025-12-20",
    "last_seen": "2026-02-05",
    "total_flights": 3214
  },

  "history": [
    {
      "timestamp": "2026-01-15T14:30:00.000Z",
      "property": "connectivity.wifi",
      "old_value": "low-speed",
      "new_value": "high-speed",
      "source": "flight_api"
    }
  ]
}
```

---

## Example: Air France 777-300ER (Multi-Class)

```json
{
  "registration": "FGSQA",
  
  "aircraft_type": {
    "icao_code": "B77W",
    "iata_code": "77W",
    "manufacturer": "Boeing",
    "model": "777-300ER",
    "variant": null
  },

  "cabin": {
    "configuration_raw": "P004J058W028Y206",
    "total_seats": 296,
    "classes": {
      "first": 4,
      "business": 58,
      "premium_economy": 28,
      "economy": 206
    }
  },

  "connectivity": {
    "wifi": "high-speed",
    "wifi_provider": "Starlink",
    "live_tv": true,
    "power_outlets": true,
    "usb_ports": true
  },

  "ife": {
    "type": "seatback",
    "personal_screens": true
  },

  "status": "active",

  "tracking": {
    "first_seen": "2025-12-20",
    "last_seen": "2026-02-05",
    "total_flights": 1137
  },

  "history": []
}
```

---

## Migration from Current Format

For existing data (e.g., Air France tracking), here's the field mapping:

| Current Field | New Path | Transformation |
|--------------|----------|----------------|
| `registration` | `registration` | Keep as-is (no dash) |
| `type_code` | `aircraft_type.iata_code` | Direct mapping |
| `type_name` | `aircraft_type.*` | Parse into manufacturer/model |
| `owner_airline_code` | Top-level `airline.iata_code` | Move to file level |
| `owner_airline_name` | Top-level `airline.name` | Move to file level |
| `wifi_enabled` | `connectivity.wifi` | Combine with `high_speed_wifi` |
| `high_speed_wifi` | `connectivity.wifi` | `Y` → `"high-speed"`, else `"low-speed"` |
| `physical_pax_configuration` | `cabin.configuration_raw` | Direct mapping |
| — | `cabin.classes` | Parse from configuration |
| `first_seen_date` | `tracking.first_seen` | Direct mapping |
| `last_seen_date` | `tracking.last_seen` | Direct mapping |
| `total_flights_tracked` | `tracking.total_flights` | Direct mapping |

### WiFi Conversion Logic

```javascript
function convertWifi(wifi_enabled, high_speed_wifi) {
  if (wifi_enabled !== 'Y') return 'none';
  if (high_speed_wifi === 'Y') return 'high-speed';
  return 'low-speed';
}
```

### Cabin Configuration Parser

```javascript
function parseCabinConfig(config) {
  // "P004J058W028Y206" → { first: 4, business: 58, premium_economy: 28, economy: 206 }
  const mapping = { P: 'first', F: 'first', J: 'business', C: 'business', W: 'premium_economy', Y: 'economy' };
  const classes = { first: 0, business: 0, premium_economy: 0, economy: 0 };
  const regex = /([PFJCWY])(\d{3})/g;
  let match;
  while ((match = regex.exec(config)) !== null) {
    const classKey = mapping[match[1]];
    classes[classKey] += parseInt(match[2], 10);
  }
  return classes;
}
```

---

## Metadata Fields (for PlaneSpotters-style data)

These fields capture additional data often found on PlaneSpotters.net:

```json
{
  "metadata": {
    "delivery_date": "2022-03-15",
    "msn": "55012",
    "line_number": "1234",
    "production_site": "Mirabel",
    "engine_type": "PW1500G",
    "aircraft_name": "Fort-de-France",
    "livery": "standard",
    "comments": "Olympic Games 2024 special livery"
  }
}
```

### Metadata Field Descriptions

| Field | Description | Example |
|-------|-------------|---------|
| `delivery_date` | Date aircraft was delivered to airline | `2022-03-15` |
| `msn` | Manufacturer Serial Number | `55012` |
| `line_number` | Production line number | `1234` |
| `production_site` | Factory location | `Toulouse`, `Hamburg`, `Mirabel`, `Charleston` |
| `engine_type` | Engine model | `Trent XWB-84`, `GE90-115B`, `PW1500G` |
| `aircraft_name` | Given name (if any) | `Fort-de-France`, `Château de Versailles` |
| `livery` | Special paint scheme | `standard`, `SkyTeam`, `Olympic 2024` |
| `comments` | Additional notes | Free text |

### Production Sites Reference

| Manufacturer | Sites |
|--------------|-------|
| Airbus | Toulouse (France), Hamburg (Germany), Tianjin (China), Mobile (USA) |
| Boeing | Everett (USA), Renton (USA), Charleston (USA) |
| Airbus Canada | Mirabel (Canada) |
| Embraer | São José dos Campos (Brazil) |

---

## Validation

A JSON Schema file should be maintained at `schema/aircraft.schema.json` for:
- CI validation on PRs
- Editor autocomplete
- Documentation generation

---

## Open Questions

1. **Registration format:** ✅ Decided: Strip dashes (`FHPND` not `F-HPND`)
2. **ICAO24 hex codes:** ✅ Decided: Yes, include for ADS-B correlation
3. **Frequency of updates:** Real-time vs. daily snapshots?
4. **Historical snapshots:** Keep full point-in-time snapshots or just deltas?
5. **API access:** Should we provide a read-only API for querying?
6. **PlaneSpotters integration:** How to merge MSN, delivery dates, aircraft names?

---

## Implementation Status

- [x] Finalize schema based on feedback
- [x] Create JSON Schema for validation (`schema/aircraft.schema.json`)
- [x] Migrate Air France data to new format (`airlines/AF.json`)
- [x] Set up repo structure
- [x] Document contribution guidelines (`README.md`)
- [ ] Add CI for schema validation
- [ ] Add more airlines (KLM, Delta, etc.)
- [ ] Integrate PlaneSpotters metadata (MSN, delivery dates, names)

