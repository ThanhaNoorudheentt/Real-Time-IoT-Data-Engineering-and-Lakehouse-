# ADR-001: Domain is smart home

**Status:** accepted

**Context:** The platform is domain-agnostic; a concrete domain is needed for schemas, thresholds and demos.

**Decision:** Smart home. Homes contain rooms; rooms contain devices (temperature, humidity, CO2, power, motion, door).

**Alternatives:** flood monitoring, agriculture, industrial equipment.

**Consequences:**
- Mixed sensor types (continuous and binary) exercise validation and windowing differently.
- Natural gold-layer use cases: comfort/air-quality alerts (CO2 > 1000 ppm, humidity), energy usage per home, occupancy from motion, device health.
- Home/room structure gives a natural partition and join dimension; `home_id` is the privacy-sensitive attribute to mask later.
