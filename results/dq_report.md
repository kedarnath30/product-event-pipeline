# Data-quality report (synthetic stream)

- Messages received: **54,230**; accepted 53,484; quarantined 746 (**1.38%**); accepted-with-warning 325
- Raw duplicate deliveries: 0.59% (removed in dbt staging)
- Events arriving > 1h late: 1.96%
- Completeness: home_utm_coverage = 0.911, identify_coverage = 0.9914, key_null_rate = 0.0, equity_null_rate = 0.0

## Violations by code

| code | severity | count |
|---|---|---|
| unexpected_property | warn | 329 |
| missing_required | block | 203 |
| invalid_enum | block | 170 |
| wrong_type | block | 136 |
| missing_identity | block | 82 |
| invalid_timestamp | block | 63 |
| future_timestamp | block | 55 |
| naming_convention | block | 27 |
| unplanned_event | block | 12 |
| pii_detected | block | 3 |

## Alerts (11)

| severity | check | when | message |
|---|---|---|---|
| critical | privacy | 2026-09-06 15:29 | PII-like key 'traits.email' sent 3x; events quarantined, not loaded |
| critical | schema_drift | 2026-09-19 18:05 | 'Estimate Completed' from 3.4.0: properties.equity_requested has the wrong type (113x, e.g. '100000' is not of type 'number') |
| critical | validity | 2026-09-19 to 2026-09-24 | 'Estimate Completed' from 3.4.0: 113 of 113 rejected across 6 day(s) |
| critical | volume | 2026-09-17 14:00 to 21:00 UTC | analytics.js: volume down 89% for 7h (~453 events missing) - likely SDK/collector outage |
| warning | schema_drift | 2026-09-19 18:05 | 'Estimate Completed' from 3.4.0 sends unplanned 'properties.credit_score_band' (113x since 2026-09-19 18:05); add it to the tracking plan or fix the client |
| warning | schema_drift | 2026-09-24 18:01 | 'Estimate Completed' from 3.4.1 sends unplanned 'properties.credit_score_band' (216x since 2026-09-24 18:01); add it to the tracking plan or fix the client |
| warning | schema_drift | whole window | 4 event names not in the tracking plan (quarantined): 'Submit Application Btn' (12x), 'clicked_offer' (11x), 'estimate_started' (9x), 'OfferViewed' (7x) |
| warning | validity | 2026-09-20 | 49 of 1,647 messages quarantined (2.98%) |
| warning | validity | 2026-09-21 | 48 of 2,038 messages quarantined (2.36%) |
| warning | validity | 2026-09-22 | 45 of 2,024 messages quarantined (2.22%) |
| warning | validity | 2026-09-23 | 44 of 1,856 messages quarantined (2.37%) |
