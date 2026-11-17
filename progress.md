# Project progress

| Milestone | Status and evidence |
| --- | --- |
| Finance topic and public repository | Defined the risk-monitoring question and published the project on 18 November 2026. [Repository](https://github.com/shiv-eshwar/half-hour-risk) |
| Sources and empirical design | Verified the official aggregate-trade and minute-bar inventories and field definitions. Fixed periods, label, comparator, feature ablation and selection rules before fitting. [Experiment protocol](plan.md) |
| Acquisition and execution | The full six-month experiment is in progress. Measured counts and results will be added as the stages complete. |

### Verified runs on 18 November 2026

- All 18 publisher checksums passed. The six tape files contain 243,917,258 aggregate trades. Each of the two minute-bar series has 262,080 records. [Source manifest](sources/manifest.json)
- Four regression tests passed, including a future-price shock that changes the target while leaving every input feature identical. [Tests](tests/test_study.py)
- A Structured Streaming replay consumed 314,003 real aggregate prints across a 63-minute input interval. Of these, 2,959 were delivered in a later synthetic file batch. All 60 evaluated minute windows agreed with batch on counts and last price. The largest scaled floating-moment discrepancy was 2.09 × 10⁻¹⁴ or less. The same-checkpoint no-input restart consumed zero new rows. [Query progress](results/streaming.json)
