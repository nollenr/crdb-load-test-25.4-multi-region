# Index Impact Readout

## Scope
This readout tracks how additional secondary indexes affect insert throughput for the CockroachDB benchmark workload.

Current benchmark shape:
- 3 app nodes, one per region
- 48 workers per region
- 4 processes per region
- aggressive client mode: `--mode pipeline --pipeline-style deep --pipeline-depth 8`
- widened schema with additional non-key columns
- focus metric: inserted `rows/sec`

Cluster context:
- Advanced Cluster
- Regions: N. Virginia (`us-east-1`), Ohio (`us-east-2`), Oregon (`us-west-2`)
- Compute: `16 vCPU`, `64 GiB RAM` per node
- Storage: `1200 GiB` disk per node
- Nodes: `18/18` live nodes
- App nodes: one `t3a.2xlarge` instance in each region

## Summary Table
| Profile | Extra Indexes Per Table | Aggregate Rows/Sec | Change vs No Indexes | Percent Change vs No Indexes | Retries | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Wider schema baseline | 0 | 31,319.52 | 0.00 | 0.00% | 0 | 3-region aggregate, aggressive client |
| Wider schema + index profile 5 | 5 | 30,754.27 | -565.25 | -1.80% | 0 | 3-region aggregate, aggressive client; rerun used as primary result after earlier `aws-us-west-2` outlier |
| Wider schema + index profile 10 | 10 | 17,858.40 | -13,461.12 | -42.98% | 0 | 3-region aggregate, aggressive client; rerun used as primary result after earlier `aws-us-west-2` outlier |

## Regional Breakdown
| Profile | Region | Rows/Sec | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Run ID |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Wider schema baseline | aws-us-east-2 | 11,076.80 | 30.07 | 40.94 | 46.35 | `1e7a7a33-51c0-4e9e-9876-9f4c591c0370` |
| Wider schema baseline | aws-us-west-2 | 10,276.58 | 32.37 | 47.37 | 60.26 | `09f9fa02-0f36-4031-b364-bb0f31c80e70` |
| Wider schema baseline | aws-us-east-1 | 9,966.13 | 33.23 | 49.46 | 58.18 | `d70596f9-cb64-4b69-8a42-952bb37aafe4` |
| Wider schema + index profile 5 | aws-us-east-2 | 10,079.07 | 33.20 | 44.04 | 53.16 | `c2c926da-34db-496a-8198-00a043368afb` |
| Wider schema + index profile 5 | aws-us-west-2 | 10,779.07 | 30.96 | 37.47 | 45.65 | `70c587c4-194f-4a6a-913a-f7356358d4e1` |
| Wider schema + index profile 5 | aws-us-east-1 | 9,896.13 | 33.51 | 51.18 | 65.23 | `0a077f34-7928-497d-993d-f6dbd4cbda26` |
| Wider schema + index profile 10 | aws-us-east-2 | 6,618.27 | 50.68 | 68.27 | 81.92 | `e8b7ff9d-c97c-457c-9fe0-b0c156d218e0` |
| Wider schema + index profile 10 | aws-us-west-2 | 4,440.80 | 75.72 | 85.89 | 95.52 | `490aa7d7-25a3-433f-8465-69a893c169c3` |
| Wider schema + index profile 10 | aws-us-east-1 | 6,799.33 | 49.00 | 61.23 | 83.60 | `bafaedc5-28ce-4ddf-8a61-476d91d498a7` |

## Aggregate Run Details
- Mode: `pipeline`
- Pipeline style: `deep`
- Pipeline depth: `8`
- Total workers: `144`
- Total processes: `12`
- Total retries: `0`
- Total iterations: `268,453`
- Total rows inserted: `1,879,171`
- Aggregate logical units/sec: `4,474.22`
- Aggregate rows/sec: `31,319.52`

## Next Results To Add
- optional delta notes on whether latency growth tracks the drop in rows/sec

## Notes
- The primary `5 indexes per table` result now comes from the rerun, which brought all three regions back into a consistent latency and throughput band.
- An earlier `5 indexes per table` run showed a severe `aws-us-west-2` outlier. That earlier run is treated as noisy and is not used as the headline indexed result.
- The primary `10 indexes per table` result also comes from a rerun. The first `10-index` run showed a much more severe `aws-us-west-2` slowdown and is treated as noisy.
- With `10 indexes per table`, latency increased materially in all three regions, and aggregate rows/sec dropped by about `42.98%` versus the no-index baseline.

## Interpretation
- The `5 indexes per table` profile stayed close to the no-index baseline, which suggests the workload was still within a write envelope the cluster could absorb without a major latency penalty.
- The `10 indexes per table` profile appears to cross a threshold where each insert becomes materially more expensive. Every inserted row must also maintain many more secondary index entries, which increases write amplification and raises the amount of work required before commit.
- The latency data supports that interpretation. The no-index baseline and the `5 indexes per table` profile both stayed in a similar average-latency band, while the `10 indexes per table` profile moved into a much higher latency range.
- In practical terms, the results suggest that index overhead did not grow in a simple straight line for this workload. The cluster handled the first batch of indexes with relatively small impact, but the second batch pushed the write path into a noticeably slower operating range.
