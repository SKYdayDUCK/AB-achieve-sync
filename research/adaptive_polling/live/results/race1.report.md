# Live race report (race1.json)

A-clock latency D = snapshot of first successful scan listing the batch - ready_at (B return leg not included, same for all). Cost J_c = (c * scans + sum D) / 60 with c in seconds-of-latency per query (lower is better).

## Overall

| policy | batches found | scans | failed | mean D s | P95 D s | max D s | J(c=3) | J(c=10) | J(c=30) |
|---|---|---|---|---|---|---|---|---|---|
| hazard(c) c=10 | 42/42 | 40 | 0 | 6.67 | 13.97 | 25.00 | 6.7 | 11.3 | 24.7 |
| gp_sqrt(a') c=10 | 42/42 | 30 | 0 | 9.09 | 19.85 | 20.40 | 7.9 | 11.4 | 21.4 |
| fixed_15s | 42/42 | 32 | 0 | 9.67 | 16.09 | 17.41 | 8.4 | 12.1 | 22.8 |
| fixed_10s | 42/42 | 44 | 0 | 7.08 | 13.99 | 15.50 | 7.2 | 12.3 | 27.0 |
| hazard(c) c=3 | 42/42 | 56 | 0 | 4.34 | 9.15 | 16.31 | 5.8 | 12.4 | 31.0 |
| fixed_20s | 42/42 | 26 | 0 | 12.12 | 22.40 | 24.80 | 9.8 | 12.8 | 21.5 |
| gp_sqrt(a') c=3 | 42/42 | 48 | 0 | 6.98 | 13.15 | 16.10 | 7.3 | 12.9 | 28.9 |
| gamma_poisson(a) pf=.5 | 42/42 | 57 | 0 | 6.36 | 10.20 | 15.90 | 7.3 | 14.0 | 33.0 |
| fixed_5s | 42/42 | 69 | 0 | 4.46 | 7.37 | 8.47 | 6.6 | 14.6 | 37.6 |
| fixed_30s | 42/42 | 19 | 0 | 16.50 | 30.88 | 31.80 | 12.5 | 14.7 | 21.0 |
| renewal(b) k=.3 | 42/42 | 76 | 0 | 4.48 | 7.85 | 8.46 | 6.9 | 15.8 | 41.1 |

- best by J(c=3): **hazard(c) c=3**

- best by J(c=10): **hazard(c) c=10**

- best by J(c=30): **fixed_30s**

## P1 10s  (t=0-180s)

| policy | batches | scans | mean poll interval T s | mean D s | P95 D s |
|---|---|---|---|---|---|
| fixed_5s | 18 | 19 | 5.0 | 5.26 | 8.33 |
| renewal(b) k=.3 | 18 | 23 | 3.7 | 5.60 | 8.32 |
| hazard(c) c=3 | 18 | 17 | 5.9 | 5.63 | 10.84 |
| gamma_poisson(a) pf=.5 | 18 | 18 | 5.6 | 7.34 | 11.05 |
| gp_sqrt(a') c=3 | 18 | 14 | 8.1 | 7.76 | 13.98 |
| fixed_10s | 18 | 12 | 10.0 | 8.65 | 15.41 |
| hazard(c) c=10 | 18 | 14 | 8.7 | 9.08 | 17.91 |
| fixed_15s | 18 | 10 | 15.0 | 9.99 | 16.29 |
| gp_sqrt(a') c=10 | 18 | 9 | 15.9 | 10.92 | 19.09 |
| fixed_20s | 18 | 8 | 20.0 | 12.52 | 22.50 |
| fixed_30s | 18 | 6 | 30.0 | 17.31 | 31.05 |

## P2 20s  (t=180-420s)

| policy | batches | scans | mean poll interval T s | mean D s | P95 D s |
|---|---|---|---|---|---|
| hazard(c) c=10 | 12 | 15 | 13.7 | 3.69 | 8.69 |
| hazard(c) c=3 | 12 | 26 | 5.9 | 3.85 | 6.04 |
| renewal(b) k=.3 | 12 | 32 | 4.5 | 4.42 | 6.98 |
| gp_sqrt(a') c=10 | 12 | 12 | 16.8 | 5.27 | 13.75 |
| fixed_5s | 12 | 28 | 5.0 | 5.43 | 7.14 |
| gamma_poisson(a) pf=.5 | 12 | 24 | 7.2 | 5.84 | 9.83 |
| gp_sqrt(a') c=3 | 12 | 19 | 9.2 | 6.37 | 12.19 |
| fixed_10s | 12 | 18 | 10.0 | 6.46 | 10.83 |
| fixed_15s | 12 | 12 | 15.0 | 9.26 | 15.88 |
| fixed_20s | 12 | 10 | 20.0 | 12.48 | 23.40 |
| fixed_30s | 12 | 7 | 30.0 | 13.84 | 27.48 |

## P3 15s  (t=420-600s)

| policy | batches | scans | mean poll interval T s | mean D s | P95 D s |
|---|---|---|---|---|---|
| fixed_5s | 12 | 22 | 5.0 | 2.28 | 5.01 |
| renewal(b) k=.3 | 12 | 21 | 4.9 | 2.87 | 4.52 |
| hazard(c) c=3 | 12 | 13 | 8.8 | 2.90 | 8.80 |
| fixed_10s | 12 | 14 | 10.0 | 5.34 | 9.18 |
| gamma_poisson(a) pf=.5 | 12 | 15 | 8.0 | 5.41 | 9.59 |
| hazard(c) c=10 | 12 | 11 | 12.3 | 6.03 | 8.73 |
| gp_sqrt(a') c=3 | 12 | 15 | 9.8 | 6.43 | 12.12 |
| fixed_15s | 12 | 10 | 15.0 | 9.61 | 16.25 |
| gp_sqrt(a') c=10 | 12 | 9 | 18.0 | 10.16 | 19.05 |
| fixed_20s | 12 | 7 | 20.0 | 11.17 | 20.04 |
| fixed_30s | 12 | 5 | 30.0 | 17.93 | 30.27 |

## Learning after a rate change (mean D of first 3 vs last 3 batches of the phase)

| policy | P2 first3 | P2 last3 | P3 first3 | P3 last3 |
|---|---|---|---|---|
| fixed_5s | 5.57 | 5.77 | 3.41 | 0.60 |
| fixed_10s | 5.67 | 5.40 | 6.96 | 7.14 |
| fixed_15s | 0.74 | 7.63 | 8.46 | 10.74 |
| fixed_20s | 10.01 | 11.19 | 10.12 | 10.41 |
| fixed_30s | 15.11 | 13.52 | 16.32 | 23.20 |
| gamma_poisson(a) pf=.5 | 3.04 | 7.50 | 5.49 | 5.87 |
| gp_sqrt(a') c=3 | 5.81 | 6.49 | 7.14 | 5.61 |
| gp_sqrt(a') c=10 | 5.78 | 5.27 | 14.27 | 6.13 |
| renewal(b) k=.3 | 4.60 | 3.70 | 2.92 | 3.25 |
| hazard(c) c=3 | 5.63 | 2.95 | 3.65 | 1.27 |
| hazard(c) c=10 | 6.40 | 3.16 | 5.49 | 5.54 |