# pf_random package

Package for generating **correlated Gaussian AR(1)** random sequences
and applying perturbation-model transforms.

## Main API

- `validate_correlation_matrix(corr, expected_dim=None)`
- `ar1_phi(interval_seconds, period_hours)`
- `build_time_axis(start, interval_seconds, n_steps)`
- `generate_correlated_ar1_series(...)`
- `CorrelatedAR1Gaussian` (stateful step-by-step generator)
- `get_default_process(...)`
- `reset_default_process(seed=None)`
- `reset_process(process, seed=None)`
- `clear_default_process()`
- `apply_lognormal(..., mode=\"multiplicative\", value_cap=...)`
- `apply_trunc_gauss(..., mode=\"multiplicative\"|\"additive\")`
- `apply_clip_gauss(..., mode=\"multiplicative\"|\"additive\")`
- `model_apply(z, model, mode, bounds, value_cap, std_norm)`
  `value_cap` is required for `lognormal`, and may be `None` otherwise.

## Minimal iterative example

```python
import numpy as np
from pf_random import generate_correlated_ar1_series, model_apply, reset_default_process

corr = np.array(
    [
        [1.0, -0.8, 0.5],
        [-0.8, 1.0, -0.5],
        [0.5, -0.5, 1.0],
    ],
    dtype=np.float64,
)

variables = ("Precip", "SWdown", "LWdown")
std_norm = {"Precip": 0.50, "SWdown": 0.30, "LWdown": 50.0}
variable_settings = {
    "Precip": {
        "model": "lognormal",
        "mode": "multiplicative",
        "bounds": None,
        "value_cap": 2.5,
    },
    "SWdown": {
        "model": "lognormal",
        "mode": "multiplicative",
        "bounds": None,
        "value_cap": 2.5,
    },
    "LWdown": {
        "model": "trunc-gauss",
        "mode": "additive",
        "bounds": (-3.0, 3.0),  # per-variable bound example
        "value_cap": None,
    },
}

reset_default_process(seed=100)  # optional: explicitly start from known initial state

for chunk_idx in range(10):
    z_chunk = generate_correlated_ar1_series(
        n_steps=144,
        n_ens=20,
        corr=corr,
        interval_seconds=1800,
        ar1_period_hours=24.0,
        seed=100,  # same settings -> same cached process is reused
    )

    # Explicit model selection + additive/multiplicative selection
    factors = {}
    for idx, var in enumerate(variables):
        config = variable_settings[var]
        model = config["model"]
        mode = config["mode"]
        bounds = config["bounds"]
        value_cap = config["value_cap"]
        z = z_chunk[:, :, idx] * std_norm[var]
        factors[var] = model_apply(
            z=z,
            model=model,
            mode=mode,
            bounds=bounds,
            value_cap=value_cap,
            std_norm=std_norm[var],
        )

        # if mode == "multiplicative":
        #     perturbed = base_chunk_var * factors[var]
        # else:
        #     perturbed = base_chunk_var + factors[var]
        # save perturbed chunk here
```

## Recommended 3-variable setup

For the common `(lognormal-multiplicative, lognormal-multiplicative, clip-gauss-additive)`
configuration, use:

```python
inflation_type = {
    "Precip": "lognormal-multiplicative",
    "SWdown": "lognormal-multiplicative",
    "LWdown": "clip-gauss-additive",
}
```

These labels are required in explicit `model-operation` form.
Shorthand labels such as `lognormal`, `multiplicative`, `additive`, or `normal-*` are not accepted.

In the current PLUMBER2 workflow, this corresponds to
`DEFAULT_INFLATION` in `merge/perturbed_forcing/perturbed_forcing.py`.

## Alternative model choices

You can switch each variable independently with the following labels:

- `lognormal-multiplicative`
- `trunc-gauss-multiplicative` / `truncated-gauss-multiplicative`
- `trunc-gauss-additive` / `truncated-gauss-additive`
- `clip-gauss-multiplicative` / `clipped-gauss-multiplicative`
- `clip-gauss-additive` / `clipped-gauss-additive`

Typical change candidates from the recommended setup:

- Replace only `LWdown` with `trunc-gauss-additive` or `clip-gauss-additive` to suppress outliers.
- Replace `Precip` and `SWdown` with `clip-gauss-multiplicative` when lognormal tails are not desired.
- Use `trunc-gauss-*` instead of `clip-gauss-*` when you want true truncated-normal remapping.

## Continuity and reset behavior

- `generate_correlated_ar1_series(...)` reuses a cached default process by default,
  so repeated calls continue the same AR(1) sequence.
- To reset explicitly, call `reset_default_process(seed=...)` or
  `clear_default_process()`.
- If you want full explicit control, create `CorrelatedAR1Gaussian(...)` and call
  `sample_step()` / `sample_series()`; reset with `process.reset(seed=...)`.

## One-shot generation (optional)

If you need all steps in one call, you can still do:

```python
import numpy as np
from datetime import datetime
from pf_random import build_time_axis, generate_correlated_ar1_series

corr = np.array(
    [
        [1.0, -0.8, 0.5],
        [-0.8, 1.0, -0.5],
        [0.5, -0.5, 1.0],
    ],
    dtype=np.float64,
)

series = generate_correlated_ar1_series(
    n_steps=48,
    n_ens=20,
    corr=corr,
    interval_seconds=1800,
    ar1_period_hours=24.0,
    seed=100,
)
times = build_time_axis(datetime(2100, 1, 1, 6, 0, 0), 1800, 48)
```

Use `reset_default_process(...)` only when you explicitly want to restart the sequence.
