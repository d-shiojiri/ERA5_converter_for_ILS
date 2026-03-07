# ERA5 -> ILS Converter

This repository converts ERA5 single-level data into ILS-compatible forcing files using a two-program pipeline.

- Program A (`era5-prep`): converts ERA5 into ILS-readable variables/metadata without changing spatial resolution
- Program B (`ils-resample`, alias: `era5-to-ils`): aligns Program A output to the ILS default 0.5deg grid

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Runner command is controlled by `python_cmd` in:
- `configs/prep_default.yaml`
- `configs/convert_default.yaml`

`python_cmd` accepts both a direct interpreter path and a multi-token wrapper command.
Current default is Singularity:
```yaml
python_cmd: singularity exec /data10/imageshare/shiojiri/pyenv.sif python3
```

If you switch back to `.venv/bin/python` and `.venv` is missing, it automatically falls back to system `python`/`python3`.

## Run A/B Separately (Single Year x Variable)

```bash
./scripts/run_year_var_task.sh --stage prep --year 2000 --var Tair
./scripts/run_year_var_task.sh --stage convert --year 2000 --var Tair
```

## Run Program A -> Program B as One Task

```bash
./scripts/run_year_var_pipeline_task.sh --year 2000 --var Tair
```

## Submit Parallel Jobs

```bash
./scripts/submit_year_var_jobs.sh --stage prep --years 2013-2022 --executor sbatch
./scripts/submit_year_var_pipeline_jobs.sh --years 2013-2022 --executor sbatch
```

`submit_year_var_pipeline_jobs.sh` now skips year-variable pairs whose final Program B output already exists, so only missing outputs are scheduled.

## Parallelism and CPU Allocation

Recommended workflow:
- Edit YAML defaults first (so you do not need to pass many CLI options repeatedly):
  - `configs/submit_default.yaml`
  - `configs/submit_pipeline_default.yaml`
  - `configs/prep_default.yaml`
  - `configs/convert_default.yaml`

Chunk-size recommendation:
- `chunks_time: 168` can be memory-heavy for ERA5 global grids.
- Approximate memory per single variable chunk (`721 x 1440 x chunks_time`):
  - `168`: `~665 MiB` (`float32`) / `~1331 MiB` (`float64`)
  - `24`: `~95 MiB` (`float32`) / `~190 MiB` (`float64`)
  - `12`: `~48 MiB` (`float32`) / `~95 MiB` (`float64`)
  - `6`: `~24 MiB` (`float32`) / `~48 MiB` (`float64`)
- Because derived-variable steps hold multiple arrays at once, practical peak memory can be several times larger than one chunk.
- Program A now casts loaded inputs to `float32` before derived-variable computation to reduce peak memory.
- Default has been set to `chunks_time: 6` in both `prep_default.yaml` and `convert_default.yaml`.
- Dask write compute defaults are memory-bounded via:
  - `dask_num_workers: auto` (uses `SLURM_CPUS_PER_TASK` when available)
  - `dask_scheduler: threads`
- NetCDF write is executed in time blocks to avoid one huge dask graph:
  - `write_time_block: 6` (configurable in `prep_default.yaml` / `convert_default.yaml`)
- NetCDF write chunking is fixed to `time=1` (one timestep per compressed chunk) for both Program A and Program B outputs.
- `Rainf` / `Snowf` can use dedicated tuning keys in both prep/convert configs:
  - `rain_snow_chunks_time`
  - `rain_snow_dask_num_workers`
  - `rain_snow_write_time_block`

- `--max-parallel-jobs N`: max concurrent jobs
  - `--executor local`: local worker count
  - `--executor sbatch`: cap concurrent jobs by dependency chaining (`afterany`)
- `--cpus N`: CPU cores per job when `--executor sbatch` (`sbatch --cpus-per-task`)
- `--mem`: memory per job for `sbatch` (default is omitted via `mem: auto`)
- `--time 02:00:00`: walltime per job for `sbatch`
- `--partition <name>`: Slurm partition for `sbatch`
- Old log cleanup on rerun is enabled by default via `clean_old_logs: true` in
  `configs/submit_default.yaml` and `configs/submit_pipeline_default.yaml`.

Examples:

```bash
# After editing YAML defaults, run with minimal options
./scripts/submit_year_var_pipeline_jobs.sh --years 2000-2001

# Local parallelism: run up to 6 tasks at once
./scripts/submit_year_var_jobs.sh --stage prep --years 2000-2001 --executor local --max-parallel-jobs 6

# Slurm: run up to 9 concurrent jobs, 6 cores/job, 24GB memory, 4 hours
./scripts/submit_year_var_pipeline_jobs.sh --years 2000-2001 --executor sbatch --max-parallel-jobs 9 --cpus 6 --mem 24G --time 04:00:00
```

## Rebuild Incomplete Outputs

```bash
./scripts/cleanup_incomplete_tmp_files.sh
./scripts/submit_year_var_pipeline_jobs.sh --years 2013-2022 --executor sbatch
```

- `cleanup_incomplete_tmp_files.sh` is delete-only (`*.tmp*` cleanup).
- After cleanup, run `submit_year_var_pipeline_jobs.sh`; it schedules only missing Program B outputs.
- Each scheduled pipeline task also skips Program A when the canonical file already exists.

## Supported Target Variables

`Tair`, `Qair`, `PSurf`, `Wind`, `SWdown`, `LWdown`, `Precip`, `Rainf`, `Snowf`, `CCover`
