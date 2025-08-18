# iCSV

[![CI](https://github.com/pleibers/iCSV.jl/actions/workflows/CI.yml/badge.svg)](https://github.com/pleibers/iCSV.jl/actions/workflows/CI.yml) [![codecov](https://codecov.io/gh/pleibers/iCSV.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/pleibers/iCSV.jl) ![Static Badge](https://img.shields.io/badge/License-MIT-blue?link=https%3A%2F%2Fgithub.com%2Fpleibers%2FiCSV.jl%2Fblob%2Fmain%2FLICENSE)

> Lightweight reader/writer for the iCSV file format in Julia, including the 2DTIMESERIES application profile. Converts to DataFrames and DimensionalData arrays.

- Works with standard iCSV files and the 2DTIMESERIES profile
- Converts to `DataFrame` and `DimensionalData.DimArray`
- Simple, explicit header structures: `MetaDataSection`, `FieldsSection`, `Geometry`, `Loc`

Docs: [latest](https://leibersp.github.io/iCSV.jl/dev) (API, guides)

## Installation

```julia
julia> ] add iCSV
```

Requires Julia 1.9+ (see `Project.toml`).

## Format Overview

An iCSV file consists of a first line, followed by three header sections.

- Standard iCSV:
  - First line: `# iCSV 1.0 UTF-8`
  - Sections: `# [METADATA]`, `# [FIELDS]`, `# [DATA]`
- 2DTIMESERIES:
  - First line: `# iCSV 1.0 UTF-8 2DTIMESERIES`
  - Within `[DATA]`, blocks are grouped by `# [DATE=yyyy-mm-ddTHH:MM:SS]`

Header structures in Julia:

- `MetaDataSection` — required keys include `field_delimiter`, `geometry`, `srid`; optional recommended keys such as `station_id`, `nodata`, `timezone`, `doi`, `timestamp_meaning`; arbitrary extra key/value pairs are supported.
- `FieldsSection` — contains the declared `fields` and optional per-column attributes (`units_multiplier`, `units`, `long_name`, `standard_name`, plus any other per-column attributes).
- `Geometry`/`Loc` — store location either as WKT-like `POINT/POINTZ` or reference a column name, plus EPSG SRID.

See docstrings for details: `?MetaDataSection`, `?FieldsSection`, `?Geometry`, `?ICSVBase`, `?ICSV2DTimeseries`.

## Quickstart

### Standard iCSV (single table)

```julia
using iCSV, DataFrames, Dates

md   = MetaDataSection(field_delimiter=",", geometry="POINT(600000 200000)", srid="EPSG:2056")
flds = FieldsSection(fields=["timestamp","a","b"]) 
geom = Geometry(md.geometry, md.srid)
df   = DataFrame(timestamp=[DateTime(2024)], a=[1], b=[2])
f    = ICSVBase(md, flds, geom, df)

write(f, "out.icsv")     # create a standard iCSV file
g = read("out.icsv")     # -> ICSVBase

df2 = todataframe(g)      # copy as DataFrame
A2  = todimarray(g)       # -> DimArray (row, field)
```

Notes:

- Timestamp columns named `time` or `timestamp` are parsed to `DateTime` automatically on read.
- `todimarray(::ICSVBase)` by default drops non-numeric columns and uses `(row, field)` dims.

### 2DTIMESERIES (per-date blocks)

```julia
using iCSV, DataFrames, Dates

d1 = DateTime(2024,1,1,10)
d2 = DateTime(2024,1,2,10)
df1 = DataFrame(layer_index = 1:3, var1 = [1.0,2.0,3.0], var2 = [10.0, 20.0, 30.0])
df2 = DataFrame(layer_index = 1:3, var1 = [1.5,2.5,3.5], var2 = [15.0, 25.0, 35.0])

md   = MetaDataSection(field_delimiter=",", geometry="POINT(600000 200000)", srid="EPSG:2056")
flds = FieldsSection(fields=["layer_index","var1","var2"]) 
geom = Geometry(md.geometry, md.srid)

p = ICSV2DTimeseries(md, flds, geom, [df1, df2], [d1, d2])
write(p, "profile.icsv")     # writes blocks with [DATE=...] markers
q = read("profile.icsv")     # -> ICSV2DTimeseries

df_long = todataframe(q)      # long-form DataFrame with :time column
A3D     = todimarray(q)       # -> DimArray (layer, field, time)

# append a new timepoint to an existing file
d3 = DateTime(2024,1,3,10)
df3 = DataFrame(layer_index = 1:3, var1 = [2.0,3.0,4.0], var2 = [12.0,22.0,32.0])
append_timepoint("profile.icsv", d3, df3; field_delimiter=",")
```

Notes:

- Comments are not allowed inside a `[DATE=...]` block; blank lines are ignored.
- `todimarray(::ICSV2DTimeseries)` defaults to dropping non-numeric columns; layers use `:layer_index` if present, otherwise `1..L`.

## Public API

Types:

- `ICSVBase` — in-memory representation of a standard iCSV file.
- `ICSV2DTimeseries` — in-memory representation of a 2D timeseries iCSV file.
- `MetaDataSection`, `FieldsSection`, `Geometry`, `Loc` — header structures.

I/O:

- `read(filename) -> Union{ICSVBase, ICSV2DTimeseries}` — auto-detects by first line.
- `write(obj, filename)` — writes headers and data for `ICSVBase` or `ICSV2DTimeseries`.
- `append_timepoint(filename, timestamp, df; field_delimiter, date_format)` — append a new date block to an existing 2DTIMESERIES file.

Conversions:

- `todataframe(obj)` — return a `DataFrame` copy. File-level metadata are attached via `DataFrames.metadata!`, and per-column attributes (when lengths match) via `DataFrames.colmetadata!`.
- `todimarray(obj; ...)` — return a `DimensionalData.DimArray`:
  - `ICSVBase`: 2D `(row, field)`;
  - `ICSV2DTimeseries`: 3D `(layer, field, time)`.

## Metadata and Fields

- Required metadata: `field_delimiter`, `geometry`, `srid`.
- Recommended metadata (optional): `station_id`, `nodata`, `timezone`, `doi`, `timestamp_meaning`.
- Per-column attributes (optional): `units_multiplier`, `units`, `long_name`, `standard_name`, plus any custom `Dict{Symbol, Vector{String}}` entries.

On write, empty/omitted optional entries are skipped.

## Geometry

- Construct from strings: `Geometry("POINT(600000 200000)", "EPSG:2056")` or `"POINTZ(x y z)"`.
- Or from values: `Geometry(2056, Loc(x=..., y=..., z=...))` or `Geometry(2056, "column_name")`.

## Integration

- DataFrames.jl — main tabular interface and metadata storage.
- CSV.jl — read/write core.
- DimensionalData.jl — structured arrays for analysis and visualization.

## Development

Run tests:

```julia
julia --project -e 'using Pkg; Pkg.test()'
```

## License

MIT &copy; SLF and contributors
