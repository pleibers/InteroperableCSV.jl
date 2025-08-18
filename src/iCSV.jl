"""
iCSV — Lightweight reader/writer for the iCSV file format

Tools to read and write iCSV files, including the 2DTIMESERIES application profile,
and to convert them to common Julia structures (`DataFrame`, `DimensionalData.DimArray`).

Format overview
- A standard iCSV file begins with a first line like `"# iCSV 1.0 UTF-8"`, then three header
  sections, each introduced by a comment line: `# [METADATA]`, `# [FIELDS]`, `# [DATA]`.
- The 2DTIMESERIES profile begins with `"# iCSV 1.0 UTF-8 2DTIMESERIES"` and, inside the
  `[DATA]` section, groups rows by markers `# [DATE=yyyy-mm-ddTHH:MM:SS]`.
- See `MetaDataSection`, `FieldsSection`, and `Geometry` for header structures.

API overview
- `read` and `write` are the main entry points. See [`read`](@ref), [`write(::ICSVBase, ...)`](@ref),
  [`write(::ICSV2DTimeseries, ...)`](@ref), and [`append_timepoint`](@ref).
- Conversions: [`todataframe`](@ref), [`todimarray`](@ref).

Minimal example
```julia
using iCSV, DataFrames, Dates
md   = MetaDataSection(field_delimiter=",", geometry="POINT(600000 200000)", srid="EPSG:2056")
flds = FieldsSection(fields=["timestamp","a","b"]) 
geom = Geometry(md.geometry, md.srid)
df   = DataFrame(timestamp=[DateTime(2024)], a=[1], b=[2])
f    = ICSVBase(md, flds, geom, df)
write(f, "out.icsv")
g = read("out.icsv")
A = todimarray(g)
```
"""
module iCSV
 
# Write your package code here.
 
 using CSV
 using DataFrames
 using Dates
 using DimensionalData
 
 export ICSVBase, ICSV2DTimeseries
 export MetaDataSection, FieldsSection, Geometry, Loc
 export read, write, append_timepoint
 export todataframe, todimarray
 
 """
 Supported iCSV spec versions recognized by this package.
 """
 const VERSIONS = ["1.0"]

 """
 Recognized first-line markers for standard iCSV files, e.g. `"# iCSV 1.0 UTF-8"`.
 See also: [`FIRSTLINES_2DTIMESERIES`](@ref).
 """
 const FIRSTLINES = ["# iCSV $(v) UTF-8" for v in VERSIONS]

 """
 Recognized first-line markers for iCSV 2DTIMESERIES files, e.g. `"# iCSV 1.0 UTF-8 2DTIMESERIES"`.
 See also: [`FIRSTLINES`](@ref), [`ICSV2DTimeseries`](@ref).
 """
 const FIRSTLINES_2DTIMESERIES = ["# iCSV $(v) UTF-8 2DTIMESERIES" for v in VERSIONS]
 
 include("utility.jl")
 include("header.jl")
 include("base.jl")
 include("timeseries.jl")
 include("factory.jl")
 
end
