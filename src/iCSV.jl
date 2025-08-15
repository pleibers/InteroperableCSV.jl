module iCSV
 
# Write your package code here.
 
 using CSV
 using DataFrames
 using Dates
 using DimensionalData
 
 export ICSVBase, ICSV2DTimeseries
 export MetaDataSection, FieldsSection, Geometry, Loc
 export readicsv, writeicsv, append_timepoint
 export todataframe, todimarray
 
 const VERSIONS = ["1.0"]
 const FIRSTLINES = ["# iCSV $(v) UTF-8" for v in VERSIONS]
 const FIRSTLINES_2DTIMESERIES = ["# iCSV $(v) UTF-8 2DTIMESERIES" for v in VERSIONS]
 
 include("utility.jl")
 include("header.jl")
 include("base.jl")
 include("timeseries.jl")
 include("read.jl")
 include("factory.jl")
 
end
