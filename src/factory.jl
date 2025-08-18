  """
    read(filename::AbstractString) -> Union{ICSVBase,ICSV2DTimeseries}

Dispatching reader that inspects the first line and calls the appropriate reader.

Selection logic
- If the first line matches [`FIRSTLINES_2DTIMESERIES`](@ref): [`read_icsv_timeseries`](@ref).
- If it matches [`FIRSTLINES`](@ref): [`read_icsv_base`](@ref).
- Otherwise throws `ArgumentError("Not an iCSV file")`.

Returns: [`ICSVBase`](@ref) for standard files or [`ICSV2DTimeseries`](@ref) for the 2D profile.
  """
function read(filename::AbstractString)
    firstline = rstrip(open(readline, filename))
    if firstline in FIRSTLINES_2DTIMESERIES
        return read_icsv_timeseries(filename)
    elseif firstline in FIRSTLINES
        return read_icsv_base(filename)
    else
        throw(ArgumentError("Not an iCSV file"))
    end
end
  

