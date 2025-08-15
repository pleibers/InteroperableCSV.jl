  
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
  

