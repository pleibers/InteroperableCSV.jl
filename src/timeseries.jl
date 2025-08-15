# 2D Timeseries application profile

@kwdef struct ICSV2DTimeseries{M <: MetaDataSection, G <: Geometry}
    metadata::M
    fields::FieldsSection
    geolocation::G
    data::Dict{DateTime,DataFrame}
    dates::Vector{DateTime}
    out_datefmt::DateFormat = dateformat"yyyy-mm-ddTHH:MM:SS"
end

function Base.show(io::IO, f::ICSV2DTimeseries)
    print(io, "2D Timeseries\n$(f.metadata)\n$(f.fields)\n$(f.geolocation)")
end

function write(f::ICSV2DTimeseries, filename::AbstractString)
    first_key = first(f.dates)
    check_validity(f.fields, size(f.data[first_key], 2))
    delimiter = f.metadata.field_delimiter
    open(filename, "w") do io
        println(io, FIRSTLINES_2DTIMESERIES[end])
        println(io, "# [METADATA]")
        for (k,v) in metadata(f.metadata)
            println(io, "# $(k) = $(v)")
        end
        println(io, "# [FIELDS]")
        for (k, val) in all_fields(f.fields)
            fields_string = join(string.(val), delimiter)
            println(io, "# $(k) = $(fields_string)")
        end
        println(io, "# [DATA]")
        for d in f.dates
            println(io, "# [DATE=$(Dates.format(d, f.out_datefmt))]")
            CSV.write(io, f.data[d]; append=true, header=false, delim=delimiter)
        end
    end
    return filename
end

function append_timepoint(filename::AbstractString, timestamp::DateTime, data::DataFrame; field_delimiter::AbstractString=",", date_format::DateFormat=dateformat"yyyy-mm-ddTHH:MM:SS")
    open(filename, "a") do io
        println(io, "# [DATE=$(Dates.format(timestamp, date_format))]")
        delim = isempty(field_delimiter) ? ',' : field_delimiter[1]
        CSV.write(io, data; append=true, header=false, delim=delim)
    end
    return nothing
end


# conversions ---------------------------------------------------------------

function todataframe(f::ICSV2DTimeseries)
    # Long form: set declared field names, add :time, then vcat
    dfs = DataFrame[]
    for d in f.dates
        df = DataFrame(f.data[d])
        # Ensure column names reflect declared fields
        if !isempty(f.fields.fields)
            rename!(df, Symbol.(f.fields.fields))
        else
            rename!(df, Symbol.(names(df)))
        end
        df.time = fill(d, nrow(df))
        push!(dfs, df)
    end
    out = vcat(dfs...; cols = :union)
    # Attach file-level metadata
    DataFrames.metadata!(out, :icsv_metadata, metadata(f.metadata); style = :note)
    # Attach per-column field metadata (recommended/other fields) to data columns only
    misc = miscellaneous_fields(f.fields)
    ncols_data = length(f.fields.fields)
    for (attr, vals) in misc
        length(vals) == ncols_data || continue
        for (i, name) in enumerate(f.fields.fields)
            DataFrames.colmetadata!(out, Symbol(name), attr, vals[i]; style = :note)
        end
    end
    return out
end

function todimarray(f::ICSV2DTimeseries; rowdim=DimensionalData.X, coldim=Dim{:field}, idxcol::Union{Symbol,Nothing}=nothing, drop_non_numeric::Bool=true)
    dates = f.dates
    isempty(dates) && throw(ArgumentError("No data"))
    # pick a reference df
    dfref = f.data[dates[1]]
    # choose index column: user > :layer_index if available > none
    nsyms = Symbol.(names(dfref))
    idx = idxcol !== nothing ? idxcol : (:layer_index in nsyms ? :layer_index : nothing)
    # fields: start from DataFrame names, drop idx/timestamp/time, then reorder by metadata if present
    skip = Set(Symbol.( ["timestamp", "time"] ))
    if idx !== nothing
        push!(skip, idx)
    end
    fields = [c for c in Symbol.(names(dfref)) if !(c in skip)]
    if drop_non_numeric
        fields = [c for c in fields if eltype(dfref[!,c]) <: Union{Missing, Number}]
    end
    if !isempty(f.fields.fields)
        ordermap = Dict(Symbol(v) => i for (i,v) in enumerate(f.fields.fields))
        fields = sort(fields, by = c -> get(ordermap, c, typemax(Int)))
    end
    isempty(fields) && throw(ArgumentError("No data fields available for 2DTIMESERIES DimArray (after filtering index/time and non-numeric)"))
    # layer/row coordinates
    has_idx = idx !== nothing && (idx in nsyms)
    if has_idx
        layers = sort(unique(vcat([collect(skipmissing(f.data[d][!, idx])) for d in dates]...)))
    else
        Lmax = maximum(nrow(f.data[d]) for d in dates)
        layers = collect(1:Lmax)
    end
    L = length(layers)
    F = length(fields)
    Tn = length(dates)
    # element type across all dates/fields
    Ts = Union{Missing}
    for d in dates
        for c in fields
            Ts = Base.promote_type(Ts, eltype(f.data[d][!, c]))
        end
    end
    # allocate with correct element type and fill with missing
    A = Array{Ts}(undef, L, F, Tn)
    fill!(A, missing)
    if has_idx
        idxmap = Dict(layers[i] => i for i in eachindex(layers))
        for (k,d) in pairs(dates)
            df = f.data[d]
            for (j,c) in enumerate(fields)
                col = df[!, c]
                for r in 1:nrow(df)
                    li = df[r, idx]
                    if li === missing; continue; end
                    i = get(idxmap, li, nothing)
                    i === nothing && continue
                    A[i,j,k] = col[r]
                end
            end
        end
    else
        for (k,d) in pairs(dates)
            df = f.data[d]
            for (j,c) in enumerate(fields)
                col = df[!, c]
                for i in 1:nrow(df)
                    A[i,j,k] = col[i]
                end
            end
        end
    end
    # Row dimension selection with fallback to Y if no idx and default X requested
    eff_rowdim = (!has_idx && rowdim === DimensionalData.X) ? DimensionalData.Y : rowdim
    drow = eff_rowdim(layers)
    dfield = coldim(String.(fields))
    dtime = Ti(dates)
    return DimArray(A, (drow, dfield, dtime))
end


# ------------------------ Read from file -----------------------
# TODO: Read the correct parts of the file instead of writing to a buffer?
function read_icsv_timeseries(filename::AbstractString, date_fmt::DateFormat=ISODateFormat)
    data = Dict{DateTime,DataFrame}()
    dates = DateTime[]
    # We'll collect data blocks per date
    blocks = Dict{DateTime, Vector{String}}()
    current_date = nothing
    section = ""
    metadata = Dict{String, Any}()
    fields = Dict{String, Any}()
    geometry = nothing
    meta_section = nothing
    fields_section = nothing
    open(filename, "r") do io
        first_line = rstrip(readline(io))
        first_line in FIRSTLINES_2DTIMESERIES || throw(ArgumentError("Not an iCSV file with the 2D timeseries application profile"))
        for rawline in eachline(io)
            if startswith(rawline, "#")
                line = strip(rawline[2:end])
                section, header = _parse_comment_line!(line, section)
                header && continue
                if section == "metadata"
                    _parse_metadata_section_line!(metadata, line)
                elseif section == "fields"
                    geometry = Geometry(metadata["geometry"], metadata["srid"])
                    meta_section = MetaDataSection(metadata...)
                    _parse_fields_section_line!(fields, meta_section, line)
                elseif section == "data" && startswith(line, "[DATE=")
                    date_str = split(split(line, "[DATE=")[2], "]")[1]
                    push!(f.dates, DateTime(date_str, date_fmt))
                    current_date = f.dates[end]
                    blocks[current_date] = String[]
                end
            else
                section == "data" || throw(ArgumentError("Data section was not specified"))
                fields_section = FieldsSection(fields...)
                current_date === nothing && throw(ArgumentError("No [DATE=...] marker before data lines"))
                push!(blocks[current_date], rawline)
            end
        end
    end
    # Build DataFrames for each date block
    delim = meta_section.field_delimiter
    for d in f.dates
        lines = get(blocks, d, String[])
        buf = IOBuffer()
        for ln in lines
            println(buf, ln)
        end
        seekstart(buf)
        df = DataFrame(CSV.File(buf; header=false, delim=delim))
        rename!(df, Symbol.(f.fields.fields))
        f.data[d] = df
    end
    # sanity checks and geometry
    isempty(f.dates) && throw(ArgumentError("No dates found in 2DTIMESERIES file"))
    check_validity(f.fields, size(f.data[f.dates[1]], 2))
    return ICSV2DTimeseries(meta_section, fields_section, geometry, data, dates)
end