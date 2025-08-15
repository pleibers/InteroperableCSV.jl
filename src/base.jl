# Core iCSV file type and I/O

struct ICSVBase{M <: MetaDataSection, G <: Geometry}
    metadata::M
    fields::FieldsSection
    geolocation::G
    data::DataFrame
end

function Base.show(io::IO, f::ICSVBase)
    print(io, "ICSVBase\n$(f.metadata)\n$(f.fields)\n$(f.geolocation)\n$(f.data)")
end

function write(f::ICSVBase, filename::AbstractString)
    f.data === nothing && throw(ArgumentError("No data to write"))
    check_validity(f.fields, size(f.data, 2))
    delimiter = f.metadata.field_delimiter
    open(filename, "w") do io
        println(io, FIRSTLINES[end])
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
    end
    CSV.write(filename, f.data; append=true, header=false, delim=delimiter)
    return filename
end

# conversions ---------------------------------------------------------------

function todataframe(f::ICSVBase)
    # Work on a copy to avoid mutating the stored data
    df = DataFrame(f.data)
    # Ensure column names reflect declared fields
    if !isempty(f.fields.fields)
        rename!(df, Symbol.(f.fields.fields))
    end
    # Attach file-level metadata as a single dictionary entry
    # Keys and values are strings per metadata(f.metadata)
    DataFrames.metadata!(df, :icsv_metadata, metadata(f.metadata); style = :note)
    # Attach per-column field metadata (recommended and other fields)
    misc = miscellaneous_fields(f.fields) # Dict{Symbol,Vector{String}}
    ncols = ncol(df)
    for (attr, vals) in misc
        # Skip if sizes mismatch
        length(vals) == ncols || continue
        for (i, name) in enumerate(f.fields.fields)
            DataFrames.colmetadata!(df, Symbol(name), attr, vals[i]; style = :note)
        end
    end
    return df
end
  
function todimarray(f::ICSVBase; rowdim=DimensionalData.X, coldim=Dim{:field}, idxcol::Union{Symbol,Nothing}=nothing, drop_non_numeric::Bool=true)
    df = f.data::DataFrame
    # Determine index column: user choice > timestamp > time > none
    idx = idxcol !== nothing ? idxcol : (:timestamp in names(df) ? :timestamp : (:time in names(df) ? :time : nothing))
    # Start from DataFrame names, drop index; then (optionally) order by metadata
    dfcols = collect(Symbol.(names(df)))
    cols = [c for c in dfcols if idx === nothing || c != idx]
    # Filter non-numeric if requested
    if drop_non_numeric
        cols = [c for c in cols if eltype(df[!,c]) <: Union{Missing, Number}]
    end
    # If metadata fields exist, reorder cols to match their order
    if !isempty(f.fields.fields)
        ordermap = Dict(Symbol(v) => i for (i,v) in enumerate(f.fields.fields))
        cols = sort(cols, by = c -> get(ordermap, c, typemax(Int)))
    end
    isempty(cols) && throw(ArgumentError("No data columns available for DimArray (after filtering index and non-numeric)"))
    # Build matrix (row x field)
    T = Base.promote_type(map(c->eltype(df[!,c]), cols)...)
    M = Array{T}(undef, nrow(df), length(cols))
    for (j,c) in enumerate(cols)
        M[:,j] = df[!,c]
    end
    # Row coordinates
    rowcoords = idx === nothing ? collect(1:nrow(df)) : df[!, idx]
    # If no index column and user left default X, fall back to Y as requested
    eff_rowdim = (idx === nothing && rowdim === DimensionalData.X) ? DimensionalData.Y : rowdim
    drow = eff_rowdim(rowcoords)
    dfield = coldim(Symbol.(cols))
    return DimArray(M, (drow, dfield))
end

# ----------- Read from file -----------------------
function read_icsv_base(filename::AbstractString) 
    skip_lines = 0
    section = ""
    metadata = Dict{String, Any}()
    fields = Dict{String, Any}()
    geometry = nothing
    meta_section  = nothing
    fields_section = nothing
    open(filename, "r") do io
        first_line = rstrip(readline(io))
        first_line in FIRSTLINES || throw(ArgumentError("Not an iCSV file"))
        skip_lines += 1
        for line in eachline(io)
            if startswith(line, "#")
                skip_lines += 1
                line = strip(line[2:end])
                section, header = _parse_comment_line!(line, section)
                header && continue
                if section == "metadata"
                    _parse_metadata_section_line!(metadata, line)
                elseif section == "fields"
                    geometry = Geometry(metadata["geometry"], metadata["srid"])
                    meta_section = MetaDataSection(metadata...)
                    _parse_fields_section_line!(fields, meta_section, line)
                elseif section == "data"
                    throw(ArgumentError("Data section should not contain any comments"))
                end
            else
                fields_section = FieldsSection(fields...)
                section == "data" || throw(ArgumentError("Data section was not specified"))
                break
            end
        end
    end
    df = DataFrame(CSV.File(filename; header=false, comment="#", delim=delim, skipto=skip_lines))
    check_validity(fields_section, size(df, 2))
    _update_columns!(df, fields_section)
    return ICSVBase(meta_section, fields_section, geometry, df)
end

function _parse_comment_line!(line::AbstractString, section::String)
    if line == "[METADATA]"
        return "metadata", true
    elseif line == "[FIELDS]"
        metadata = MetaDataSection(metadata...) # is a dict for now, transform into Section
        return "fields", true
    elseif line == "[DATA]"
        return "data", true
    else
        return section, false
    end
end

function _parse_vals(line)
    line_vals = split(line, "=")
    length(line_vals) == 2 || throw(ArgumentError("Invalid $(section) line: $(line), got 2 assignment operators \"=\""))
    key = strip(line_vals[1])
    val = strip(line_vals[2])
    return key, val
end

function _parse_metadata_section_line!(metadata, line)
    key, val = _parse_vals(line)
    metadata[key] = val
end

function _parse_fields_section_line!(fields, metadata::MetaDataSection, line)
    key, val = _parse_vals(line)
    delim = String(get_attribute(metadata, "field_delimiter"))
    fields_vec = [strip(s) for s in split(val, delim)]
    fields[key] = fields_vec
end

function _update_columns!(df::DataFrame, fields::FieldsSection)
    rename!(df, Symbol.(fields.fields))
    # timestamp/time parsing
    for field in ("time", "timestamp")
        if field in fields.fields
            col = df[!, Symbol(field)]
            df[!, Symbol(field)] = parsedatetimevec(col)
        end
    end
    return df
end