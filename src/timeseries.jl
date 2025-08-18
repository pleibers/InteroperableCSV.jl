# 2D Timeseries application profile

"""
    ICSV2DTimeseries(metadata::MetaDataSection, fields::FieldsSection, geolocation::Geometry,
                      data::Dict{DateTime,DataFrame}, dates::Vector{DateTime}; out_datefmt=dateformat"yyyy-mm-ddTHH:MM:SS")

In-memory representation of the iCSV 2DTIMESERIES application profile.

Format
- First line must match [`FIRSTLINES_2DTIMESERIES`](@ref), e.g. `"# iCSV 1.0 UTF-8 2DTIMESERIES"`.
- Header sections `[METADATA]` and `[FIELDS]` as in the standard profile.
- Data section `[DATA]` is split into blocks marked by `# [DATE=yyyy-mm-ddTHH:MM:SS]`.
- Each date block contains a full table with the same schema.

Components
- `metadata` → [`MetaDataSection`](@ref)
- `fields` → [`FieldsSection`](@ref)
- `geolocation` → [`Geometry`](@ref)
- `data` maps each `DateTime` to a `DataFrame` of rows for that timestamp.

See also: [`write(::ICSV2DTimeseries, ...)`](@ref), [`append_timepoint`](@ref), [`todataframe(::ICSV2DTimeseries)`](@ref), [`todimarray(::ICSV2DTimeseries)`](@ref).

Convenience constructor accepting `Vector{DataFrame}` and matching `Vector{DateTime}` is also provided.
"""
@kwdef struct ICSV2DTimeseries{M <: MetaDataSection, G <: Geometry}
    metadata::M
    fields::FieldsSection
    geolocation::G
    data::Dict{DateTime,DataFrame}
    dates::Vector{DateTime}
    out_datefmt::DateFormat = dateformat"yyyy-mm-ddTHH:MM:SS"
end

"""
    ICSV2DTimeseries(meta::MetaDataSection, fields::FieldsSection, geom::Geometry,
                      data::Dict{DateTime,DataFrame}, dates::Vector{DateTime};
                      out_datefmt=dateformat"yyyy-mm-ddTHH:MM:SS")

Convenience constructor to build directly from a dictionary of per-date DataFrames.
"""
function ICSV2DTimeseries(meta::MetaDataSection, fields::FieldsSection, geom::Geometry,
                           data::Dict{DateTime,DataFrame}, dates::Vector{DateTime};
                           out_datefmt::DateFormat = dateformat"yyyy-mm-ddTHH:MM:SS")
    return ICSV2DTimeseries(meta, fields, geom, data, collect(dates), out_datefmt)
end

function ICSV2DTimeseries(meta::MetaDataSection, fields::FieldsSection, geom::Geometry,
                           dfs::Vector{DataFrame}, dates::Vector{DateTime})
    length(dfs) == length(dates) || throw(ArgumentError("Number of data frames and dates must match"))
    d = Dict{DateTime,DataFrame}()
    for (i, dt) in enumerate(dates)
        d[dt] = dfs[i]
    end
    return ICSV2DTimeseries(meta, fields, geom, d, collect(dates))
end

function Base.show(io::IO, f::ICSV2DTimeseries)
    print(io, "2D Timeseries\n$(f.metadata)\n$(f.fields)\n$(f.geolocation)")
end

"""
    write(f::ICSV2DTimeseries, filename) -> filename

Write a 2DTIMESERIES iCSV file. Emits:
- First line from [`FIRSTLINES_2DTIMESERIES`](@ref)
- `# [METADATA]` from [`metadata(f.metadata)`](@ref metadata)
- `# [FIELDS]` from [`all_fields(f.fields)`](@ref all_fields)
- `# [DATA]` followed by, for each `d in f.dates`:
  - `# [DATE=...]` using `f.out_datefmt`
  - CSV rows of `f.data[d]` delimited by `f.metadata.field_delimiter`
"""
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

"""
    append_timepoint(filename, timestamp::DateTime, data::DataFrame; field_delimiter=",", date_format=dateformat"yyyy-mm-ddTHH:MM:SS")

Append a new `[DATE=...]` block and rows to an existing 2DTIMESERIES file.
Assumes the header has already been written and that `data` matches the declared fields.

Notes
- `field_delimiter` must match `[METADATA].field_delimiter` of the target file.
- `date_format` controls how the `[DATE=...]` marker is rendered; default is ISO-like.
- This function does not validate column count or types against the file header.
"""
function append_timepoint(filename::AbstractString, timestamp::DateTime, data::DataFrame; field_delimiter::AbstractString=",", date_format::DateFormat=dateformat"yyyy-mm-ddTHH:MM:SS")
    open(filename, "a") do io
        println(io, "# [DATE=$(Dates.format(timestamp, date_format))]")
        delim = isempty(field_delimiter) ? ',' : field_delimiter[1]
        CSV.write(io, data; append=true, header=false, delim=delim)
    end
    return nothing
end


# conversions ---------------------------------------------------------------

"""
    todataframe(f::ICSV2DTimeseries) -> DataFrame

Return a long-form `DataFrame` with declared field names and an added `:time` column.
- Attaches file-level metadata via `DataFrames.metadata!(..., :icsv_metadata, ...)` from [`metadata`](@ref).
- Propagates per-column attributes via `DataFrames.colmetadata!` for matching-length vectors; see [`miscellaneous_fields`](@ref).
See also: [`ICSVBase.todataframe`](@ref).
"""
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
    for (k,v) in metadata(f.metadata)
        DataFrames.metadata!(out, String(k), v)
    end
    # Attach per-column field metadata (recommended/other fields) to data columns only
    misc = miscellaneous_fields(f.fields)
    ncols_data = length(f.fields.fields)
    for (attr, vals) in misc
        length(vals) == ncols_data || continue
        for (i, name) in enumerate(f.fields.fields)
            DataFrames.colmetadata!(out, name, String(attr), vals[i]; style = :note)
        end
    end
    return out
end

"""
    todimarray(f::ICSV2DTimeseries; rowdim=DimensionalData.X, coldim=Dim{:field}, idxcol=nothing, drop_non_numeric=true) -> DimArray

Convert to a 3D `DimensionalData.DimArray` with dimensions `(layer, field, time)`.

- `idxcol` — optional per-row index column (e.g., `:layer_index`). If absent, layers are 1..L.
- `drop_non_numeric` — keep only numeric data columns.

Row dimension falls back to `DimensionalData.Y` when `idxcol` is not used and `rowdim` is the default `X`.
See also: [`ICSVBase.todimarray`](@ref) for the 2D case.
"""
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
"""
    read_icsv_timeseries(filename::AbstractString, date_fmt=dateformat"yyyy-mm-ddTHH:MM:SS") -> ICSV2DTimeseries

Low-level reader for the 2DTIMESERIES application profile. Used by `read`.

Behavior
- Validates first line against [`FIRSTLINES_2DTIMESERIES`](@ref).
- Parses `[METADATA]` and `[FIELDS]`; builds [`MetaDataSection`](@ref) and [`FieldsSection`](@ref).
- Scans `[DATA]` for `[DATE=...]` markers, counts rows per block, and errors on stray comments.
- Reads all rows in one CSV pass, validates schema via [`check_validity`](@ref), sets column names, parses timestamps.
"""
function read_icsv_timeseries(filename::AbstractString, date_fmt::DateFormat = dateformat"yyyy-mm-ddTHH:MM:SS")
    # Outputs
    data = Dict{DateTime,DataFrame}()
    dates = DateTime[]

    # Header and section parsing state
    section = ""
    metadata = Dict{Symbol, Any}()
    fields = Dict{Symbol, Any}()
    geometry = nothing
    meta_section = nothing
    fields_section = nothing

    # Pre-scan: gather dates and number of data rows per date block; validate no comments inside blocks
    block_lengths = Int[]
    in_block = false
    current_len = 0
    saw_data_section = false

    open(filename, "r") do io
        first_line = rstrip(readline(io))
        first_line in FIRSTLINES_2DTIMESERIES || throw(ArgumentError("Not an iCSV file with the 2D TIMESERIES application profile"))
        for rawline in eachline(io)
            if startswith(rawline, "#")
                line = strip(rawline[2:end])
                section, header = _parse_comment_line!(line, section)
                header && continue
                if section == "metadata"
                    _parse_metadata_section_line!(metadata, line)
                elseif section == "fields"
                    geometry = Geometry(metadata[:geometry], metadata[:srid])
                    meta_section = MetaDataSection(;metadata...)
                    _parse_fields_section_line!(fields, meta_section, line)
                elseif section == "data"
                    saw_data_section = true
                    if startswith(line, "[DATE=")
                        # close previous block
                        if in_block
                            push!(block_lengths, current_len)
                            current_len = 0
                        end
                        date_str = split(split(line, "[DATE=")[2], "]")[1]
                        push!(dates, DateTime(date_str, date_fmt))
                        in_block = true
                    else
                        # comment encountered inside data section: only [DATE=...] markers are allowed
                        in_block && throw(ArgumentError("Comments inside a data block are not allowed (only [DATE=...] markers). Offending line: $(rawline)"))
                        # allow comments before the first [DATE=...] within the data section
                    end
                end
            else
                if section == "data"
                    in_block || throw(ArgumentError("No [DATE=...] marker before data lines"))
                    # count only non-empty lines to match CSV ignoreemptyrows behavior
                    !isempty(strip(rawline)) && (current_len += 1)
                else
                    throw(ArgumentError("Data section was not specified"))
                end
            end
        end
    end

    # Finalize header sections and block bookkeeping
    fields_section = FieldsSection(;fields...)
    meta_section === nothing && throw(ArgumentError("Missing [METADATA]/[FIELDS] sections"))
    saw_data_section || throw(ArgumentError("Missing [DATA] section"))
    if in_block
        push!(block_lengths, current_len)
    end
    isempty(dates) && throw(ArgumentError("No [DATE=...] markers found in 2DTIMESERIES file"))

    # Single CSV pass over whole file; comments skipped
    delim_str = String(get_attribute(meta_section, "field_delimiter"))
    delim = isempty(delim_str) ? ',' : delim_str[1]
    df_all = DataFrame(CSV.File(filename; header=false, comment="#", delim=delim, ignoreemptyrows=true))

    total_rows = sum(block_lengths)
    nrow(df_all) == total_rows || throw(ArgumentError("Data row count mismatch: expected $(total_rows) rows across $(length(dates)) dates, got $(nrow(df_all))"))

    # Validate and set column names; also parse timestamp/time columns
    check_validity(fields_section, size(df_all, 2))
    _update_columns!(df_all, fields_section)

    # Split df_all into per-date DataFrames by cumulative row counts
    offsets = cumsum([0; block_lengths[1:end-1]])
    for (i, d) in enumerate(dates)
        r1 = offsets[i] + 1
        r2 = offsets[i] + block_lengths[i]
        @views data[d] = DataFrame(df_all[r1:r2, :])
    end

    return ICSV2DTimeseries(meta_section, fields_section, geometry, data, dates)
end