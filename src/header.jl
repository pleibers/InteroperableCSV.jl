# Header structures: metadata, fields, and geometry


# -------------------- Geometry --------------------
"""
    Loc(; x::Float64, y::Float64, z::Union{Nothing,Float64}=nothing)

A simple 2D/3D point location used in iCSV headers.

- `x`, `y` are required coordinates.
- Optional `z` adds altitude if provided.

Examples
```julia
julia> using InteroperableCSV
julia> Loc(x=600000.0, y=200000.0)
Location: (X: 600000.0, Y: 200000.0)

julia> Loc(x=7.0, y=8.0, z=9.0)
Location: (X: 7.0, Y: 8.0, Z: 9.0)
```
"""
struct Loc{Z <: Union{Nothing,Float64}}
    x::Float64
    y::Float64
    z::Z
end


function Base.show(io::IO, l::Loc)
    print(io, "Location: ")
    print(io, "(X: $(l.x), Y: $(l.y)")
    if l.z !== nothing
        print(io, ", Z: $(l.z)")
    end
    print(io, ")")
end

function Loc(;x::Float64,y::Float64,z::Union{Nothing,Float64}=nothing)
    return Loc{typeof(z)}(x,y,z)
end

"""
    Geometry(epsg::Int, location::Loc)
    Geometry(epsg::Int, column_name::String)
    Geometry(geometry::AbstractString, srid::AbstractString)

Geolocation metadata for an iCSV file.

- Either a concrete `location` (POINT/POINTZ) or a `column_name` is provided.
- `epsg` is the spatial reference (e.g. `2056`).
- The constructor from strings accepts WKT-like geometry (e.g. `"POINT(600000 200000)"` or `"POINTZ(7 8 9)"`) and SRID string (e.g. `"EPSG:2056"`).

Examples
```julia
julia> using InteroperableCSV
julia> Geometry("POINT(600000 200000)", "EPSG:2056")
Geolocation (EPSG 2056) : Location: (X: 600000.0, Y: 200000.0)

julia> Geometry(2056, Loc(x=7.0, y=8.0, z=9.0))
Geolocation (EPSG 2056) : Location: (X: 7.0, Y: 8.0, Z: 9.0)
```
"""
struct Geometry{CN <: Union{Nothing,String}, L <: Union{Nothing,Loc}}
    epsg::Int
    column_name::CN
    location::L
end

function Geometry(geometry::AbstractString, srid::AbstractString)
    epsg = parse_srid(String(srid))
    location, column_name = parse_location(String(geometry))
    return Geometry(epsg, column_name, location)
end
Geometry(epsg::Int, location::Loc) = Geometry(epsg, nothing, location)
Geometry(epsg::Int, column_name::String) = Geometry(epsg, column_name, nothing)

function Base.show(io::IO, g::Geometry)
    print(io, "Geolocation (EPSG $(g.epsg)) ")
    if g.column_name !== nothing 
        println(io,"in column: $(g.column_name)")
    else
        println(io,": $(g.location)")
    end
end

"""
    parse_srid(srid::String) -> Int

Internal: parse `"EPSG:XXXX"` strings from the [METADATA] section into an integer EPSG code.
Used by [`Geometry(::AbstractString, ::AbstractString)`](@ref).
"""
function parse_srid(srid::String)
    try 
        parts = split(srid, ":")
        return parse(Int, parts[end])
    catch
        throw(ArgumentError("Invalid SRID: $(srid), expected format: EPSG:XXXX"))
    end
end

"""
    parse_location(geometry::String) -> (Loc|nothing, String|nothing)

Internal: parse WKT-like `"POINT(...)"`/`"POINTZ(...)"` strings from [METADATA] into a
[`Loc`](@ref). If parsing fails, returns `(nothing, geometry)` treating the string as a column name.
Used by [`Geometry(::AbstractString, ::AbstractString)`](@ref).
"""
function parse_location(geometry::String)
    if occursin("POINTZ", geometry)
        content = split(split(geometry, "(")[2], ")")[1]
        vals = split(content, " ")
        length(vals) == 3 || throw(ArgumentError("Invalid POINTZ geometry"))
        x = parse(Float64, vals[1])
        y = parse(Float64, vals[2])
        z = parse(Float64, vals[3])
        return Loc(x, y, z), nothing
    elseif occursin("POINT", geometry)
        content = split(split(geometry, "(")[2], ")")[1]
        vals = split(content, " ")
        length(vals) == 2 || throw(ArgumentError("Invalid POINT geometry"))
        x = parse(Float64, vals[1])
        y = parse(Float64, vals[2])
        return Loc(x, y, nothing), nothing
    else
        @info "Using geometry string $geometry as column name for location, unknown WKTZ string"
        return nothing, geometry
    end
end

"""
    get_geometry_string(g::Geometry) -> String

Internal: render a [`Geometry`](@ref) back into the form expected in [METADATA]
(`"POINT..."` or a column name).
"""
function get_geometry_string(g::Geometry)
    if g.column_name !== nothing
        return g.column_name
    end
    if g.location.z !== nothing
        return "POINTZ($(g.location.x) $(g.location.y) $(g.location.z))"
    else
        return "POINT($(g.location.x) $(g.location.y))"
    end
end

"""
    get_srid_string(g::Geometry) -> String

Internal: render an EPSG string for [METADATA], e.g. `"EPSG:2056"`.
"""
get_srid_string(g::Geometry) = "EPSG:$(g.epsg)"

# -------------------- MetaDataSection --------------------
"""
    RequiredMetadata(; field_delimiter::String, geometry::Union{String,Geometry}, srid=nothing)

Required [METADATA] keys for an iCSV file.

- `field_delimiter` — the delimiter used in the `[FIELDS]` header and the data section.
- `geometry` — either a WKT-like `"POINT(...)"`/`"POINTZ(...)"` or a column name carrying geometry.
- `srid` — spatial reference as `"EPSG:XXXX"`; inferred from `Geometry` if omitted.

Usually constructed internally via [`MetaDataSection`](@ref).
See also: [`Geometry`](@ref), [`metadata`](@ref).
"""
struct RequiredMetadata
    field_delimiter::String
    geometry::String
    srid::String
end
function RequiredMetadata(;field_delimiter::AbstractString, geometry::Union{AbstractString,Geometry}, srid::Union{AbstractString,Nothing}=nothing, kwargs...) 
    if geometry isa Geometry
        geometry_string = get_geometry_string(geometry)
        srid_string = get_srid_string(geometry)
    elseif geometry isa AbstractString
        geometry_string = String(geometry)
        srid_string = String(srid)
    end
    return RequiredMetadata(String(field_delimiter), geometry_string, srid_string)
end

"""
    RecommendedMetadata(; station_id=nothing, nodata=nothing, timezone=nothing, doi=nothing, timestamp_meaning=nothing)

Optional, recommended [METADATA] keys. Empty fields are omitted on write.

Common conventions
- `station_id` — site identifier
- `nodata` — sentinel value meaning missing in data rows
- `timezone` — e.g. `"UTC"`, `"Europe/Zurich"`, or numeric offset
- `doi` — dataset DOI
- `timestamp_meaning` — e.g. `"start"`, `"end"`, `"instant"`

See also: [`MetaDataSection`](@ref).
"""
struct RecommendedMetadata{SID <: Union{String,Nothing}, NO <: Union{Float64,Int,Nothing}, TZ <: Union{String,Int,Float64,Nothing}, DOI <: Union{String,Nothing}, TM <: Union{String,Nothing}}
    station_id::SID
    nodata::NO
    timezone::TZ
    doi::DOI
    timestamp_meaning::TM
end
Base.isempty(rec::RecommendedMetadata) = (rec.station_id === nothing && rec.nodata === nothing && rec.timezone === nothing && rec.doi === nothing && rec.timestamp_meaning === nothing)
function RecommendedMetadata(; station_id=nothing, nodata=nothing, timezone=nothing, doi=nothing, timestamp_meaning=nothing, kwargs...)
    return RecommendedMetadata(station_id, nodata, timezone, doi, timestamp_meaning)
end

function _pop_from_metadata_kwargs!(kwargs::Dict{Symbol,S}) where S <: Any
    required_kwargs = fieldnames(RequiredMetadata)
    recommended_kwargs = fieldnames(RecommendedMetadata)
    for k in required_kwargs
        if haskey(kwargs, k)
            delete!(kwargs, k)
        end
    end
    for k in recommended_kwargs
        if haskey(kwargs, k)
            delete!(kwargs, k)
        end
    end
    return
end
    

"""
    MetaDataSection(; field_delimiter, geometry, srid, station_id=nothing, nodata=nothing, timezone=nothing, doi=nothing, timestamp_meaning=nothing, kwargs...)

Container for all file-level [METADATA]. Composed of:

- Required: `field_delimiter`, `geometry`, `srid` (see [`RequiredMetadata`](@ref)).
- Recommended: `station_id`, `nodata`, `timezone`, `doi`, `timestamp_meaning` (see [`RecommendedMetadata`](@ref)).
- Other: any additional key/value pairs.

Access fields directly, e.g. `md.field_delimiter` or `md.station_id`.

See also: [`metadata`](@ref) for a flat Dict view used when writing headers,
[`Geometry`](@ref) for SRID/POINT parsing.

Example
```julia
md = MetaDataSection(field_delimiter=",", geometry="POINT(600000 200000)", srid="EPSG:2056", station_id="X")
```
"""
struct MetaDataSection{RM <: RecommendedMetadata}
    required::RequiredMetadata
    recommended::RM
    other_metadata::Dict{Symbol,String}
end

function MetaDataSection(;kwargs...)
    required = nothing
    try 
        required = RequiredMetadata(;kwargs...)
    catch e
        throw(ArgumentError("Invalid required metadata, needs field_delimiter::String, geometry::String, srid::Int\n\n Error: $(e)"))
    end
    recommended = RecommendedMetadata(;kwargs...)
    kw = Dict(kwargs)
    _pop_from_metadata_kwargs!(kw)
    other_metadata = Dict{Symbol,String}(kw)
    return MetaDataSection(required, recommended, other_metadata)
end

function Base.getproperty(md::MetaDataSection, field::Symbol)
    if field in fieldnames(RequiredMetadata)
        return getfield(getfield(md, :required), field)
    elseif field in fieldnames(RecommendedMetadata)
        return getfield(getfield(md, :recommended), field)
    elseif field in keys(getfield(md, :other_metadata))
        return getfield(md, :other_metadata)[field]
    else
        return getfield(md, field)
    end
end

function Base.show(io::IO, md::MetaDataSection)
    out_msg = "METADATA:\nRequired:\n"
    req_md = getfield(md, :required)
    rec_md = getfield(md, :recommended)
    oth_md = getfield(md, :other_metadata)
    req = join(["$k : $(getfield(req_md, k))" for k in keys(req_md)], "\n")
    out_msg *= req
    if !isempty(rec_md)
        out_msg *= "\nRecommended:\n"
        rec = join(["$k : $(getfield(rec_md, k))" for k in keys(rec_md) if getfield(rec_md, k) !== nothing], "\n")
        out_msg *= rec
    end
    if !isempty(oth_md)
        out_msg *= "\nOther Metadata:\n"
        oth = join(["$k : $(v)" for (k,v) in oth_md if v !== nothing], "\n")
        out_msg *= oth
    end
    print(io, out_msg)
end

"""
    get_attribute(obj, name)

Retrieve a metadata/field attribute by name. Works for both `MetaDataSection` and
`FieldsSection`. Returns `nothing` if not set.
"""
get_attribute(md::MetaDataSection, attribute_name::String) = get_attribute(md, Symbol(attribute_name))
function get_attribute(md::MetaDataSection, attribute_name::Symbol)
    req_md = getfield(md, :required)
    rec_md = getfield(md, :recommended)
    oth_md = getfield(md, :other_metadata)
    if attribute_name in fieldnames(RequiredMetadata)
        return getfield(req_md, attribute_name)
    elseif attribute_name in fieldnames(RecommendedMetadata)
        return getfield(rec_md, attribute_name)
    elseif attribute_name in keys(oth_md)
        return get(oth_md, attribute_name, nothing)
    else
        @warn "Invalid attribute name: $(attribute_name)"
        return nothing
    end
end

"""
    metadata(md::MetaDataSection) -> Dict{String,String}

Return a flat Dict view of all metadata values (required, recommended if set, and other), stringified for writing.
"""
function metadata(md::MetaDataSection)::Dict{String,String}
    metadata = Dict{String,String}()
    req_md = md.required
    rec_md = md.recommended
    oth_md = md.other_metadata
    for k in fieldnames(RequiredMetadata)
        metadata[string(k)] = string(getfield(req_md, k))
    end
    for k in fieldnames(RecommendedMetadata)
        v = getfield(rec_md, k)
        v !== nothing && (metadata[string(k)] = string(v))
    end
    for (k,v) in oth_md
        v !== nothing && (metadata[string(k)] = string(v))
    end
    return metadata
end

# -------------------- FieldsSection --------------------
"""
    RecommendedFields(; units_multiplier=[], units=[], long_name=[], standard_name=[])

Optional per-column attributes that may appear in the [FIELDS] section.
Vectors must either be empty or have the same length as `fields`.

Common conventions
- `units_multiplier` — numeric scaling factors per column
- `units` — e.g. `"m"`, `"K"`
- `long_name`, `standard_name` — human- and standard-vocabulary names

See also: [`FieldsSection`](@ref), [`check_validity`](@ref).
"""
struct RecommendedFields
    units_multiplier::Vector{Float64}
    units::Vector{String}
    long_name::Vector{String}
    standard_name::Vector{String}
end
function RecommendedFields(;units_multiplier=Float64[], units=String[], long_name=String[], standard_name=String[],kwargs...)
    return RecommendedFields(units_multiplier, String.(units), String.(long_name), String.(standard_name))
end

function Base.isempty(rec::RecommendedFields)
    for k in fieldnames(RecommendedFields)
        isempty(getfield(rec, k)) || return false
    end
    return true
end

function _pop_from_fields_kwargs!(kwargs::Dict{Symbol,S}) where S <: Any
   recommended_fields_kwargs = fieldnames(RecommendedFields)
   for k in recommended_fields_kwargs
       if haskey(kwargs, k)
           delete!(kwargs, k)
       end
   end
end
"""
    FieldsSection

Representation of the [FIELDS] header. Contains:

- `fields::Vector{String}` — column names in the data section (required)
- `recommended_fields::RecommendedFields` — optional per-column attributes
- `other_fields::Dict{Symbol,Vector{String}}` — any other per-column attributes

See also: [`all_fields`](@ref), [`miscellaneous_fields`](@ref), [`check_validity`](@ref).
"""
struct FieldsSection
    fields::Vector{String}
    recommended_fields::RecommendedFields
    other_fields::Dict{Symbol,Vector{String}}
end

"""
    FieldsSection(; fields::Vector{String}, kwargs...)

Construct from keyword arguments typically parsed from the [FIELDS] section.
`fields` is required; any provided vectors under known recommended keys populate
[`RecommendedFields`](@ref), and all other vector-valued keys go into `other_fields`.

Notes
- String-valued attributes are ignored with a warning; use vector values per column.
- Lengths are validated later by [`check_validity`](@ref).
"""
function FieldsSection(;fields::Vector{S},kwargs...) where S <: AbstractString
    rec = RecommendedFields(;kwargs...)
    kw = Dict(kwargs)
    _pop_from_fields_kwargs!(kw)
    other_fields = Dict{Symbol,Vector{String}}()
    for (k,v) in kw
        if v isa AbstractString
            @warn "String values for $(k) are not supported, skipping"
            continue
        end
        other_fields[k] = v
    end
    fields =  FieldsSection(fields, rec, other_fields)
    return fields
end

function Base.show(io::IO, f::FieldsSection)
    out_msg = "Fields: $(f.fields)\n"
    if !isempty(f.recommended_fields)
        out_msg *= "\nRecommended Fields:\n"
        out_msg *= join(["$k : $(v)" for (k,v) in f.recommended_fields if !isempty(v)], "\n")
    end
    if !isempty(f.other_fields)
        out_msg *= "\nOther Fields:\n"
        out_msg *= join(["$k : $(v)" for (k,v) in f.other_fields if !isempty(v)], "\n")
    end
    print(io, out_msg)
end

function get_attribute(f::FieldsSection, attribute_name::Symbol)
    if attribute_name == :fields
        return f.fields
    elseif attribute_name in fieldnames(RecommendedFields)
        return getfield(f.recommended_fields, attribute_name)
    elseif attribute_name in keys(f.other_fields)
        return get(f.other_fields, attribute_name, nothing)
    else
        @warn "Invalid attribute name: $(attribute_name)"
        return nothing
    end
end

"""
    all_fields(f::FieldsSection) -> Dict{Symbol,Vector{String}}

Return all declared per-column attributes including `:fields` and any non-empty recommended/other fields.
"""
function all_fields(f::FieldsSection)::Dict{Symbol,Vector{String}}
    all_fields = Dict{Symbol,Vector{String}}()
    all_fields[:fields] = f.fields
    for k in fieldnames(RecommendedFields)
        v = getfield(f.recommended_fields, k)
        !isempty(v) && (all_fields[k] = v)
    end
    for (k,v) in f.other_fields
        !isempty(v) && (all_fields[k] = v)
    end
    return all_fields
end

"""
    miscellaneous_fields(f::FieldsSection) -> Dict{Symbol,Vector{String}}

Return all per-column attributes except the main `:fields` names (used for metadata propagation to DataFrames/DimArrays).
"""
function miscellaneous_fields(f::FieldsSection)::Dict{Symbol,Vector{String}}
    misc_fields = Dict{Symbol,Vector{String}}()
    for k in fieldnames(RecommendedFields)
        v = getfield(f.recommended_fields, k)
        !isempty(v) && (misc_fields[k] = v)
    end
    for (k,v) in f.other_fields
        !isempty(v) && (misc_fields[k] = v)
    end
    return misc_fields
end

"""
    check_validity(f::FieldsSection, ncols::Int) -> true

Validate that all declared field vectors have length `ncols`.
Throws `ArgumentError` on mismatch.
"""
function check_validity(f::FieldsSection, ncols::Int)
    length(f.fields) == ncols || throw(ArgumentError("Number of fields does not match the number of columns"))
    for k in fieldnames(RecommendedFields)
        v = getfield(f.recommended_fields, k)
        (!isempty(v) && length(v) != ncols) && throw(ArgumentError("Number of $(k) does not match the number of columns"))
    end
    for (k,v) in f.other_fields
        (!isempty(v) && length(v) != ncols) && throw(ArgumentError("Number of $(k) does not match the number of columns"))
    end
    return true
end
