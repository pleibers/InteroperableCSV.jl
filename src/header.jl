# Header structures: metadata, fields, and geometry


# -------------------- Geometry --------------------
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

struct Geometry{CN <: Union{Nothing,String}, L <: Union{Nothing,Loc}}
    epsg::Int
    column_name::CN
    location::L
end

function Geometry(geometry::String, srid::String)
    epsg = parse_srid(srid)
    location, column_name = parse_location(geometry)
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

function parse_srid(srid::String)
    try 
        parts = split(srid, ":")
        return parse(Int, parts[end])
    catch
        throw(ArgumentError("Invalid SRID: $(srid), expected format: EPSG:XXXX"))
    end
end

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

get_srid_string(g::Geometry) = "EPSG:$(g.epsg)"

# -------------------- MetaDataSection --------------------
struct RequiredMetadata
    field_delimiter::String
    geometry::String
    srid::String
end
Base.keys(::RequiredMetadata) = fieldnames(RequiredMetadata)
Base.haskey(md::RequiredMetadata, key::AbstractString) = key in keys(md)
function RequiredMetadata(;field_delimiter::String, geometry::Union{String,Geometry}, srid::Union{String,Nothing}=nothing, kwargs...) 
    if geometry isa Geometry
        geometry_string = get_geometry_string(geometry)
        srid_string = get_srid_string(geometry)
    elseif geometry isa String
        geometry_string = geometry
        srid_string = srid
    end
    return RequiredMetadata(field_delimiter, geometry_string, srid_string)
end

struct RecommendedMetadata{SID <: Union{String,Nothing}, NO <: Union{Float64,Int,Nothing}, TZ <: Union{String,Int,Float64,Nothing}, DOI <: Union{String,Nothing}, TM <: Union{String,Nothing}}
    station_id::SID
    nodata::NO
    timezone::TZ
    doi::DOI
    timestamp_meaning::TM
end
Base.keys(::RecommendedMetadata) = fieldnames(RecommendedMetadata)
Base.haskey(md::RecommendedMetadata, key::AbstractString) = key in keys(md)
Base.isempty(rec::RecommendedMetadata{SID <: Union{String,Nothing}, NO <: Union{Float64,Int,Nothing}, TZ <: Union{String,Int,Float64,Nothing}, DOI <: Union{String,Nothing}, TM <: Union{String,Nothing}}) = (rec.station_id === nothing && rec.nodata === nothing && rec.timezone === nothing && rec.doi === nothing && rec.timestamp_meaning === nothing)
function RecommendedMetadata(; station_id=nothing, nodata=nothing, timezone=nothing, doi=nothing, timestamp_meaning=nothing, kwargs...)
    return RecommendedMetadata(station_id, nodata, timezone, doi, timestamp_meaning)
end

function _pop_from_metadata_kwargs!(kwargs::Dict{Symbol,Any})
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
    

struct MetaDataSection{RM <: RecommendedMetadata}
    required::RequiredMetadata
    recommended::RM
    other_metadata::Dict{Symbol,String}
end

function MetaDataSection(;kwargs...)
    try 
        required = RequiredMetadata(;kwargs...)
    catch e
        throw(ArgumentError("Invalid required metadata, needs field_delimiter::String, geometry::String, srid::Int\n\n Error: $(e)"))
    end
    recommended = RecommendedMetadata(;kwargs...)
    _pop_from_metadata_kwargs!(kwargs)
    other_metadata = Dict{Symbol,String}(kwargs)
    return MetaDataSection(required, recommended, other_metadata)
end

function Base.getfield(md::MetaDataSection, field::Symbol)
    if field in fieldnames(RequiredMetadata)
        return getfield(md.required, field)
    elseif field in fieldnames(RecommendedMetadata)
        return getfield(md.recommended, field)
    elseif field in keys(md.other_metadata)
        return md.other_metadata[field]
    else
        throw(ArgumentError("Invalid field name: $(field)"))
    end
end

function Base.show(io::IO, md::MetaDataSection)
    out_msg = "METADATA:\nRequired:\n"
    req = join(["$k : $(getfield(md.required, k))" for k in keys(md.required)], "\n")
    out_msg *= req
    if !isempty(md.recommended)
        out_msg *= "\nRecommended:\n"
        rec = join(["$k : $(getfield(md.recommended, k))" for k in keys(md.recommended) if getfield(md.recommended, k) !== nothing], "\n")
        out_msg *= rec
    end
    if !isempty(md.other_metadata)
        out_msg *= "\nOther Metadata:\n"
        oth = join(["$k : $(v)" for (k,v) in md.other_metadata if v !== nothing], "\n")
        out_msg *= oth
    end
    print(io, out_msg)
end
get_attribute(md::MetaDataSection, attribute_name::String) = get_attribute(md, Symbol(attribute_name))
function get_attribute(md::MetaDataSection, attribute_name::Symbol)
    if haskey(md.required, attribute_name)
        return getfield(md.required, attribute_name)
    elseif haskey(md.recommended, attribute_name)
        return getfield(md.recommended, attribute_name)
    else
        return get(md.other_metadata, attribute_name, nothing)
    end
end

function metadata(md::MetaDataSection)::Dict{String,String}
    metadata = Dict{String,String}()
    for k in keys(md.required)
        metadata[k] = string(getfield(md.required, k))
    end
    for k in keys(md.recommended)
        v = getfield(md.recommended, k)
        v !== nothing && (metadata[k] = string(v))
    end
    for (k,v) in md.other_metadata
        v !== nothing && (metadata[k] = string(v))
    end
    return metadata
end

# -------------------- FieldsSection --------------------
struct RecommendedFields
    units_multiplier::Vector{Float64}
    units::Vector{String}
    long_name::Vector{String}
    standard_name::Vector{String}
end
function RecommendedFields(;units_multiplier=Float64[], units=String[], long_name=String[], standard_name=String[],kwargs...)
    return RecommendedFields(units_multiplier, units, long_name, standard_name)
end

Base.haskey(::RecommendedFields, key::AbstractString) = key in fieldnames(RecommendedFields)
Base.keys(::RecommendedFields) = fieldnames(RecommendedFields)
function Base.isempty(rec::RecommendedFields)
    for k in fieldnames(RecommendedFields)
        isempty(getfield(rec, k)) || return false
    end
    return true
end

function _pop_from_fields_kwargs!(kwargs::Dict{Symbol,Any})
   recommended_fields_kwargs = fieldnames(RecommendedFields)
   for k in recommended_fields_kwargs
       if haskey(kwargs, k)
           delete!(kwargs, k)
       end
   end
end
struct FieldsSection
    fields::Vector{String}
    recommended_fields::RecommendedFields
    other_fields::Dict{Symbol,Vector{String}}
end

function FieldsSection(;fields::Vector{String},kwargs...)
    rec = RecommendedFields(;kwargs...)
    _pop_from_fields_kwargs!(kwargs)
    other_fields = Dict{Symbol,Vector{String}}(kwargs)
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
    elseif haskey(f.recommended_fields, attribute_name)
        return getfield(f.recommended_fields, attribute_name)
    else
        return get(f.other_fields, attribute_name, nothing)
    end
end

function all_fields(f::FieldsSection)::Dict{Symbol,Vector{String}}
    all_fields = Dict{Symbol,Vector{String}}()
    all_fields[:fields] = f.fields
    for (k,v) in f.recommended_fields
        !isempty(v) && (all_fields[k] = v)
    end
    for (k,v) in f.other_fields
        !isempty(v) && (all_fields[k] = v)
    end
    return all_fields
end

function miscellaneous_fields(f::FieldsSection)::Dict{Symbol,Vector{String}}
    misc_fields = Dict{Symbol,Vector{String}}()
    for (k,v) in f.recommended_fields
        !isempty(v) && (misc_fields[k] = v)
    end
    for (k,v) in f.other_fields
        !isempty(v) && (misc_fields[k] = v)
    end
    return misc_fields
end

function check_validity(f::FieldsSection, ncols::Int)
    length(f.fields) == ncols || throw(ArgumentError("Number of fields does not match the number of columns"))
    for k in keys(f.recommended_fields)
        v = getfield(f.recommended_fields, k)
        (!isempty(v) && length(v) != ncols) && throw(ArgumentError("Number of $(k) does not match the number of columns"))
    end
    for (k,v) in f.other_fields
        (!isempty(v) && length(v) != ncols) && throw(ArgumentError("Number of $(k) does not match the number of columns"))
    end
    return true
end
