# Utility helpers for iCSV

"""
    isnumber(x) -> Bool

Return `true` if `x` is a numeric value (`x isa Number`). See also [`isnumber(::AbstractString)`](@ref).
"""
isnumber(x)::Bool = x isa Number

"""
    isnumber(s::AbstractString) -> Bool

Return `true` if the string `s` parses as a `Float64`, otherwise `false`.
Useful when ingesting numeric columns encoded as strings in `[DATA]`.
"""
function isnumber(s::AbstractString)::Bool
    try
        parse(Float64, s)
        return true
    catch
        return false
    end
end


"""
    _DT_FORMATS

Internal: set of common ISO-like `DateFormat`s attempted by [`tryparsedatetime`](@ref).
"""
# Try to parse a DateTime from a string using a few common ISO-ish formats
const _DT_FORMATS = (
    dateformat"yyyy-mm-ddTHH:MM:SS",
    dateformat"yyyy-mm-dd HH:MM:SS",
    dateformat"yyyy-mm-ddTHH:MM:SS.s",
    dateformat"yyyy-mm-dd HH:MM:SS.s",
    dateformat"yyyy-mm-ddTHH:MM:SS.sss",
    dateformat"yyyy-mm-dd HH:MM:SS.sss",
)

"""
    tryparsedatetime(s) -> Union{DateTime,Nothing}

Try to parse a `DateTime` from `s` using a few common ISO-like formats.
Returns `nothing` if parsing fails.
"""
function tryparsedatetime(s)
    for fmt in _DT_FORMATS
        try
            return DateTime(s, fmt)
        catch
        end
    end
    try
        # last attempt: let Dates try default parsing
        return DateTime(s)
    catch
        return nothing
    end
end

"""
    parsedatetimevec(vec)

Convert a vector of possibly string timestamps to `Union{DateTime,Missing}` if any
element can be parsed; otherwise return the original vector.
"""
# Convert a vector that may be strings into DateTime if possible; otherwise return original
function parsedatetimevec(vec)
    if eltype(vec) <: Union{Date,DateTime}
        return vec
    end
    if eltype(vec) <: AbstractString || eltype(vec) <: Any
        out = Vector{Union{DateTime,Missing}}(undef, length(vec))
        anyparsed = false
        @inbounds for i in eachindex(vec)
            v = vec[i]
            if v === missing || v === nothing
                out[i] = missing
                continue
            end
            if v isa DateTime
                out[i] = v
                anyparsed = true
            else
                dt = tryparsedatetime(String(v))
                if dt === nothing
                    return vec # give up, return original
                else
                    out[i] = dt
                    anyparsed = true
                end
            end
        end
        return anyparsed ? out : vec
    end
    return vec
end
