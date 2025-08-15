# Utility helpers for iCSV

isnumber(x)::Bool = x isa Number

function isnumber(s::AbstractString)::Bool
    try
        parse(Float64, s)
        return true
    catch
        return false
    end
end

# Try to parse a DateTime from a string using a few common ISO-ish formats
const _DT_FORMATS = (
    dateformat"yyyy-mm-ddTHH:MM:SS",
    dateformat"yyyy-mm-dd HH:MM:SS",
    dateformat"yyyy-mm-ddTHH:MM:SS.s",
    dateformat"yyyy-mm-dd HH:MM:SS.s",
    dateformat"yyyy-mm-ddTHH:MM:SS.sss",
    dateformat"yyyy-mm-dd HH:MM:SS.sss",
)

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
