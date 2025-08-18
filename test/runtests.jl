using iCSV
using Test
using DataFrames
using Dates
using DimensionalData

@testset "ICSV basic read/write" begin
    tmp = mktempdir()
    file = joinpath(tmp, "basic.icsv")

    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:5]
    df = DataFrame(timestamp = ts, a = 1:5, b = 6:10)

    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINTZ(7 8 9)"
    metadata[:srid] = "EPSG:2056"
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["timestamp","a","b"]
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    f = ICSVBase(meta_section, fields_section, geometry, df)
    iCSV.write(f, file)

    g = iCSV.read(file)
    @test g isa ICSVBase
    @test size(iCSV.todataframe(g)) == size(df)
    @test names(iCSV.todataframe(g)) == names(df)
    @test !any(ismissing, g.data.timestamp)
    @test all(g.data.a .== df.a)
    @test all(g.data.b .== df.b)
    @test all(g.data.timestamp .== df.timestamp)

    loc = g.geolocation.location
    epsg = g.geolocation.epsg
    @test loc.x == 7.0 && loc.y == 8.0 && loc.z == 9.0 && epsg == 2056

    A = iCSV.todimarray(g)
    @test size(A) == (nrow(df), 2) # drop timestamp column
end

@testset "ICSV 2DTIMESERIES edge cases" begin
    tmp = mktempdir()
    function write_manual_timeseries(path::AbstractString; meta=Dict("field_delimiter"=>",","geometry"=>"POINT(1 2)","srid"=>"EPSG:2056"), fields::AbstractString="a,b", body_lines::Vector{String})
        open(path, "w") do io
            println(io, iCSV.FIRSTLINES_2DTIMESERIES[end])
            println(io, "# [METADATA]")
            for (k,v) in meta
                println(io, "# $(k) = $(v)")
            end
            println(io, "# [FIELDS]")
            println(io, "# fields = $(fields)")
            println(io, "# [DATA]")
            for ln in body_lines
                # Ensure DATE markers are written as comment lines per spec
                if startswith(ln, "[DATE=")
                    println(io, "# $(ln)")
                else
                    println(io, ln)
                end
            end
        end
        return path
    end

    # 1) Comments before first [DATE] are allowed
    file1 = joinpath(tmp, "ok_comment_before_date.icsv")
    body1 = [
        "# a comment before first date",
        "[DATE=2024-01-01T00:00:00]",
        "1,2",
        "3,4",
    ]
    write_manual_timeseries(file1; fields="a,b", body_lines=body1)
    q1 = iCSV.read(file1)
    @test q1 isa iCSV.ICSV2DTimeseries
    @test length(q1.dates) == 1
    @test Symbol.(names(q1.data[q1.dates[1]])) == [:a,:b]
    @test all(q1.data[q1.dates[1]][!, :a] .== [1,3])
    @test all(q1.data[q1.dates[1]][!, :b] .== [2,4])

    # 2) Comment inside a data block (after a [DATE]) should error
    file2 = joinpath(tmp, "bad_comment_inside_block.icsv")
    body2 = [
        "[DATE=2024-01-01T00:00:00]",
        "1,2",
        "# this should not be here",
        "3,4",
    ]
    write_manual_timeseries(file2; fields="a,b", body_lines=body2)
    @test_throws ArgumentError iCSV.read(file2)

    # 3) Blank lines inside blocks are ignored
    file3 = joinpath(tmp, "blank_lines_ok.icsv")
    body3 = [
        "[DATE=2024-01-01T00:00:00]",
        "",
        "1,2",
        "",
        "3,4",
        "",
        "[DATE=2024-01-02T00:00:00]",
        "5,6",
        "",
    ]
    write_manual_timeseries(file3; fields="a,b", body_lines=body3)
    q3 = iCSV.read(file3)
    @test length(q3.dates) == 2
    @test nrow(q3.data[q3.dates[1]]) == 2
    @test nrow(q3.data[q3.dates[2]]) == 1

    # 4) Missing [DATE] marker should error
    file4 = joinpath(tmp, "missing_date.icsv")
    body4 = [
        "1,2",
    ]
    write_manual_timeseries(file4; fields="a,b", body_lines=body4)
    @test_throws ArgumentError iCSV.read(file4)

    # 5) Column mismatch should error
    file5 = joinpath(tmp, "mismatch_columns.icsv")
    body5 = [
        "[DATE=2024-01-01T00:00:00]",
        "1,2",
    ]
    write_manual_timeseries(file5; fields="a,b,c", body_lines=body5)
    @test_throws ArgumentError iCSV.read(file5)
end

@testset "Enhanced DimArray options" begin
    # 2DTIMESERIES without index column -> rowdim fallback to Y
    d1 = DateTime(2024,1,1,10)
    d2 = DateTime(2024,1,2,10)
    df1 = DataFrame(var1 = [1.0,2.0,3.0], var2 = [10.0,20.0,30.0])
    df2 = DataFrame(var1 = [1.5,2.5,3.5], var2 = [15.0,25.0,35.0])
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINTZ(7 8 9)"
    metadata[:srid] = "EPSG:2056"
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["var1","var2"]
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    p = ICSV2DTimeseries(meta_section, fields_section, geometry, [df1, df2], [d1,d2])
    A = iCSV.todimarray(p)
    @test size(A) == (3, 2, 2)
    @test DimensionalData.dims(A)[1] isa DimensionalData.Y
    # idxcol override with missing symbol should still fall back
    A2 = iCSV.todimarray(p; idxcol=:layer_index)
    @test DimensionalData.dims(A2)[1] isa DimensionalData.Y

    # drop_non_numeric flag behavior for 2DTIMESERIES
    df1b = DataFrame(layer_index=1:3, num=[1,2,3], str=["a","b","c"])
    df2b = DataFrame(layer_index=1:3, num=[2,3,4], str=["d","e","f"])
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINTZ(7 8 9)"
    metadata[:srid] = "EPSG:2056"
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["layer_index","num","str"]
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    p2 = ICSV2DTimeseries(meta_section, fields_section, geometry, [df1b, df2b], [d1,d2])
    A3 = iCSV.todimarray(p2) # default drop_non_numeric=true
    @test size(A3) == (3, 1, 2) # only numeric field kept
    A4 = iCSV.todimarray(p2; drop_non_numeric=false)
    @test size(A4) == (3, 2, 2) # numeric + string fields

    # drop_non_numeric for ICSVBase
    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:3]
    dff = DataFrame(timestamp = ts, a = [1,2,3], s = ["x","y","z"])
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINTZ(7 8 9)"
    metadata[:srid] = "EPSG:2056"
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["timestamp","a","s"]
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    f = ICSVBase(meta_section, fields_section, geometry,dff)
    A5 = iCSV.todimarray(f) # drop non-numeric
    @test size(A5) == (3, 1)
    A6 = iCSV.todimarray(f; drop_non_numeric=false)
    @test size(A6) == (3, 3)
end

@testset "ICSV 2DTIMESERIES read/write & conversions" begin
    tmp = mktempdir()
    file = joinpath(tmp, "profile.icsv")
    d1 = DateTime(2024,1,1,10)
    d2 = DateTime(2024,1,2,10)
    df1 = DataFrame(layer_index = 1:3, var1 = [1.0,2.0,3.0], var2 = [10.0, 20.0, 30.0])
    df2 = DataFrame(layer_index = 1:3, var1 = [1.5,2.5,3.5], var2 = [15.0, 25.0, 35.0])

    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINT(600000 200000)"
    metadata[:srid] = "EPSG:2056"
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["layer_index","var1","var2"]
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    p = ICSV2DTimeseries(meta_section, fields_section, geometry, [df1, df2], [d1,d2])
    iCSV.write(p, file)

    q = iCSV.read(file)
    @test q isa ICSV2DTimeseries
    @test length(q.dates) == 2
    A = iCSV.todimarray(q)
    @test size(A) == (3, 2, 2) # layers, fields, time
    df_long = iCSV.todataframe(q)
    @test :time in Symbol.(names(df_long))
    @test length(unique(df_long[!, "time"])) == 2

    # append a timepoint
    d3 = DateTime(2024,1,3,10)
    df3 = DataFrame(layer_index = 1:3, var1 = [2.0,3.0,4.0], var2 = [12.0, 22.0, 32.0])
    iCSV.append_timepoint(file, d3, df3; field_delimiter=",")
    r = iCSV.read(file)
    @test length(r.dates) == 3
end
