using InteroperableCSV
using Test
using DataFrames
using Dates
using DimensionalData
metadata = Dict{Symbol, String}(:field_delimiter => ",", :geometry => "POINT(1 2)", :srid => "EPSG:2056")
fields = Dict{Symbol, Vector{String}}(:fields => ["timestamp","a","b"]) 
meta_section = MetaDataSection(;metadata...)
    
@testset "Flexible constructors: ICSVBase" begin
    metadata = Dict{Symbol, String}(:field_delimiter => ",", :geometry => "POINT(1 2)", :srid => "EPSG:2056")
    fields = Dict{Symbol, Vector{String}}(:fields => ["timestamp","a","b"]) 
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])

    # from Matrix
    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:3]
    mat = hcat(ts, collect(1:3), collect(10:12))
    fM = ICSVBase(meta_section, fields_section, geometry, mat)
    @test fM isa ICSVBase
    @test Symbol.(names(fM.data)) == Symbol.(fields[:fields])
    @test all(fM.data.a .== 1:3) && all(fM.data.b .== 10:12)

    # from Dict with Symbol keys
    dS = Dict(:timestamp=>ts, :a=>[2,3,4], :b=>[20,30,40])
    fD = ICSVBase(meta_section, fields_section, geometry, dS)
    @test fD isa ICSVBase
    @test Symbol.(names(fD.data)) == Symbol.(fields[:fields])
    @test all(fD.data.a .== [2,3,4]) && all(fD.data.b .== [20,30,40])

    # from Dict with String keys
    dStr = Dict("timestamp"=>ts, "a"=>[5,6,7], "b"=>[50,60,70])
    fDS = ICSVBase(meta_section, fields_section, geometry, dStr)
    @test Symbol.(names(fDS.data)) == Symbol.(fields[:fields])
    @test all(fDS.data.a .== [5,6,7])
end

@testset "Flexible constructors: ICSV2DTimeseries" begin
    d1 = DateTime(2024,1,1,0)
    d2 = DateTime(2024,1,2,0)
    metadata = Dict{Symbol, String}(:field_delimiter => ",", :geometry => "POINT(1 2)", :srid => "EPSG:2056")
    fields = Dict{Symbol, Vector{String}}(:fields => ["layer_index","var1","var2"]) 
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])

    # Dict{DateTime, Matrix}
    M1 = hcat(collect(1:3), [1.0,2.0,3.0], [10.0,20.0,30.0])
    M2 = hcat(collect(1:3), [1.5,2.5,3.5], [15.0,25.0,35.0])
    dm = Dict(d1=>M1, d2=>M2)
    tsm = ICSV2DTimeseries(meta_section, fields_section, geometry, dm, [d1,d2])
    @test tsm isa ICSV2DTimeseries
    @test all(Symbol.(names(tsm.data[d1])) .== [:layer_index,:var1,:var2])

    # Vector of Dicts
    dct1 = Dict(:layer_index=>1:2, :var1=>[1.0,2.0], :var2=>[10.0,20.0])
    dct2 = Dict(:layer_index=>1:3, :var1=>[1.5,2.5,3.5], :var2=>[15.0,25.0,35.0])
    tsv = ICSV2DTimeseries(meta_section, fields_section, geometry, [dct1, dct2], [d1,d2])
    @test nrow(tsv.data[d1]) == 2
    @test nrow(tsv.data[d2]) == 3
end
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
    InteroperableCSV.write(f, file)

    g = InteroperableCSV.read(file)
    @test g isa ICSVBase
    @test size(InteroperableCSV.todataframe(g)) == size(df)
    @test names(InteroperableCSV.todataframe(g)) == names(df)
    @test !any(ismissing, g.data.timestamp)
    @test all(g.data.a .== df.a)
    @test all(g.data.b .== df.b)
    @test all(g.data.timestamp .== df.timestamp)

    loc = g.geolocation.location
    epsg = g.geolocation.epsg
    @test loc.x == 7.0 && loc.y == 8.0 && loc.z == 9.0 && epsg == 2056

    A = InteroperableCSV.todimarray(g)
    @test size(A) == (nrow(df), 2) # drop timestamp column
end

@testset "ICSV 2DTIMESERIES edge cases" begin
    tmp = mktempdir()
    function write_manual_timeseries(path::AbstractString; meta=Dict("field_delimiter"=>",","geometry"=>"POINT(1 2)","srid"=>"EPSG:2056"), fields::AbstractString="a,b", body_lines::Vector{String})
        open(path, "w") do io
            println(io, InteroperableCSV.FIRSTLINES_2DTIMESERIES[end])
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
    q1 = InteroperableCSV.read(file1)
    @test q1 isa InteroperableCSV.ICSV2DTimeseries
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
    @test_throws ArgumentError InteroperableCSV.read(file2)

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
    q3 = InteroperableCSV.read(file3)
    @test length(q3.dates) == 2
    @test nrow(q3.data[q3.dates[1]]) == 2
    @test nrow(q3.data[q3.dates[2]]) == 1

    # 4) Missing [DATE] marker should error
    file4 = joinpath(tmp, "missing_date.icsv")
    body4 = [
        "1,2",
    ]
    write_manual_timeseries(file4; fields="a,b", body_lines=body4)
    @test_throws ArgumentError InteroperableCSV.read(file4)

    # 5) Column mismatch should error
    file5 = joinpath(tmp, "mismatch_columns.icsv")
    body5 = [
        "[DATE=2024-01-01T00:00:00]",
        "1,2",
    ]
    write_manual_timeseries(file5; fields="a,b,c", body_lines=body5)
    @test_throws ArgumentError InteroperableCSV.read(file5)
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
    A = InteroperableCSV.todimarray(p)
    @test size(A) == (3, 2, 2)
    @test DimensionalData.dims(A)[1] isa DimensionalData.Y
    # idxcol override with missing symbol should still fall back
    A2 = InteroperableCSV.todimarray(p; idxcol=:layer_index)
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
    A3 = InteroperableCSV.todimarray(p2) # default drop_non_numeric=true
    @test size(A3) == (3, 1, 2) # only numeric field kept
    A4 = InteroperableCSV.todimarray(p2; drop_non_numeric=false)
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
    A5 = InteroperableCSV.todimarray(f) # drop non-numeric
    @test size(A5) == (3, 1)
    A6 = InteroperableCSV.todimarray(f; drop_non_numeric=false)
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
    InteroperableCSV.write(p, file)

    q = InteroperableCSV.read(file)
    @test q isa ICSV2DTimeseries
    @test length(q.dates) == 2
    A = InteroperableCSV.todimarray(q)
    @test size(A) == (3, 2, 2) # layers, fields, time
    df_long = InteroperableCSV.todataframe(q)
    @test :time in Symbol.(names(df_long))
    @test length(unique(df_long[!, "time"])) == 2

    # append a timepoint
    d3 = DateTime(2024,1,3,10)
    df3 = DataFrame(layer_index = 1:3, var1 = [2.0,3.0,4.0], var2 = [12.0, 22.0, 32.0])
    InteroperableCSV.append_timepoint(file, d3, df3; field_delimiter=",")
    r = InteroperableCSV.read(file)
    @test length(r.dates) == 3

    # append matrix
    d4 = DateTime(2024,1,4,10)
    mat4 = hcat(collect(1:3), [2.5,3.5,4.5], [12.5, 22.5, 32.5])
    InteroperableCSV.append_timepoint(file, d4, mat4; field_delimiter=",")
    r2 = InteroperableCSV.read(file)
    @test length(r2.dates) == 4

    # append dict (Symbol keys)
    d5 = DateTime(2024,1,5,10)
    dict5 = Dict(:layer_index=>1:2, :var1=>[3.0,4.0], :var2=>[13.0, 23.0])
    InteroperableCSV.append_timepoint(file, d5, dict5; field_delimiter=",")
    r3 = InteroperableCSV.read(file)
    @test length(r3.dates) == 5
end

@testset "Metadata parsing from strings" begin
    # Test that metadata fields are correctly parsed from string values (as they come from files)
    # This was missing and caused real-world files to fail
    
    # nodata as string -> numeric
    md1 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", nodata="-999")
    @test md1.nodata === -999
    @test md1.nodata isa Int
    
    md2 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", nodata="-999.0")
    @test md2.nodata === -999.0
    @test md2.nodata isa Float64
    
    # timezone as string -> numeric
    md3 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", timezone="1")
    @test md3.timezone === 1
    @test md3.timezone isa Int
    
    md4 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", timezone="1.5")
    @test md4.timezone === 1.5
    @test md4.timezone isa Float64
    
    # timezone as string name (non-numeric)
    md5 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", timezone="UTC")
    @test md5.timezone == "UTC"
    @test md5.timezone isa String
    
    # station_id and other string fields
    md6 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", station_id="TEST123")
    @test md6.station_id == "TEST123"
    @test md6.station_id isa String
end

@testset "Read/write with metadata parsing" begin
    # Test round-trip with metadata that needs parsing
    tmp = mktempdir()
    file = joinpath(tmp, "metadata_test.icsv")
    
    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:3]
    df = DataFrame(timestamp = ts, a = [1, 2, -999], b = [10, 20, 30])
    
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINT(600000 200000)"
    metadata[:srid] = "EPSG:2056"
    metadata[:nodata] = "-999"  # String value as it would come from file
    metadata[:station_id] = "SITE01"
    metadata[:timezone] = "1.0"
    
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["timestamp","a","b"]
    
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    
    f = ICSVBase(meta_section, fields_section, geometry, df)
    InteroperableCSV.write(f, file)
    
    # Read back and verify metadata was parsed correctly
    g = InteroperableCSV.read(file)
    @test g.metadata.nodata === -999
    @test g.metadata.nodata isa Int
    @test g.metadata.station_id == "SITE01"
    @test g.metadata.timezone === 1.0
    @test g.metadata.timezone isa Float64
end

@testset "Metadata edge cases and errors" begin
    # Missing required fields should error
    @test_throws ArgumentError MetaDataSection(field_delimiter=",", geometry="POINT(1 2)")
    @test_throws ArgumentError MetaDataSection(field_delimiter=",", srid="EPSG:2056")
    @test_throws ArgumentError MetaDataSection(geometry="POINT(1 2)", srid="EPSG:2056")
    
    # Invalid SRID format - validation happens at Geometry construction
    @test_throws ArgumentError Geometry("POINT(1 2)", "invalid")
    @test_throws ArgumentError Geometry("POINT(1 2)", "EPSG:")
    @test_throws ArgumentError Geometry("POINT(1 2)", "2056")
    
    # Invalid geometry format - validation happens at Geometry construction
    @test_throws ArgumentError Geometry("POINT(1)", "EPSG:2056")  # only 1 coordinate
    @test_throws ArgumentError Geometry("POINTZ(1 2)", "EPSG:2056")  # POINTZ needs 3 coords
    @test_throws ArgumentError Geometry("POINT(a b)", "EPSG:2056")  # non-numeric coords
    @test_throws ArgumentError Geometry("POINTZ(1 2 c)", "EPSG:2056")  # non-numeric z
    
    # Invalid field types for parse_string_field
    @test_throws ArgumentError InteroperableCSV.parse_string_field(123)
    @test_throws ArgumentError InteroperableCSV.parse_string_field(1.5)
    @test_throws ArgumentError InteroperableCSV.parse_string_field(true)
    
    # Valid edge cases that should work
    md1 = MetaDataSection(field_delimiter=",", geometry="POINT(0 0)", srid="EPSG:4326")
    @test md1.geometry == "POINT(0 0)"
    geom1 = Geometry(md1.geometry, md1.srid)
    @test geom1.epsg == 4326
    
    md2 = MetaDataSection(field_delimiter=",", geometry="POINTZ(-180.0 -90.0 -1000.0)", srid="EPSG:4326")
    @test md2.geometry == "POINTZ(-180.0 -90.0 -1000.0)"
    geom2 = Geometry(md2.geometry, md2.srid)
    @test geom2.location.x == -180.0
    @test geom2.location.y == -90.0
    @test geom2.location.z == -1000.0
    
    # Empty/nothing values for optional fields should work
    md3 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", nodata=nothing)
    @test md3.nodata === nothing
    
    md4 = MetaDataSection(field_delimiter=",", geometry="POINT(1 2)", srid="EPSG:2056", timezone=nothing)
    @test md4.timezone === nothing
end

@testset "Fields section edge cases and errors" begin
    # Field validation errors
    fields = FieldsSection(fields=["a", "b", "c"])
    @test_throws ArgumentError InteroperableCSV.check_validity(fields, 2)  # mismatch: 3 fields, 2 columns
    @test_throws ArgumentError InteroperableCSV.check_validity(fields, 4)  # mismatch: 3 fields, 4 columns
    @test InteroperableCSV.check_validity(fields, 3)  # correct match
    
    # Recommended fields with wrong length should error
    fields2 = FieldsSection(fields=["a", "b"], units=["m", "kg", "s"])  # 2 fields, 3 units
    @test_throws ArgumentError InteroperableCSV.check_validity(fields2, 2)
    
    # Valid: empty recommended fields
    fields3 = FieldsSection(fields=["a", "b"], units=String[])
    @test InteroperableCSV.check_validity(fields3, 2)
    
    # Valid: matching recommended fields
    fields4 = FieldsSection(fields=["a", "b"], units=["m", "kg"])
    @test InteroperableCSV.check_validity(fields4, 2)
end

@testset "Data coercion edge cases and errors" begin
    metadata = Dict{Symbol, String}(:field_delimiter => ",", :geometry => "POINT(1 2)", :srid => "EPSG:2056")
    fields = Dict{Symbol, Vector{String}}(:fields => ["a", "b"])
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    
    # Matrix with wrong number of columns
    mat_wrong = hcat([1, 2, 3])  # 1 column, but 2 fields declared
    @test_throws ArgumentError ICSVBase(meta_section, fields_section, geometry, mat_wrong)
    
    # Dict missing required columns
    dict_missing = Dict(:a => [1, 2, 3])  # missing column 'b'
    @test_throws ArgumentError ICSVBase(meta_section, fields_section, geometry, dict_missing)
    
    # Dict with mismatched column lengths
    dict_mismatch = Dict(:a => [1, 2, 3], :b => [4, 5])  # different lengths
    @test_throws ArgumentError ICSVBase(meta_section, fields_section, geometry, dict_mismatch)
    
    # Dict with non-vector values
    dict_nonvec = Dict(:a => 123, :b => [4, 5, 6])
    @test_throws ArgumentError ICSVBase(meta_section, fields_section, geometry, dict_nonvec)
    
    # Valid edge case: empty DataFrame
    df_empty = DataFrame(a=Int[], b=Int[])
    f_empty = ICSVBase(meta_section, fields_section, geometry, df_empty)
    @test nrow(f_empty.data) == 0
end

@testset "Missing value parsing (nodata)" begin
    tmp = mktempdir()
    file = joinpath(tmp, "missing_test.icsv")
    
    # Test 1: Write and read with missing values using nodata=-999
    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:5]
    df = DataFrame(timestamp = ts, a = [1, missing, 3, missing, 5], b = [10, 20, missing, 40, 50])
    
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINT(600000 200000)"
    metadata[:srid] = "EPSG:2056"
    metadata[:nodata] = "-999"
    
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["timestamp","a","b"]
    
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    
    f = ICSVBase(meta_section, fields_section, geometry, df)
    InteroperableCSV.write(f, file)
    
    # Read back and verify missing values are parsed correctly
    g = InteroperableCSV.read(file)
    @test g.metadata.nodata === -999
    @test ismissing(g.data.a[2])
    @test ismissing(g.data.a[4])
    @test ismissing(g.data.b[3])
    @test g.data.a[1] == 1
    @test g.data.a[3] == 3
    @test g.data.a[5] == 5
    @test g.data.b[1] == 10
    @test g.data.b[2] == 20
    
    # Test 2: Write and read with float nodata
    file2 = joinpath(tmp, "missing_float.icsv")
    df2 = DataFrame(timestamp = ts, x = [1.5, missing, 3.5, 4.5, missing], y = [10.0, 20.0, 30.0, missing, 50.0])
    
    metadata2 = Dict{Symbol, String}()
    metadata2[:field_delimiter] = ","
    metadata2[:geometry] = "POINT(1 2)"
    metadata2[:srid] = "EPSG:2056"
    metadata2[:nodata] = "-999.0"
    
    fields2 = Dict{Symbol, Vector{String}}()
    fields2[:fields] = ["timestamp","x","y"]
    
    meta_section2 = MetaDataSection(;metadata2...)
    fields_section2 = FieldsSection(;fields2...)
    geometry2 = Geometry(metadata2[:geometry], metadata2[:srid])
    
    f2 = ICSVBase(meta_section2, fields_section2, geometry2, df2)
    InteroperableCSV.write(f2, file2)
    
    g2 = InteroperableCSV.read(file2)
    @test g2.metadata.nodata === -999.0
    @test ismissing(g2.data.x[2])
    @test ismissing(g2.data.x[5])
    @test ismissing(g2.data.y[4])
    @test g2.data.x[1] == 1.5
    @test g2.data.y[1] == 10.0
    
    # Test 3: No nodata specified - missing values written as empty string
    file3 = joinpath(tmp, "missing_nospec.icsv")
    df3 = DataFrame(timestamp = ts[1:3], a = [1, missing, 3], b = [10, 20, missing])
    
    metadata3 = Dict{Symbol, String}()
    metadata3[:field_delimiter] = ","
    metadata3[:geometry] = "POINT(1 2)"
    metadata3[:srid] = "EPSG:2056"
    
    fields3 = Dict{Symbol, Vector{String}}()
    fields3[:fields] = ["timestamp","a","b"]
    
    meta_section3 = MetaDataSection(;metadata3...)
    fields_section3 = FieldsSection(;fields3...)
    geometry3 = Geometry(metadata3[:geometry], metadata3[:srid])
    
    f3 = ICSVBase(meta_section3, fields_section3, geometry3, df3)
    InteroperableCSV.write(f3, file3)
    
    g3 = InteroperableCSV.read(file3)
    @test g3.metadata.nodata === nothing
    @test ismissing(g3.data.a[2])
    @test ismissing(g3.data.b[3])
end

@testset "Missing values in 2DTIMESERIES" begin
    tmp = mktempdir()
    file = joinpath(tmp, "timeseries_missing.icsv")
    
    d1 = DateTime(2024,1,1,10)
    d2 = DateTime(2024,1,2,10)
    df1 = DataFrame(layer_index = 1:3, var1 = [1.0, missing, 3.0], var2 = [10.0, 20.0, missing])
    df2 = DataFrame(layer_index = 1:3, var1 = [missing, 2.5, 3.5], var2 = [15.0, missing, 35.0])
    
    metadata = Dict{Symbol, String}()
    metadata[:field_delimiter] = ","
    metadata[:geometry] = "POINT(600000 200000)"
    metadata[:srid] = "EPSG:2056"
    metadata[:nodata] = "-999.0"
    
    fields = Dict{Symbol, Vector{String}}()
    fields[:fields] = ["layer_index","var1","var2"]
    
    meta_section = MetaDataSection(;metadata...)
    fields_section = FieldsSection(;fields...)
    geometry = Geometry(metadata[:geometry], metadata[:srid])
    
    p = ICSV2DTimeseries(meta_section, fields_section, geometry, [df1, df2], [d1,d2])
    InteroperableCSV.write(p, file)
    
    q = InteroperableCSV.read(file)
    @test q isa ICSV2DTimeseries
    @test q.metadata.nodata === -999.0
    @test ismissing(q.data[d1].var1[2])
    @test ismissing(q.data[d1].var2[3])
    @test ismissing(q.data[d2].var1[1])
    @test ismissing(q.data[d2].var2[2])
    @test q.data[d1].var1[1] == 1.0
    @test q.data[d2].var1[2] == 2.5
end

@testset "Read Real World File" begin
    file = InteroperableCSV.read("MST96.icsv")
    @test size(file.data) == (21985, 15)
    @test file.metadata.station_id == "MST96"
    @test file.metadata.geometry == "POINTZ(9.81 46.831 2540.0)"
    @test file.metadata.srid == "EPSG:4326"
end