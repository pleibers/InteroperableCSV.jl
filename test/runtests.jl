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

    f = ICSVBase()
    iCSV.set_attribute!(f.metadata, "field_delimiter", ",")
    iCSV.set_attribute!(f.metadata, "geometry", "POINTZ(7 8 9)")
    iCSV.set_attribute!(f.metadata, "srid", "EPSG:2056")
    iCSV.setdata!(f, df)

    iCSV.writeicsv(f, file)
    g = iCSV.readicsv(file)
    @test g isa ICSVBase
    @test size(iCSV.todataframe(g)) == size(df)
    @test names(iCSV.todataframe(g)) == names(df)
    @test !any(ismissing, g.data.timestamp)
    @test all(g.data.a .== df.a)
    @test all(g.data.b .== df.b)
    @test all(g.data.timestamp .== df.timestamp)

    loc = iCSV.get_location(g.geometry)
    @test loc.x == 7.0 && loc.y == 8.0 && loc.z == 9.0 && loc.epsg == 2056

    A = iCSV.todimarray(g)
    @test size(A) == (nrow(df), 2) # drop timestamp column
end

@testset "Enhanced DimArray options" begin
    # 2DTIMESERIES without index column -> rowdim fallback to Y
    d1 = DateTime(2024,1,1,10)
    d2 = DateTime(2024,1,2,10)
    df1 = DataFrame(var1 = [1.0,2.0,3.0], var2 = [10.0,20.0,30.0])
    df2 = DataFrame(var1 = [1.5,2.5,3.5], var2 = [15.0,25.0,35.0])
    p = ICSV2DTimeseries()
    iCSV.set_attribute!(p.metadata, "field_delimiter", ",")
    iCSV.set_attribute!(p.fields, "fields", ["var1","var2"])
    iCSV.setdata!(p, d1, df1)
    iCSV.setdata!(p, d2, df2)
    A = iCSV.todimarray(p)
    @test size(A) == (3, 2, 2)
    @test DimensionalData.dims(A)[1] isa DimensionalData.Y
    # idxcol override with missing symbol should still fall back
    A2 = iCSV.todimarray(p; idxcol=:layer_index)
    @test DimensionalData.dims(A2)[1] isa DimensionalData.Y

    # drop_non_numeric flag behavior for 2DTIMESERIES
    df1b = DataFrame(layer_index=1:3, num=[1,2,3], str=["a","b","c"])
    df2b = DataFrame(layer_index=1:3, num=[2,3,4], str=["d","e","f"])
    p2 = ICSV2DTimeseries()
    iCSV.set_attribute!(p2.metadata, "field_delimiter", ",")
    iCSV.set_attribute!(p2.fields, "fields", ["layer_index","num","str"])
    iCSV.setdata!(p2, d1, df1b)
    iCSV.setdata!(p2, d2, df2b)
    A3 = iCSV.todimarray(p2) # default drop_non_numeric=true
    @test size(A3) == (3, 1, 2) # only numeric field kept
    A4 = iCSV.todimarray(p2; drop_non_numeric=false)
    @test size(A4) == (3, 2, 2) # numeric + string fields

    # drop_non_numeric for ICSVBase
    ts = [DateTime(2024,1,1) + Day(i-1) for i in 1:3]
    dff = DataFrame(timestamp = ts, a = [1,2,3], s = ["x","y","z"])
    f = ICSVBase()
    iCSV.set_attribute!(f.metadata, "field_delimiter", ",")
    iCSV.setdata!(f, dff)
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

    p = ICSV2DTimeseries()
    iCSV.set_attribute!(p.metadata, "field_delimiter", ",")
    iCSV.set_attribute!(p.metadata, "geometry", "POINT(600000 200000)")
    iCSV.set_attribute!(p.metadata, "srid", "EPSG:2056")
    iCSV.set_attribute!(p.fields, "fields", ["layer_index","var1","var2"])
    iCSV.setdata!(p, d1, df1)
    iCSV.setdata!(p, d2, df2)
    iCSV.writeicsv(p, file)

    q = iCSV.readicsv(file)
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
    r = iCSV.readicsv(file)
    @test length(r.dates) == 3
end
