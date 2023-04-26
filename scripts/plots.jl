using Plots 
using StatsPlots
using DataFrames
using CSV

Big   = "Logs/LogTestBig1-2-3_copy.csv"
Small = "Logs/LogTest1-2-3_copy2.csv"
Cash  = "Logs/LogTestCash.csv"
df = CSV.File(Big) |> DataFrame

@df df plot(:time_read, 
            :time_write, 
            group = :compresion_type, 
            xlabel = "Time Read, c",
            ylabel = "Time Write, c",
            marker = :circle)
savefig("Plots/BigWrite_t(read_t).png")

@df df plot(:quant_points, 
            :compression_coef, 
            group = :compresion_type,
            xlabel = "Quant Points",
            ylabel = "Compression Coef", 
            marker = :circle)
savefig("Plots/BigCompression_coef(quant_points).png")

@df df plot(:read_interval_points, 
            :read_interval_time, 
            group = :compresion_type, 
            xlabel = "Read Interval Points",
            ylabel = "Read INterval TIme, c",
            marker = :circle)
savefig("Plots/BigRead_interval_time(read_interval_points).png")

@df df plot(:read_interval_points, 
            :read_interval_time, 
            group = :compresion_type, 
            xlabel = "X",
            ylabel = "Y",
            marker = :circle)

groupby(df, :compresion_type)
