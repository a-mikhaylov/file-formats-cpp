using Plots 
using StatsPlots
using DataFrames
using DataFramesMeta
using CSV

Big   = "Logs/LogTestBig1-2-3_copy.csv"
Small = "Logs/LogTest1-2-3_copy2.csv"
Shuffle  = "Logs/LogShuffle1k.csv"
file = "Logs/LogEncodeBig.csv"
df = CSV.File(file) |> DataFrame

# @df df plot(cols(:time_read))

@df df plot(:quant_points, 
            :time_write, 
            group = :compresion_type, 
            xlabel = "Quant point",
            ylabel = "Time Write, c",
            marker = :circle)
savefig("Plots/Big_WriteT(QuantPoints).png")

@df df plot(:quant_points, 
            :time_read, 
            group = :compresion_type, 
            xlabel = "Quant points",
            ylabel = "Time Read, c",
            marker = :circle)
savefig("Plots/BigRead_t(quant_points).png")

@df df plot(:quant_points, 
            :compression_coef, 
            group = :compresion_type,
            xlabel = "Quant Points",
            ylabel = "Compression Coef", 
            marker = :circle)
savefig("Plots/Big_CompressionCoef(QuantPoints).png")

@df df plot(:read_interval_points, 
            :read_interval_time, 
            group = :compresion_type, 
            xlabel = "Read Interval Points",
            ylabel = "Read INterval TIme, c",
            marker = :circle)
savefig("Plots/Big_ReadIntervalTime(ReadIntervalPoints).png")

@df df plot(:quant_points, 
            :time_read_quant, 
            group = :compresion_type, 
            xlabel = "Quant points",
            ylabel = "Time Quant Read, c",
            marker = :circle)
savefig("Plots/Big_TimeQuantRead(QuantPoints).png")
# ---------------------------------------------------------------------------
@df df plot(:read_interval_points, 
            :read_interval_time, 
            group = :compresion_type, 
            xlabel = "X",
            ylabel = "Y",
            marker = :circle)

groupby(df, :compresion_type)

@df df plot(:quant_points, 
            :time_read, 
            group = :compresion_type, 
            xlabel = "Quant points",
            ylabel = "Time Rand Read, c",
            marker = :circle)
savefig("Plots/1MSmallRandRead_t(quant_points).png")

