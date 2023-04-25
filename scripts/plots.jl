using Plots 
using StatsPlots
using DataFrames
using CSV

df = CSV.File("Logs/LogTest1-2-3_copy.csv") |> DataFrame
@df df plot(:time_read, 
            :time_write, 
            group = :compresion_type, 
            marker = :circle)
savefig("Plots/write_t(read_t).png")

@df df plot(:quant_points, 
            :compression_coef, 
            group = :compresion_type, 
            marker = :circle)
savefig("Plots/compression_coef(quant_points).png")

@df df plot(:read_interval_time, 
            :read_interval_points, 
            group = :compresion_type, 
            marker = :circle)
savefig("Plots/read_interval_time(read_interval_points).png")

