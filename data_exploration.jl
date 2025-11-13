using Pkg
Pkg.activate(".")

using CSV
using DataFrames
using Plots
using Dates

# Load the dataset
n_sensors=161

# for i in 1:n_sensors
for i in 2:2
    file_path = "Powers/$(i).csv"
    df = CSV.read(file_path, DataFrame)
    df[!,"serial_no"].=i
    df[!, "Time"] = DateTime.(df[!, "Time"], dateformat"dd-uuu-yyyy HH:MM:SS.sss")
    sort!(df, :Time)
    println("Sensor $(i) data:")
    println(first(df, 5))
    df_plot = df[1:1000, :]
    new_plt=Plots.plot(df_plot.Time, df_plot.Pplus_kW_, label="Sensor $(i)", xlabel="Time", ylabel="Active Power (Pplus_kW_) (kW)", title="Active Power over Time for Sensor $(i)")
    display(new_plt)
end

