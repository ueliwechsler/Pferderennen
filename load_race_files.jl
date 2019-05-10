using DelimitedFiles
using Dates
using DataFrames


REGEX_date =  r"^([0-2][0-9]|(3)[0-1])(.)(((0)[0-9])|((1)[0-2]))(.)\d{4}$"
labels_horse = ["Cl", "No", "Retr", "Cheval", "Ec.", "Jockey", "Poids", "Oeil", "Fers", "Cote", "Longueur", "Etat"]
labels_wheels = ["Cl", "No", "Retr", "Cheval", "Ec.", "Driver", "Cote", "Temps", "Redk", "Longueur", "Distance"]
LABELS = Symbol.(unique([labels_wheels; labels_horse]))


function load_race(file_path::String)
    data = []
    data_raw = readdlm(file_path)
    for i=1:size(data_raw,1)
        push!(data,join(data_raw[i,:],' '))
    end
    data = strip.(data)

    date = match(REGEX_date, "01.01.2018").match
    race_info = "Run $(data_raw[2,3]): $(data[3])"

    row_header = 10
    header = split(data[row_header])
    n_columns = length(header)
    n_participants = parse(Int64,split(data[7]," ")[1])
    col_data = Array{String}(undef, n_participants, n_columns)
    for col = 1:n_columns
        start_col = (col-1)*n_participants + 1 + row_header-1
        for horse=1:n_participants
            col_data[horse, col] =  data[start_col + horse]
        end
    end

    race = DataFrame(col_data, Symbol.(header))

    for label in LABELS
        if !(label in names(race))
            race[label] = ""
        end
    end

    race[:race] = race_info
    race[:date] = date
    return race
end

dir = @__DIR__
data_dir = joinpath(dir, "data")
db_path = joinpath(data_dir, "db.csv")
file_dir = joinpath(data_dir,"september_2018")

files = joinpath.(file_dir,readdir(file_dir))
df = vcat(load_race.(files)...)

# df = vcat(load_race(files[2]))

using Feather
using CSV

CSV.write(db_path, df)

df = CSV.read(db_path)
idx = df[:Cheval] .== "ENATTOF"
df[idx,:]

names(df)
