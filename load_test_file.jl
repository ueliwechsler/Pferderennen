using DelimitedFiles
using Dates
using DataFrames


REGEX_date =  r"^([0-2][0-9]|(3)[0-1])(.)(((0)[0-9])|((1)[0-2]))(.)\d{4}$"
labels_horse = ["Cl", "No", "Retr", "Cheval", "Ec.", "Jockey", "Poids", "Oeil", "Fers", "Cote", "Longueur", "Etat"]
labels_wheels = ["Cl", "No", "Retr", "Cheval", "Ec.", "Driver", "Cote", "Temps", "Redk", "Longueur", "Distance"]
LABELS = Symbol.(unique([labels_wheels; labels_horse]))

fileName = "test_file_2.jl"
filePath = joinpath(dir,fileName)
isfile(filePath)


data_raw = readdlm(filePath)
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
    if !(label in keys(race))
        race[label] = ""
    end
end

race[:race] = race_info
race[:date] = date
