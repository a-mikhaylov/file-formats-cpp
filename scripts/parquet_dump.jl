# using LocalRegistry
# using Pkg; Pkg.add("Parquet2")
# using Parquet2: Dataset

base_dir = raw"/media/oldwizzard/kotel/_work/git/file-formats-cpp"
file = base_dir * raw"/$EncodeDELTA_LR_BIN_PACK_data/small-1024-GZIP.parquet"
ds = Dataset(file)
rg = ds[1]
col = rg["FR"]
dump(col)