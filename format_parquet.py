import pyarrow.csv as pv
import pyarrow.parquet as pq

filename = 'month.csv'

table = pv.read_csv(filename)
pq.write_table(table, filename.replace('csv', 'parquet'))