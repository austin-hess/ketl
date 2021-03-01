import petl as etl
from get_delta import get_delta
import config

table1 = etl.fromcsv(config.SOURCE_PATH)
table2 = etl.fromcsv(config.TARGET_PATH)

result_table = get_delta(table1, table2)

print(etl.lookall(result_table))
