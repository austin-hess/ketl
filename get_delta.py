import petl as etl
import pandas as pd

def get_delta(source_table, target_table, key='id'):
    source_table_headers = etl.header(source_table)
    target_table_headers = etl.header(target_table)

    if source_table_headers != target_table_headers:
        raise Exception('Source table columns do not match target table columns')

    source_ids = etl.cut(source_table, key)
    target_ids = etl.cut(target_table, key)
    added_ids_table, _ = etl.diff(source_ids, target_ids)

    merged_table = etl.merge(source_table,
                            target_table,
                            key=key)

    load_frame = etl.todataframe(etl.selectin(target_table, key, etl.values(added_ids_table, key)))
    print(load_frame)

    for row in etl.data(merged_table):
        for i, col in enumerate(row):
            if isinstance(col, etl.transform.reductions.Conflict):
                changes = tuple(col)
                print('For car {}, {} changed from {} to {}'
                    .format(row[0], source_table_headers[i], changes[1], changes[0]))
                row_dict = dict(zip(source_table_headers,list(row)))
                row_dict[source_table_headers[i]] = changes[0]
                row_dict = { key: [val] for (key,val) in row_dict.items() }
                print(row_dict)
                df = pd.DataFrame(row_dict)
                load_frame = load_frame.append(df, ignore_index=True)
                break
    
    return etl.fromdataframe(load_frame)
