import petl as etl

def get_delta(source_table, target_table):
    source_table_headers = etl.header(source_table)
    target_table_headers = etl.header(target_table)

    if source_table_headers != target_table_headers:
        raise Exception('Source table columns do not match target table columns')

    source_ids = etl.cut(source_table, 'id')
    target_ids = etl.cut(target_table, 'id')
    added_ids_table, _ = etl.diff(source_ids, target_ids)

    merged_table = etl.merge(source_table,
                            target_table,
                            key='id')

    load_table = etl.selectin(target_table, 'id', etl.values(added_ids_table, 'id'))

    for row in etl.data(merged_table):
        for i, col in enumerate(row):
            if isinstance(col, etl.transform.reductions.Conflict):
                changes = tuple(col)
                print('For car {}, {} changed from {} to {}'
                    .format(row[0], source_table_headers[i], changes[0], changes[1]))
                updated_row = list(row)
                updated_row[i] = changes[0]
                load_table = etl.merge(load_table, [source_table_headers, updated_row], key='id')
                break
    print(etl.lookall(load_table))
