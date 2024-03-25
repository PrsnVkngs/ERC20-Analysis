import cudf


def make_efficient(df: cudf.DataFrame):
    df['block_number'] = df['block_number'].astype('category')
    df['transaction_index'] = df['transaction_index'].astype('category')
    df['from_address'] = df['from_address'].astype('category')
    df['to_address'] = df['to_address'].astype('category')
    df['contract_address'] = df['contract_address'].astype('category')
    df['value'] = cudf.to_numeric(df['value'], downcast='float')

    return df
