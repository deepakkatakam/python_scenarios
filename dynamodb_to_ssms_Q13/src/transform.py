import pandas as pd

def transform_data(data):
    df = pd.DataFrame(data)

    df = df.sort_values(by=['project_id', 'status'])

    return df
