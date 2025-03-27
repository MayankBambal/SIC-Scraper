import pandas as pd

def update_sic_from_file(file1, file2):
    """
    Updates SIC_code in file1 using non-zero SIC_code values from file2,
    matching on cik_str. Only non-zero SIC_codes from file2 are applied.
    file1 is updated in-place.

    Args:
        file1 (str): Path to the main CSV file to update (will be modified).
        file2 (str): Path to the CSV file containing updated SIC_codes.
    """
    # Read both files
    df_main = pd.read_csv(file1)
    df_sic = pd.read_csv(file2)

    # Ensure cik_str is properly formatted
    df_main['cik_str'] = df_main['cik_str'].astype(str).str.zfill(10)
    df_sic['cik_str'] = df_sic['cik_str'].astype(str).str.zfill(10)

    # Keep only non-zero SIC_code rows from file2
    df_sic['SIC_code'] = df_sic['SIC_code'].fillna(0).astype(float).astype(int)
    df_sic_nonzero = df_sic[df_sic['SIC_code'] != 0][['cik_str', 'SIC_code']]

    # Set index for fast lookup
    # Set index for fast lookup and drop duplicate cik_str values
    df_sic_nonzero.set_index('cik_str', inplace=True)
    df_sic_nonzero = df_sic_nonzero[~df_sic_nonzero.index.duplicated(keep='last')]


    # Update SIC_code in df_main
    def get_updated_sic(row):
        return df_sic_nonzero.loc[row['cik_str'], 'SIC_code'] \
            if row['cik_str'] in df_sic_nonzero.index else row.get('SIC_code', 0)

    df_main['SIC_code'] = df_main.apply(get_updated_sic, axis=1)

    # Save back to file1
    df_main.to_csv(file1, index=False)
    print(f"Updated SIC_code values saved to: {file1}")
