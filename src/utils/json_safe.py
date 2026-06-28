import pandas as pd
import numpy as np
from datetime import date, datetime
from decimal import Decimal


def _convert_value(v):
    # Converte qualquer tipo não serializável para tipos JSON seguros.

    if isinstance(v, (np.integer, int)):
        return int(v)

    if isinstance(v, (np.floating, float)):
        if np.isnan(v):
            return None
        return float(v)

    if isinstance(v, Decimal):
        return float(v)

    if isinstance(v, (np.datetime64, datetime)):
        return str(pd.to_datetime(v))

    if isinstance(v, (np.bool_)):
        return bool(v)

    if isinstance(v, (date,)):
        return v.isoformat()

    if pd.isna(v):
        return None

    return v


def json_safe_dataframe(df: pd.DataFrame):
    # Converte todo o DataFrame para tipos JSON-compatíveis.
    safe_df = df.copy()

    for col in safe_df.columns:
        safe_df[col] = safe_df[col].apply(_convert_value)

    return safe_df
