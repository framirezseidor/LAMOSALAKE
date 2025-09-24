import pandas as pd
import numpy as np

def mean_past6Months(series: pd.Series, months: int) -> float:
    date_months_before = series.resample("W").sum().index[-1] - pd.Timedelta(4*months, unit="W")
    series_rem = series.loc[series.index >= date_months_before]
    return series_rem.resample("M").sum().mean()


# Serie de ventas diarias
rng = pd.date_range("2025-01-01", "2025-08-31", freq="D")
ventas = pd.Series(np.random.poisson(lam=100, size=len(rng)), index=rng)

# Promedio de las sumas mensuales en los últimos 6 "meses" (~24 semanas)
promedio = mean_past6Months(ventas, months=6)
print(ventas)    # -> pd.Series con ventas diarias
print(promedio)  # -> float (p.ej. ~3000-3500 si lam=100)

