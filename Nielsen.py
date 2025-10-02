from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd


def create_temp_engine():
    """
    Create connection
    """

    engine = create_engine(
        'postgresql://niq_test_user:niq_test_pwd@rc1d-9nhcng0zw57wke57.mdb.yandexcloud.net:6432/niq_test_db')

    return engine


request = """SELECT public.sales.sale_id, public.sales.store_id, public.sales.period_id, public.sales.sales_volume
             FROM public.sales
             LEFT JOIN public.store_chars ON sales.store_id=store_chars.store_id
             LEFT JOIN public.store_types ON store_chars.store_type_id=store_types.type_id
             WHERE store_types.type_name='supermarkets' """

engine = create_temp_engine()
with engine.connect() as con:
    df = pd.read_sql(text(request), con)
engine.dispose()


def create_promo_period(df:pd.DataFrame) -> pd.DataFrame:
    df['promo_period'] = (df.groupby('store_id')['period_id'].diff() > 1).cumsum()
    return df


def first_task(df:pd.DataFrame) -> int:
    return df['promo_period'].nunique()


def second_task(df:pd.DataFrame):
    promo_period_dur = df.groupby(['store_id', 'promo_period'])['period_id'].agg(['min', 'max'])
    promo_period_dur['duration'] = promo_period_dur['max'] - promo_period_dur['min']
    return promo_period_dur['duration'].median()


def third_task(df:pd.DataFrame):
    return df.groupby(['store_id', 'promo_period'])['sales_volume'].sum()


def fourth_task(df:pd.DataFrame):
    return df.groupby('store_id')['promo_period'].nunique().median()


df = df.sort_values(['store_id', 'period_id'])
df = create_promo_period(df)

print('Задание №1: ', first_task(df), sep='')  # Вывод: "Задание №1:  117658"
print('Задание №2: ', second_task(df), sep='')  # Вывод: "Задание №2:  2.0"
print('Задание №3: ', third_task(df), sep='\n')
"""Краткий вывод:
   store_id  promo_period
   4168621   0               86.3500
             1                9.9000
             2               15.8500
             3               18.2000
             4               48.3000
                               ...   
   38126908  117656           4.8000
   38126926  117656           3.9500
   38126935  117656           0.9500
             117657           4.6500
   38126938  117657           8.3765"""
print('Задание №4: ', fourth_task(df), sep='')
