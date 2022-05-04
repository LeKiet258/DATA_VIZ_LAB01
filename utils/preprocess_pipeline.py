import pandas as pd
import numpy as os

def preprocess_pipeline(d):
    df = pd.read_csv(f'./Worldometer-data/table_{d}_raw.csv')
    df.drop(columns='Unnamed: 0', inplace=True)
    df.rename(columns={'Country, Other': 'Country', 'Serious, Critical': 'Serious'}, inplace=True)

    del_cols = ['Diamond Princess', 'MS Zaandam', 'World', 'Total:']
    df = df[~df['Country'].isin(del_cols)]
    df.drop(columns=['Deaths/1M pop', 'Tot Cases/1M pop', 'Tests/1M pop'], inplace=True)

    df[['New Cases', 'New Deaths', 'New Recovered']] = df[['New Cases', 'New Deaths', 'New Recovered']].applymap(lambda x: x if x != x else float(x[1:].replace(',', '')))
    cols = list(df)
    num_cols = list(set(cols).difference(set(['New Cases', 'New Deaths', 'New Recovered', 'Country'])))
    df[num_cols] = df[num_cols].applymap(lambda x: x if x != x else float(x.replace(',', '')))

    df[['New Cases', 'New Deaths', 'New Recovered', 'Serious', 'Total Deaths']] = \
        df[['New Cases', 'New Deaths', 'New Recovered', 'Serious', 'Total Deaths']].fillna(0)

    # Thêm cột `continent`
    continent_df = pd.read_csv(f'./utils/continents.csv').rename(columns={'Unnamed: 0': 'Country'})
    df = df.merge(continent_df, on='Country', how='inner')

    # điền khuyết cho 2 cột 'Total Recovered', 'Total Tests'
    mean_filler = df.groupby('continent')[['Total Recovered', 'Total Tests']].transform(lambda x: x.fillna(x.median()))
    df[['Total Recovered', 'Total Tests']] = mean_filler[['Total Recovered', 'Total Tests']]
    df.head()

    # điền khuyết cho 2 cột 'Active Cases'
    fval = df['Total Cases'] - df['Total Deaths'] - df['Total Recovered']
    fval[fval < 0] = 0
    df['Active Cases'] = df['Active Cases'].fillna(fval)

    return df


def add_density(input_df:pd.DataFrame):
    temp_df = pd.read_csv(f'./utils/population_density.csv')
    pre_df = input_df.join(other=temp_df.set_index('Country'), on='Country')

    print("Add Column Density Successfully")
    return pre_df