import pandas as pd
import os

temp_df = pd.read_csv('preprocessed_density.csv')
temp_df.head()

for i in os.listdir('Worldometer-data-preprocessed'):
    data_df = pd.read_csv(f'Worldometer-data-preprocessed/{i}')

    pre_df = data_df.join(other=temp_df.set_index('Country'), on='Country')
    
    pre_df.to_csv(f'Worldometer-data-preprocessed/{i}', index=False)