# import libraries
import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    """ Load disaster messages and categories into dataframe.

    Args:
        messages_filepath: String. This is a csv file and contains disaster messages.
        categories_filepath: String. This is a csv file and contains disaster categories for each messages.

    Returns:
       pandas.DataFrame
    """

# load messages dataset
    message_df = pd.read_csv(messages_filepath, keep_default_na=False, na_values=[""])
    
# load categories dataset
    categories_df = pd.read_csv(categories_filepath, keep_default_na=False, na_values=[""])
    
# merge datasets
    df=pd.merge(left=message_df, right=categories_df, left_on='id', right_on='id')
    
    return df

def clean_data(df):
    """Clean data included in the DataFrame and transform categories part
    INPUT
    df -- type pandas DataFrame
    OUTPUT
    df -- cleaned pandas DataFrame
    """
# create a dataframe of the 36 individual category columns
    categories =df['categories'].str.split(";" ,expand=True)
    categories.head()
# select the first row of the categories dataframe
    categories_colnames= categories.rename(columns=categories.iloc[0])
    categories_colnames.info()
#remove -, 0 and 1 from the column names
    categories_colnames.columns =categories_colnames.columns.str.rstrip('-0')
    categories_colnames.columns =categories_colnames.columns.str.rstrip('-1')
    categories_colnames.head()
# rename the columns of `categories`with prefix category_
    categories_columns= categories_colnames.add_prefix('category_')
    categories_columns.info()
# set each value of the columns to be the last character of the string
    for column in categories_columns:
       categories_columns[column] = categories_columns[column].str[-1]  
    categories_columns.head()    
# convert column from string to numeric
    categories_columns =categories_columns.apply(pd.to_numeric) # convert all columns of DataFrame
    categories_columns.head()
# Convert all value into binary (0 or 1)
    categories = (categories_columns > 0).astype(int)
# drop the original categories column from df
    df.drop('categories',axis='columns', inplace=True)
    df.head()
# concatenate the original dataframe with the new `categories` dataframe
    df = pd.concat([df, categories], axis=1)
    df.head()
# check number of duplicates
    print(df.duplicated().sum())  
# drop duplicates
    df = df.drop_duplicates()
    df
# check again the number of duplicates 
    print(df.duplicated().sum()) 
   
    return df



#Save the clean dataset into an sqlite database.

engine = create_engine('sqlite:///DisasterResponse.db')
df.to_sql('Disasters', engine, index=False, if_exists='replace', chunksize=600)

def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
