import argparse
import glob, os

import pandas.io.parsers

out_dir = "output_data"
outdir = '/Users/davidakuma/projects/sainsbury-tech-interview/aspire-data-test-python-pandas'
path = os.path.join(outdir,out_dir) 
if not os.path.exists(path):
    os.mkdir(path)

def read_csv(csv_location: str):
    return pandas.read_csv(csv_location, header=0)


def read_json_folder(json_folder: str):
    transactions_files = glob.glob("{}*/*.json".format(json_folder))

    return pandas.concat(pandas.read_json(tf, lines=True) for tf in transactions_files)


def run_transformations(customers_location: str, products_location: str,
                        transactions_location: str, output_location: str):
    customers_df = read_csv(customers_location)
    products_df = read_csv(products_location)
    transactions_df = read_json_folder(transactions_location)

    transactions_df['basket'] = transactions_df['basket'].map(lambda x:[i['product_id'] for i in x])
    #Unpack the product id column
    transactions_df = transactions_df.explode('basket',ignore_index=True)
    transactions_df = transactions_df.rename(columns={'basket':'product_id'})
    

    #Count the number of items bought by each customer
    purchase_table = transactions_df.groupby(["customer_id","product_id"]).size().reset_index(name="purchase_count")

    #Merge the our purchase df with the original product table 
    product_merged = purchase_table.merge(products_df,left_on="product_id",right_on="product_id")

    #Merge the customer table
    full_merge = product_merged.merge(customers_df,left_on="customer_id",right_on="customer_id")

    #Extract our required table
    all_customers = full_merge[["customer_id","loyalty_score","product_id","product_category","purchase_count"]]

    all_customers.to_csv('output_data/output.csv')

    return all_customers


def get_latest_transaction_date(transactions):
    latest_purchase = transactions.date_of_purchase.max()
    latest_transaction = transactions[transactions.date_of_purchase == latest_purchase]
    return latest_transaction


def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')

# print(customers_df)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/")
    args = vars(parser.parse_args())

    run_transformations(args['customers_location'], args['products_location'],
                        args['transactions_location'], args['output_location'])
