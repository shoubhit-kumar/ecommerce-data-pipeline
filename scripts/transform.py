import pandas as pd
import numpy as np
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import os
import io

# Set up authentication - handle both local and GitHub Actions environments
if 'GITHUB_ACTIONS' in os.environ:
    # In GitHub Actions, authentication is handled by the auth action
    pass
else:
    # For local development only
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\ShoubhitKumar\OneDrive - IBM\Desktop\de-learn\ecommerce-data-pipeline\gcp-key.json"

def download_from_gcs(bucket_name):
    """Download CSV files from Google Cloud Storage"""
    print("📥 Downloading data from Google Cloud Storage...")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Download each file
    data_files = {}
    for filename in ['products.csv', 'customers.csv', 'orders.csv']:
        try:
            blob = bucket.blob(f'raw_data/{filename}')
            csv_data = blob.download_as_text()
            df = pd.read_csv(io.StringIO(csv_data))
            data_files[filename.replace('.csv', '')] = df
            print(f"✅ Downloaded {filename}: {len(df):,} records")
        except Exception as e:
            print(f"❌ Failed to download {filename}: {str(e)}")
            raise
    
    return data_files

def create_fact_orders(orders_df, products_df, customers_df):
    """Create fact table with all business metrics"""
    print("🔄 Creating fact_orders table...")
    
    # Start with orders as base
    fact_orders = orders_df.copy()
    
    # Add product dimensions
    product_dims = products_df[['product_id', 'name', 'category', 'brand', 'cost']].rename(columns={'name': 'product_name'})
    fact_orders = fact_orders.merge(product_dims, on='product_id', how='left')
    
    # Add customer dimensions
    customer_dims = customers_df[['customer_id', 'city', 'age_group', 'customer_segment']].rename(columns={'name': 'customer_name'})
    fact_orders = fact_orders.merge(customer_dims, on='customer_id', how='left')
    
    # Convert dates
    fact_orders['order_date'] = pd.to_datetime(fact_orders['order_date'])
    
    # Calculate business metrics
    fact_orders['gross_profit'] = (fact_orders['unit_price'] - fact_orders['cost']) * fact_orders['quantity']
    fact_orders['net_profit'] = fact_orders['gross_profit'] - fact_orders['discount_amount']
    fact_orders['profit_margin'] = (fact_orders['net_profit'] / fact_orders['item_total'] * 100).round(2)
    fact_orders['revenue_after_discount'] = fact_orders['item_total'] - fact_orders['discount_amount']
    
    # Add time dimensions
    fact_orders['year'] = fact_orders['order_date'].dt.year
    fact_orders['month'] = fact_orders['order_date'].dt.month
    fact_orders['quarter'] = fact_orders['order_date'].dt.quarter
    fact_orders['day_of_week'] = fact_orders['order_date'].dt.day_name()
    fact_orders['week_number'] = fact_orders['order_date'].dt.isocalendar().week
    fact_orders['is_weekend'] = fact_orders['order_date'].dt.weekday >= 5
    
    print(f"✅ Created fact_orders: {len(fact_orders):,} records")
    return fact_orders

def create_daily_sales_summary(fact_orders):
    """Create daily aggregated sales metrics"""
    print("🔄 Creating daily_sales_summary...")
    
    daily_summary = fact_orders.groupby(['order_date', 'category']).agg({
        'item_total': 'sum',
        'revenue_after_discount': 'sum',
        'net_profit': 'sum',
        'gross_profit': 'sum',
        'discount_amount': 'sum',
        'quantity': 'sum',
        'order_id': 'nunique',
        'customer_id': 'nunique'
    }).round(2)
    
    daily_summary.columns = [
        'total_revenue', 'revenue_after_discount', 'net_profit', 'gross_profit',
        'total_discount', 'total_quantity', 'unique_orders', 'unique_customers'
    ]
    
    daily_summary = daily_summary.reset_index()
    daily_summary['profit_margin'] = (daily_summary['net_profit'] / daily_summary['total_revenue'] * 100).round(2)
    daily_summary['average_order_value'] = (daily_summary['total_revenue'] / daily_summary['unique_orders']).round(2)
    
    print(f"✅ Created daily_sales_summary: {len(daily_summary):,} records")
    return daily_summary

def create_customer_analytics(fact_orders, customers_df):
    """Create customer lifetime value and behavior analytics"""
    print("🔄 Creating customer_analytics...")
    
    # Customer transaction summary
    customer_summary = fact_orders.groupby('customer_id').agg({
        'item_total': ['sum', 'mean', 'count'],
        'net_profit': 'sum',
        'order_date': ['min', 'max'],
        'order_id': 'nunique',
        'quantity': 'sum'
    }).round(2)
    
    # Flatten column names
    customer_summary.columns = [
        'total_spent', 'avg_order_value', 'total_orders',
        'total_profit_generated', 'first_order_date', 'last_order_date',
        'unique_orders', 'total_items_bought'
    ]
    
    customer_summary = customer_summary.reset_index()
    
    # Add customer master data
    customer_dims = customers_df[['customer_id', 'city', 'age_group', 'customer_segment', 'signup_date']]
    customer_analytics = customer_summary.merge(customer_dims, on='customer_id', how='left')
    
    # Calculate additional metrics
    customer_analytics['signup_date'] = pd.to_datetime(customer_analytics['signup_date'])
    customer_analytics['first_order_date'] = pd.to_datetime(customer_analytics['first_order_date'])
    customer_analytics['last_order_date'] = pd.to_datetime(customer_analytics['last_order_date'])
    
    # Days from signup to first order
    customer_analytics['days_to_first_order'] = (
        customer_analytics['first_order_date'] - customer_analytics['signup_date']
    ).dt.days
    
    # Customer lifetime (days between first and last order)
    customer_analytics['customer_lifetime_days'] = (
        customer_analytics['last_order_date'] - customer_analytics['first_order_date']
    ).dt.days
    
    # Customer value segments
    customer_analytics['ltv_segment'] = pd.qcut(
        customer_analytics['total_spent'], 
        q=4, 
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    print(f"✅ Created customer_analytics: {len(customer_analytics):,} records")
    return customer_analytics

def create_product_performance(fact_orders, products_df):
    """Create product performance analytics"""
    print("🔄 Creating product_performance...")
    
    # Product performance metrics
    product_performance = fact_orders.groupby(['product_id', 'category', 'brand']).agg({
        'item_total': 'sum',
        'net_profit': 'sum', 
        'quantity': 'sum',
        'order_id': 'nunique',
        'customer_id': 'nunique'
    }).round(2)
    
    product_performance.columns = [
        'total_revenue', 'total_profit', 'total_quantity_sold',
        'unique_orders', 'unique_customers'
    ]
    
    product_performance = product_performance.reset_index()
    
    # Add product master data
    product_dims = products_df[['product_id', 'name', 'price', 'cost', 'stock_quantity']]
    product_performance = product_performance.merge(product_dims, on='product_id', how='left')
    
    # Calculate performance metrics
    product_performance['profit_margin'] = (product_performance['total_profit'] / product_performance['total_revenue'] * 100).round(2)
    product_performance['avg_selling_price'] = (product_performance['total_revenue'] / product_performance['total_quantity_sold']).round(2)
    product_performance['inventory_turnover'] = (product_performance['total_quantity_sold'] / product_performance['stock_quantity']).round(2)
    
    # Performance rankings
    product_performance['revenue_rank'] = product_performance['total_revenue'].rank(method='dense', ascending=False)
    product_performance['profit_rank'] = product_performance['total_profit'].rank(method='dense', ascending=False)
    
    print(f"✅ Created product_performance: {len(product_performance):,} records")
    return product_performance

def create_bigquery_dataset_and_tables(project_id, dataset_id):
    """Create BigQuery dataset if it doesn't exist"""
    client = bigquery.Client(project=project_id)
    
    try:
        dataset_ref = client.dataset(dataset_id)
        dataset = client.get_dataset(dataset_ref)
        print(f"✅ Dataset {dataset_id} already exists")
    except:
        dataset_ref = client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        print(f"✅ Created dataset {dataset_id}")

def load_to_bigquery(data_dict, project_id, dataset_id):
    """Load all transformed tables to BigQuery"""
    print("\n☁️ Loading data to BigQuery...")
    
    client = bigquery.Client(project=project_id)
    
    # Ensure dataset exists
    create_bigquery_dataset_and_tables(project_id, dataset_id)
    
    success_count = 0
    for table_name, df in data_dict.items():
        try:
            table_ref = client.dataset(dataset_id).table(table_name)
            
            # Configure job to overwrite existing data
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True
            )
            
            # Load data
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for job to complete
            
            print(f"✅ Loaded {table_name}: {len(df):,} rows → BigQuery")
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed to load {table_name}: {str(e)}")
    
    return success_count

def main():
    """Main transformation pipeline with environment variable support"""
    print("🚀 Starting E-commerce Data Pipeline - Transform Phase")
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration from environment variables
    bucket_name = os.getenv('BUCKET_NAME', 'ecommerce-pipeline-bucket-471107')
    project_id = os.getenv('PROJECT_ID', 'ecommerce-pipeline-471107')
    dataset_id = os.getenv('DATASET_ID', 'ecommerce_analytics')
    
    print(f"🔧 Configuration:")
    print(f"  Bucket: {bucket_name}")
    print(f"  Project: {project_id}")
    print(f"  Dataset: {dataset_id}")
    print(f"  Environment: {'GitHub Actions' if 'GITHUB_ACTIONS' in os.environ else 'Local Development'}")
    
    try:
        # Step 1: Download raw data
        raw_data = download_from_gcs(bucket_name)
        products_df = raw_data['products']
        customers_df = raw_data['customers'] 
        orders_df = raw_data['orders']
        
        print(f"\n📊 Raw Data Summary:")
        print(f"  Products: {len(products_df):,} records")
        print(f"  Customers: {len(customers_df):,} records")
        print(f"  Orders: {len(orders_df):,} records")
        
        # Step 2: Create analytical tables
        print(f"\n🔄 Starting data transformations...")
        
        fact_orders = create_fact_orders(orders_df, products_df, customers_df)
        daily_sales_summary = create_daily_sales_summary(fact_orders)
        customer_analytics = create_customer_analytics(fact_orders, customers_df)
        product_performance = create_product_performance(fact_orders, products_df)
        
        # Step 3: Prepare dimension tables
        dim_products = products_df.copy()
        dim_customers = customers_df.copy()
        dim_customers['signup_date'] = pd.to_datetime(dim_customers['signup_date'])
        
        # Step 4: Organize all tables
        transformed_tables = {
            'fact_orders': fact_orders,
            'daily_sales_summary': daily_sales_summary,
            'customer_analytics': customer_analytics,
            'product_performance': product_performance,
            'dim_products': dim_products,
            'dim_customers': dim_customers
        }
        
        # Step 5: Load to BigQuery
        success_count = load_to_bigquery(transformed_tables, project_id, dataset_id)
        
        # Step 6: Generate summary
        total_revenue = fact_orders['revenue_after_discount'].sum()
        total_profit = fact_orders['net_profit'].sum()
        avg_profit_margin = (total_profit / fact_orders['item_total'].sum() * 100)
        
        print(f"\n📈 Business Intelligence Summary:")
        print(f"  Total Revenue (30 days): ${total_revenue:,.2f}")
        print(f"  Total Profit (30 days): ${total_profit:,.2f}")
        print(f"  Average Profit Margin: {avg_profit_margin:.1f}%")
        print(f"  Unique Customers: {fact_orders['customer_id'].nunique():,}")
        print(f"  Unique Products Sold: {fact_orders['product_id'].nunique():,}")
        print(f"  Total Orders: {fact_orders['order_id'].nunique():,}")
        
        if success_count == len(transformed_tables):
            print(f"\n✅ Transform phase completed successfully!")
            print(f"📊 Created {len(transformed_tables)} analytical tables in BigQuery")
            print(f"🎯 Ready for Fabric dashboard creation!")
            
            # Set outputs for GitHub Actions
            if 'GITHUB_ACTIONS' in os.environ and 'GITHUB_OUTPUT' in os.environ:
                with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
                    f.write(f"tables_created={len(transformed_tables)}\n")
                    f.write(f"total_revenue={total_revenue:.2f}\n")
                    f.write(f"profit_margin={avg_profit_margin:.1f}\n")
        else:
            print(f"\n⚠️ Transform phase completed with {len(transformed_tables)-success_count} errors")
            if 'GITHUB_ACTIONS' in os.environ:
                exit(1)
            
    except Exception as e:
        print(f"\n❌ Transform phase failed: {str(e)}")
        if 'GITHUB_ACTIONS' in os.environ:
            exit(1)
        raise

if __name__ == "__main__":
    main()
