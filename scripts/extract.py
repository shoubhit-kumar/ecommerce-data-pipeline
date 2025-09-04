import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import storage
import os
import json

# Set up authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-key.json'  # Local testing

def generate_ecommerce_data(days=30):
    """Generate realistic e-commerce data for the last 30 days"""
    np.random.seed(42)  # For reproducible data
    
    print(f"Generating {days} days of e-commerce data...")
    
    # Products master data
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports & Fitness']
    brands = ['TechCorp', 'StyleMax', 'BookWorld', 'HomeLife', 'FitZone', 'Generic']
    
    products = []
    for i in range(1000):
        category = np.random.choice(categories)
        base_price = np.random.uniform(10, 500)
        products.append({
            'product_id': f'PROD{i:06d}',
            'name': f'{np.random.choice(brands)} {category} Item {i%100}',
            'category': category,
            'brand': np.random.choice(brands),
            'price': round(base_price, 2),
            'cost': round(base_price * np.random.uniform(0.3, 0.7), 2),  # 30-70% cost ratio
            'stock_quantity': np.random.randint(0, 1000),
            'created_date': (datetime.now() - timedelta(days=np.random.randint(30, 365))).date()
        })
    
    # Customer master data
    cities = ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata', 'Hyderabad', 'Pune', 'Ahmedabad']
    customers = []
    for i in range(5000):
        signup_days_ago = np.random.randint(1, 365)
        customers.append({
            'customer_id': f'CUST{i:06d}',
            'name': f'Customer {i}',
            'email': f'customer{i}@email.com',
            'city': np.random.choice(cities),
            'age_group': np.random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
            'signup_date': (datetime.now() - timedelta(days=signup_days_ago)).date(),
            'customer_segment': np.random.choice(['Premium', 'Regular', 'Budget'], p=[0.2, 0.6, 0.2])
        })
    
    # Generate orders for the specified date range
    orders = []
    base_date = datetime.now() - timedelta(days=days)
    
    print("Generating daily orders...")
    for day in range(days):
        current_date = base_date + timedelta(days=day)
        
        # Weekend effect - more orders on weekends
        weekend_multiplier = 1.3 if current_date.weekday() >= 5 else 1.0
        daily_orders = int(np.random.poisson(400) * weekend_multiplier)
        
        for order_num in range(daily_orders):
            customer = np.random.choice(customers)
            
            # Order can have 1-4 items
            items_in_order = np.random.choice([1, 2, 3, 4], p=[0.6, 0.25, 0.1, 0.05])
            
            order_id = f'ORD{len(orders):08d}'
            order_total = 0
            
            for item in range(items_in_order):
                product = np.random.choice(products)
                quantity = np.random.choice([1, 2, 3], p=[0.8, 0.15, 0.05])
                unit_price = product['price']
                item_total = unit_price * quantity
                order_total += item_total
                
                orders.append({
                    'order_id': order_id,
                    'customer_id': customer['customer_id'],
                    'product_id': product['product_id'],
                    'order_date': current_date.date(),
                    'quantity': quantity,
                    'unit_price': unit_price,
                    'item_total': round(item_total, 2),
                    'status': np.random.choice(['completed', 'pending', 'cancelled'], 
                                            p=[0.85, 0.1, 0.05]),
                    'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'UPI', 'COD'], 
                                                     p=[0.4, 0.3, 0.25, 0.05]),
                    'discount_amount': round(np.random.uniform(0, item_total * 0.2), 2)
                })
    
    print(f"Data generation complete!")
    return pd.DataFrame(products), pd.DataFrame(customers), pd.DataFrame(orders)

def upload_to_gcs(df, bucket_name, file_name):
    """Upload DataFrame to Google Cloud Storage"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(f'raw_data/{file_name}')
        
        # Upload as CSV
        csv_data = df.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
        
        print(f'✅ Uploaded {file_name} ({len(df):,} records) to GCS')
        return True
    except Exception as e:
        print(f'❌ Failed to upload {file_name}: {str(e)}')
        return False

def main():
    """Main execution function"""
    print("🚀 Starting E-commerce Data Pipeline - Extract Phase")
    print(f"📅 Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration
    bucket_name = 'ecommerce-pipeline-bucket-471107'

    try:
        # Generate data
        products_df, customers_df, orders_df = generate_ecommerce_data(30)
        
        # Display summary
        print("\n📊 Data Summary:")
        print(f"  Products: {len(products_df):,} records")
        print(f"  Customers: {len(customers_df):,} records") 
        print(f"  Orders: {len(orders_df):,} records")
        print(f"  Date range: {orders_df['order_date'].min()} to {orders_df['order_date'].max()}")
        print(f"  Total revenue: ${orders_df['item_total'].sum():,.2f}")
        
        # Upload to GCS
        print("\n☁️ Uploading to Google Cloud Storage...")
        success_count = 0
        success_count += upload_to_gcs(products_df, bucket_name, 'products.csv')
        success_count += upload_to_gcs(customers_df, bucket_name, 'customers.csv')
        success_count += upload_to_gcs(orders_df, bucket_name, 'orders.csv')
        
        if success_count == 3:
            print("\n✅ Extract phase completed successfully!")
            print("Ready for transformation phase.")
        else:
            print(f"\n⚠️ Extract phase completed with {3-success_count} errors.")
            
    except Exception as e:
        print(f"\n❌ Extract phase failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
