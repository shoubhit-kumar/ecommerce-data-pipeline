import os
from datetime import datetime
from google.cloud import bigquery

def generate_pipeline_report():
    """Generate comprehensive pipeline execution report"""
    project_id = os.getenv('PROJECT_ID', 'ecommerce-pipeline-471107')
    dataset_id = os.getenv('DATASET_ID', 'ecommerce_analytics')
    
    client = bigquery.Client(project=project_id)
    
    print("📊 PIPELINE EXECUTION REPORT")
    print("=" * 50)
    print(f"🕒 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"🏗️ Project: {project_id}")
    print(f"🗂️ Dataset: {dataset_id}")
    
    # Data freshness check
    freshness_query = f"""
    SELECT 
        MAX(order_date) as latest_order_date,
        COUNT(*) as total_orders
    FROM `{project_id}.{dataset_id}.fact_orders`
    """
    
    result = client.query(freshness_query).to_dataframe()
    latest_date = result['latest_order_date'].iloc[0]
    total_orders = result['total_orders'].iloc[0]
    
    print(f"\n📈 DATA SUMMARY:")
    print(f"  📅 Latest order date: {latest_date}")
    print(f"  📦 Total orders processed: {total_orders:,}")
    
    # Business metrics
    business_query = f"""
    SELECT 
        ROUND(SUM(revenue_after_discount), 2) as total_revenue,
        ROUND(SUM(net_profit), 2) as total_profit,
        ROUND(AVG(profit_margin), 1) as avg_profit_margin,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT product_id) as unique_products
    FROM `{project_id}.{dataset_id}.fact_orders`
    WHERE status = 'completed'
    """
    
    result = client.query(business_query).to_dataframe()
    
    print(f"\n💰 BUSINESS METRICS:")
    print(f"  💵 Total Revenue: ${result['total_revenue'].iloc[0]:,.2f}")
    print(f"  📊 Total Profit: ${result['total_profit'].iloc[0]:,.2f}")
    print(f"  📈 Avg Profit Margin: {result['avg_profit_margin'].iloc[0]}%")
    print(f"  👥 Unique Customers: {result['unique_customers'].iloc[0]:,}")
    print(f"  📦 Unique Products: {result['unique_products'].iloc[0]:,}")
    
    # Top performing categories
    category_query = f"""
    SELECT 
        category,
        ROUND(SUM(revenue_after_discount), 2) as revenue,
        COUNT(*) as orders
    FROM `{project_id}.{dataset_id}.fact_orders`
    WHERE status = 'completed'
    GROUP BY category
    ORDER BY revenue DESC
    LIMIT 3
    """
    
    result = client.query(category_query).to_dataframe()
    
    print(f"\n🏆 TOP CATEGORIES:")
    for _, row in result.iterrows():
        print(f"  📦 {row['category']}: ${row['revenue']:,.2f} ({row['orders']:,} orders)")
    
    # Data quality summary
    table_counts_query = f"""
    SELECT 
        'fact_orders' as table_name,
        COUNT(*) as record_count
    FROM `{project_id}.{dataset_id}.fact_orders`
    UNION ALL
    SELECT 
        'customer_analytics' as table_name,
        COUNT(*) as record_count
    FROM `{project_id}.{dataset_id}.customer_analytics`
    UNION ALL
    SELECT 
        'product_performance' as table_name,
        COUNT(*) as record_count
    FROM `{project_id}.{dataset_id}.product_performance`
    """
    
    result = client.query(table_counts_query).to_dataframe()
    
    print(f"\n📊 DATA QUALITY:")
    for _, row in result.iterrows():
        print(f"  ✅ {row['table_name']}: {row['record_count']:,} records")
    
    print(f"\n✅ PIPELINE STATUS: SUCCESS")
    print("=" * 50)

def main():
    try:
        generate_pipeline_report()
    except Exception as e:
        print(f"❌ Report generation failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
