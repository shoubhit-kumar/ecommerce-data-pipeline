import pandas as pd
from google.cloud import bigquery
import os
from datetime import datetime

def run_data_quality_checks(project_id, dataset_id):
    """Run comprehensive data quality checks"""
    print("🔍 Running Data Quality Checks...")
    
    client = bigquery.Client(project=project_id)
    issues = []
    
    # Check 1: Record counts
    tables = ['fact_orders', 'daily_sales_summary', 'customer_analytics', 'product_performance']
    
    for table in tables:
        query = f"SELECT COUNT(*) as record_count FROM `{project_id}.{dataset_id}.{table}`"
        result = client.query(query).to_dataframe()
        count = result['record_count'].iloc[0]
        
        if count == 0:
            issues.append(f"❌ {table} is empty")
        else:
            print(f"✅ {table}: {count:,} records")
    
    # Check 2: Revenue consistency
    revenue_check = f"""
    SELECT 
        SUM(item_total) as total_from_fact,
        (SELECT SUM(total_revenue) FROM `{project_id}.{dataset_id}.daily_sales_summary`) as total_from_summary
    FROM `{project_id}.{dataset_id}.fact_orders`
    """
    
    result = client.query(revenue_check).to_dataframe()
    fact_total = result['total_from_fact'].iloc[0]
    summary_total = result['total_from_summary'].iloc[0]
    
    if abs(fact_total - summary_total) > 1:  # Allow small rounding differences
        issues.append(f"❌ Revenue mismatch: Fact({fact_total:,.2f}) vs Summary({summary_total:,.2f})")
    else:
        print(f"✅ Revenue consistency check passed: ${fact_total:,.2f}")
    
    # Check 3: Data freshness
    freshness_check = f"""
    SELECT MAX(order_date) as latest_order
    FROM `{project_id}.{dataset_id}.fact_orders`
    """
    
    result = client.query(freshness_check).to_dataframe()
    latest_order = pd.to_datetime(result['latest_order'].iloc[0]).date()
    days_old = (datetime.now().date() - latest_order).days
    
    if days_old > 1:
        issues.append(f"⚠️ Data is {days_old} days old (latest: {latest_order})")
    else:
        print(f"✅ Data freshness check passed: Latest order {latest_order}")
    
    # Summary
    if not issues:
        print("\n✅ All data quality checks passed!")
        return True
    else:
        print(f"\n⚠️ Found {len(issues)} data quality issues:")
        for issue in issues:
            print(f"  {issue}")
        return False

def main():
    project_id = 'ecommerce-pipeline-471107'  # Replace with your project ID
    dataset_id = 'ecommerce_analytics'
    
    try:
        run_data_quality_checks(project_id, dataset_id)
    except Exception as e:
        print(f"❌ Data quality checks failed: {str(e)}")

if __name__ == "__main__":
    main()
