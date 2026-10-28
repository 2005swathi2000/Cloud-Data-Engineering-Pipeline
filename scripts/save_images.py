import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import duckdb

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(base_dir, 'warehouse', 'retail_analytics.duckdb')
img_dir = os.path.join(base_dir, 'images')
os.makedirs(img_dir, exist_ok=True)

conn = duckdb.connect(db_path)

# 1. Daily Revenue Trend
daily_df = conn.execute("""
    SELECT transaction_date, SUM(total_amount) AS revenue, COUNT(*) AS orders
    FROM retail_transactions
    GROUP BY transaction_date
    ORDER BY transaction_date ASC;
""").fetchdf()

plt.figure(figsize=(12, 5))
plt.plot(daily_df['transaction_date'], daily_df['revenue'], marker='o', color='#1a73e8', lw=2.2, label='Daily Billed Revenue ($)')
plt.title('Daily Retail Revenue Velocity (DuckDB OLAP)', fontsize=13, weight='bold', pad=12)
plt.xlabel('Calendar Date', fontsize=11)
plt.ylabel('Daily Revenue in USD ($)', fontsize=11)
plt.xticks(rotation=45, ha='right', fontsize=9)
plt.grid(True, linestyle=':', alpha=0.6)
plt.legend(loc='upper right')
plt.tight_layout()
plt.savefig(os.path.join(img_dir, 'daily_revenue_trend.png'), dpi=200)
plt.close()

# 2. Category Revenue Distribution
cat_df = conn.execute("""
    SELECT product_category, SUM(total_amount) AS revenue
    FROM retail_transactions
    GROUP BY product_category
    ORDER BY revenue DESC;
""").fetchdf()

plt.figure(figsize=(9, 5))
sns.barplot(x='revenue', y='product_category', data=cat_df, palette='Blues_r', hue='product_category', legend=False)
plt.title('Billed Revenue by Product Category (USD)', fontsize=13, weight='bold', pad=12)
plt.xlabel('Revenue in USD ($)', fontsize=11)
plt.ylabel('Product Category', fontsize=11)
plt.grid(axis='x', linestyle=':', alpha=0.6)
plt.tight_layout()
plt.savefig(os.path.join(img_dir, 'category_revenue_distribution.png'), dpi=200)
plt.close()

# 3. Regional Performance
reg_df = conn.execute("""
    SELECT store_region, SUM(total_amount) AS revenue, COUNT(*) AS orders
    FROM retail_transactions
    GROUP BY store_region
    ORDER BY revenue DESC;
""").fetchdf()

fig, ax1 = plt.subplots(figsize=(10, 5))
ax2 = ax1.twinx()
sns.barplot(x='store_region', y='revenue', data=reg_df, ax=ax1, palette='Blues_d', hue='store_region', legend=False)
ax2.plot(reg_df['store_region'], reg_df['orders'], color='#ea4335', marker='s', lw=2.5, label='Order Count')
ax1.set_title('Regional Revenue ($) and Transaction Volume', fontsize=13, weight='bold', pad=12)
ax1.set_xlabel('Market Region', fontsize=11)
ax1.set_ylabel('Total Revenue ($)', fontsize=11, color='#1a73e8')
ax2.set_ylabel('Transaction Count', fontsize=11, color='#ea4335')
ax1.grid(axis='y', linestyle=':', alpha=0.6)
plt.tight_layout()
plt.savefig(os.path.join(img_dir, 'regional_performance.png'), dpi=200)
plt.close()

# 4. Quarantine Reason Distribution
rej_df = conn.execute("""
    SELECT rejection_reason, COUNT(*) AS count
    FROM rejected_transactions
    GROUP BY rejection_reason
    ORDER BY count DESC;
""").fetchdf()

plt.figure(figsize=(11, 5))
sns.barplot(x='count', y='rejection_reason', data=rej_df, palette='Reds_r', hue='rejection_reason', legend=False)
plt.title('Quarantine Routing: Breakdown of Rejected Rows by Error Code', fontsize=13, weight='bold', pad=12)
plt.xlabel('Quarantined Row Count', fontsize=11)
plt.ylabel('Rejection Reason Code', fontsize=11)
plt.grid(axis='x', linestyle=':', alpha=0.6)
plt.tight_layout()
plt.savefig(os.path.join(img_dir, 'data_quality_governance.png'), dpi=200)
plt.close()

conn.close()
print('All 4 Data Engineering output images generated successfully in:', img_dir)

