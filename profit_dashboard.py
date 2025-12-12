import pandas as pd
import numpy as np

# --- 1. SIMULATE SHOPIFY ORDER DATA (Updated with Customer ID) ---
# This data now includes customer_id and a flag indicating if this was their first purchase (New).
shopify_orders = {
    'order_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
    'customer_id': ['C001', 'C002', 'C001', 'C003', 'C004', 'C005', 'C002', 'C006', 'C005', 'C003'],
    'is_new_customer': [True, True, False, True, True, True, False, True, False, False], # False means Repeat
    'revenue': [150.00, 45.00, 100.00, 120.00, 55.00, 250.00, 60.00, 30.00, 80.00, 70.00],
    'cogs': [40.00, 15.00, 30.00, 35.00, 20.00, 60.00, 20.00, 10.00, 25.00, 20.00],
    'order_date': ['2025-11-01', '2025-11-01', '2025-11-02', '2025-11-02', '2025-11-03', '2025-11-03', '2025-11-04', '2025-11-04', '2025-11-05', '2025-11-05'],
    'source_utm': ['fb_ad_c1', 'google_ad_b2', 'fb_ad_c1', 'organic_search', 'tiktok_ad_x3', 'google_ad_a1', 'fb_ad_c1', 'organic_search', 'google_ad_a1', 'google_ad_b2']
}

# --- 2. SIMULATE PAID MEDIA SPEND DATA (Updated for new campaign) ---
ad_spend = {
    'campaign_id': ['fb_ad_c1', 'google_ad_b2', 'tiktok_ad_x3', 'google_ad_a1'],
    'ad_spend': [120.00, 40.00, 30.00, 50.00],
    'platform': ['Facebook', 'Google', 'TikTok', 'Google']
}

# Convert the dictionaries into pandas DataFrames
df_orders = pd.DataFrame(shopify_orders)
df_spend = pd.DataFrame(ad_spend)

print("--- 1. SHOPIFY ORDERS DATA (df_orders) ---")
print(df_orders.head()) # Use .head() to show the first few rows

# ... (Keep the rest of your Phase 3 code in the file)

print("\n--- 2. AD SPEND DATA (df_spend) ---")
print(df_spend)

print("\n--- Phase 1 Complete. Ready for Unification in the next step. ---")

# --- PHASE 3: CORE UNIFICATION AND PROFIT CALCULATION ---

print("\n\n#####################################################")
print("### PHASE 3: UNIFICATION AND PROFIT CALCULATION ###")
print("#####################################################")


# 1. MERGE DATA: Link Orders (Revenue/Profit) to Ad Spend (Cost)
# We use a 'left' merge to keep all orders, even those without a corresponding ad spend (like 'organic_search').
# The link is the order's 'source_utm' and the campaign's 'campaign_id'.
df_merged = pd.merge(
    df_orders, 
    df_spend[['campaign_id', 'ad_spend', 'platform']], 
    left_on='source_utm', 
    right_on='campaign_id', 
    how='left'
)

# Fill NaN (Not a Number) values created by the merge for non-paid traffic.
# If an order has no matching ad_spend, we assume the spend for that order is 0.
df_merged['ad_spend'] = df_merged['ad_spend'].fillna(0)
df_merged['platform'] = df_merged['platform'].fillna('Organic/Direct')


# 2. CALCULATE PROFIT PER ORDER
# We calculate Net Contribution Profit (Revenue - COGS - Ad Spend) per order.
# NOTE: In a real scenario, Ad Spend here would be an ESTIMATE per order.
# For simplicity, we calculate Gross Profit (Revenue - COGS) and apply Ad Spend later for True ROAS.
df_merged['gross_profit'] = df_merged['revenue'] - df_merged['cogs']

# Let's see the combined, clean data before aggregation
print("\n--- 3. MERGED DATA (Order-level linking of Revenue, Profit, and Ad Spend) ---")
print(df_merged[['order_id', 'platform', 'revenue', 'gross_profit', 'ad_spend']].head(10))


# 3. AGGREGATE by CHANNEL (The True ROAS View)
# We group the data by the 'platform' (Facebook, Google, etc.) to get channel totals.
df_agg = df_merged.groupby('platform').agg(
    total_revenue=('revenue', 'sum'),
    total_gross_profit=('gross_profit', 'sum'),
    total_ad_spend=('ad_spend', 'max'),  # We use max since the total spend is repeated in the merged data
    total_orders=('order_id', 'count')
).reset_index()


# 4. CALCULATE TRUE ROAS AND NET CONTRIBUTION PROFIT (The Core Value)
# TRUE ROAS = Total Revenue / Total Ad Spend
df_agg['true_roas'] = df_agg['total_revenue'] / df_agg['total_ad_spend']

# NET CONTRIBUTION PROFIT = Total Gross Profit - Total Ad Spend
df_agg['net_contribution_profit'] = df_agg['total_gross_profit'] - df_agg['total_ad_spend']

# Fill any Infinite ROAS (where total_ad_spend was 0, like Organic) with 0 or a placeholder.
df_agg['true_roas'] = df_agg['true_roas'].replace([np.inf, -np.inf], 0)


# 5. PRESENT THE FINAL PROFIT INTELLIGENCE REPORT
print("\n--- 4. FINAL PROFIT INTELLIGENCE REPORT (Aggregated by Channel) ---")
print(df_agg.round(2))
print("\n#####################################################")
print("### PHASE 3 COMPLETE: True Profit is Calculated! ###")
print("#####################################################")

# --- PHASE 4: SIMPLE LTV SEGMENTATION ---

print("\n\n######################################################")
print("### PHASE 4: LTV & SEGMENTATION STRATEGY           ###")
print("######################################################")


# 1. IDENTIFY NEW VS. REPEAT CUSTOMERS
df_ltv = df_orders.copy()
df_ltv['gross_profit'] = df_ltv['revenue'] - df_ltv['cogs']

# 2. AGGREGATE METRICS BY NEW/REPEAT SEGMENT
# Group by the 'is_new_customer' flag
df_segment = df_ltv.groupby('is_new_customer').agg(
    total_revenue=('revenue', 'sum'),
    total_profit=('gross_profit', 'sum'),
    total_orders=('order_id', 'count'),
    unique_customers=('customer_id', 'nunique')
).reset_index()


# 3. CALCULATE CORE LTV METRICS
# LTV = Total Revenue / Total Unique Customers
df_segment['avg_order_value'] = df_segment['total_revenue'] / df_segment['total_orders']
df_segment['revenue_ltv'] = df_segment['total_revenue'] / df_segment['unique_customers']
df_segment['profit_ltv'] = df_segment['total_profit'] / df_segment['unique_customers']


# 4. CLEAN AND PRESENT THE FINAL LTV REPORT
df_segment['is_new_customer'] = df_segment['is_new_customer'].replace({True: 'New Customer', False: 'Repeat Customer'})
df_segment = df_segment.rename(columns={'is_new_customer': 'Customer Segment'})
df_segment = df_segment[['Customer Segment', 'unique_customers', 'total_orders', 'avg_order_value', 'revenue_ltv', 'profit_ltv']]


print("\n--- 5. CUSTOMER SEGMENTATION AND PROFIT LTV REPORT ---")
print(df_segment.round(2))

print("\n######################################################")
print("### PHASE 4 COMPLETE: LTV Segments Established!    ###")
print("######################################################")

# --- PHASE 5: LTV-CAC RATIO PER ACQUISITION CHANNEL ---

print("\n\n######################################################")
print("### PHASE 5: LTV:CAC RATIO (Strategic Budget View) ###")
print("######################################################")

# 1. FILTER FOR ACQUISITION DATA ONLY
# We need to look only at the orders where a customer was NEW
df_acquisition = df_orders[df_orders['is_new_customer'] == True].copy()

# 2. CALCULATE ACQUISITION PROFIT PER CHANNEL
# Merge acquisition orders with the ad spend data
df_acq_merged = pd.merge(
    df_acquisition, 
    df_spend[['campaign_id', 'ad_spend', 'platform']], 
    left_on='source_utm', 
    right_on='campaign_id', 
    how='left'
)
df_acq_merged['platform'] = df_acq_merged['platform'].fillna('Organic/Direct')
df_acq_merged['gross_profit'] = df_acq_merged['revenue'] - df_acq_merged['cogs']

# 3. AGGREGATE THE ACQUISITION METRICS
df_acq_agg = df_acq_merged.groupby('platform').agg(
    new_customers=('customer_id', 'nunique'),
    total_ad_spend=('ad_spend', 'max'), # Total Ad Spend for the campaign
    acq_profit_sum=('gross_profit', 'sum') # Total Gross Profit from first orders
).reset_index()

# 4. CALCULATE CORE ACQUISITION METRICS (CAC and Initial LTV)
# CAC (Customer Acquisition Cost) = Total Ad Spend / New Customers
df_acq_agg['customer_acquisition_cost'] = df_acq_agg['total_ad_spend'] / df_acq_agg['new_customers']

# Initial LTV (based on first order profit) = Total Acq Profit / New Customers
df_acq_agg['initial_profit_ltv'] = df_acq_agg['acq_profit_sum'] / df_acq_agg['new_customers']

# LTV:CAC Ratio = Initial Profit LTV / CAC
df_acq_agg['ltv_cac_ratio'] = df_acq_agg['initial_profit_ltv'] / df_acq_agg['customer_acquisition_cost']

# Clean up results for presentation
df_acq_agg['customer_acquisition_cost'] = df_acq_agg['customer_acquisition_cost'].fillna(0)
df_acq_agg['ltv_cac_ratio'] = df_acq_agg['ltv_cac_ratio'].replace([np.inf, -np.inf], 0)

# 5. PRESENT THE FINAL STRATEGIC REPORT
df_final_strategic = df_acq_agg[['platform', 'new_customers', 'customer_acquisition_cost', 'initial_profit_ltv', 'ltv_cac_ratio']]
df_final_strategic = df_final_strategic.rename(columns={'initial_profit_ltv': '1st_Order_Profit_LTV'})

print("\n--- 6. LTV:CAC STRATEGIC BUDGET REPORT (New Customers Only) ---")
print(df_final_strategic.round(2))

print("\n######################################################")
print("### ALL PHASES COMPLETE: Python Prototype Finished ###")
print("######################################################")

try:
    import plotly.graph_objects as go
    # --- PHASE 6: DASHBOARD VISUALIZATION (Plotly) ---

    print("\n\n#####################################################")
    print("### PHASE 6: CREATING LTV:CAC VISUALIZATION     ###")
    print("#####################################################")

    df_viz = df_final_strategic[df_final_strategic['platform'] != 'Organic/Direct'].copy()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_viz['platform'],
        y=df_viz['ltv_cac_ratio'],
        name='LTV:CAC Ratio',
        marker_color=['#2ca02c' if v >= 3 else '#ff7f0e' if v >= 1 else '#d62728' for v in df_viz['ltv_cac_ratio']]
    ))

    fig.add_trace(go.Scatter(
        x=df_viz['platform'],
        y=df_viz['customer_acquisition_cost'],
        name='CAC',
        yaxis='y2',
        mode='markers+lines',
        marker=dict(color='black')
    ))

    fig.update_layout(
        title='Strategic Budget View: LTV to CAC Ratio by Paid Channel',
        xaxis_title='Acquisition Channel',
        yaxis_title='LTV:CAC Ratio',
        yaxis2=dict(title='CAC ($)', overlaying='y', side='right')
    )

    fig.show()
    print("\n#####################################################")
    print("### VISUALIZATION COMPLETE: Plot rendered with Plotly ###")
    print("#####################################################")
except Exception:
    # Fallback to matplotlib if Plotly isn't available
    import matplotlib.pyplot as plt

    print("\n\n#####################################################")
    print("### PHASE 6 (fallback): CREATING LTV:CAC VISUALIZATION     ###")
    print("#####################################################")

    df_viz = df_final_strategic[df_final_strategic['platform'] != 'Organic/Direct'].copy()

    platforms = df_viz['platform']
    ltv_cac_ratios = df_viz['ltv_cac_ratio']
    acq_costs = df_viz['customer_acquisition_cost']
    bar_colors = ['#1877F2', '#4285F4', '#000000']

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(platforms, ltv_cac_ratios, color=bar_colors)
    ax.axhline(3.0, color='r', linestyle='--', linewidth=1.5, label='LTV:CAC Target (3.0)')
    ax.set_title('Strategic Budget View: LTV to CAC Ratio by Paid Channel', fontsize=16, pad=20)
    ax.set_ylabel('LTV:CAC Ratio (Profit / Cost)', fontsize=12)
    ax.set_xlabel('Acquisition Channel', fontsize=12)
    ax.legend()
    ax.grid(axis='y', linestyle=':', alpha=0.7)

    for bar, ratio, cac in zip(bars, ltv_cac_ratios, acq_costs):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2., height + 0.1, f'{ratio:.2f}', ha='center', va='bottom', fontsize=11, fontweight='bold')
        ax.text(bar.get_x() + bar.get_width() / 2., 0.1, f'CAC: ${cac:.2f}', ha='center', va='bottom', fontsize=10, color='dimgray')

    plt.tight_layout()
    plt.show()

    print("\n#####################################################")
    print("### VISUALIZATION COMPLETE (fallback): Plot window is open!  ###")
    print("#####################################################")