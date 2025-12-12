import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_mock_orders(days=120, n_per_day=5):
    """Generate a realistic set of mock orders across the last `days` days."""
    today = datetime.utcnow().date()
    rows = []
    customer_ids = [f'C{str(i).zfill(3)}' for i in range(1, 51)]
    campaign_choices = ['fb_ad_c1', 'google_ad_a1', 'google_ad_b2', 'tiktok_ad_x3', 'organic_search']

    order_id = 1000
    for d in range(days):
        order_date = today - timedelta(days=d)
        for i in range(np.random.poisson(n_per_day)):
            order_id += 1
            cust = np.random.choice(customer_ids)
            revenue = float(np.random.choice([30,45,60,80,100,120,150,200,250]))
            cogs = round(revenue * np.random.uniform(0.2,0.45),2)
            source = np.random.choice(campaign_choices, p=[0.3,0.2,0.15,0.05,0.3])
            is_new = np.random.rand() < 0.25
            rows.append({
                'order_id': order_id,
                'customer_id': cust,
                'is_new_customer': is_new,
                'revenue': revenue,
                'cogs': cogs,
                'source_utm': source,
                'order_date': order_date.isoformat()
            })
    df = pd.DataFrame(rows)
    if df.empty:
        # create minimal sample
        df = pd.DataFrame([{
            'order_id':1001,'customer_id':'C001','is_new_customer':True,'revenue':150.0,'cogs':40.0,'source_utm':'fb_ad_c1','order_date':today.isoformat()
        }])
    return df
