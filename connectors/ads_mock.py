import pandas as pd
from datetime import datetime, timedelta

def generate_mock_spend():
    data = [
        {'campaign_id':'fb_ad_c1','ad_spend':120.0,'platform':'Facebook'},
        {'campaign_id':'google_ad_a1','ad_spend':50.0,'platform':'Google'},
        {'campaign_id':'google_ad_b2','ad_spend':40.0,'platform':'Google'},
        {'campaign_id':'tiktok_ad_x3','ad_spend':30.0,'platform':'TikTok'}
    ]
    return pd.DataFrame(data)
