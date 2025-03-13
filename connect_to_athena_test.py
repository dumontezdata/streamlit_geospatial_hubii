import streamlit as st
import pandas as pd
import boto3
from pyathena import connect
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static
import numpy as np

AWS_ACCESS_KEY = st.secrets["AWS_ACCESS_KEY"]
AWS_SECRET_KEY = st.secrets["AWS_SECRET_KEY"]
AWS_REGION = st.secrets["AWS_REGION"]
S3_STAGING_DIR = st.secrets["S3_STAGING_DIR"]

athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

conn = connect(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    s3_staging_dir=S3_STAGING_DIR,
    region_name=AWS_REGION
)

query1 = """SELECT 
    order_id,
    ordered_at_brt,
    time_first_pending_brt,
    fact_order.hub_id,
    dim_hubs.best_hub_name,
    dim_hubs."praça_hub",
    channel_id,
    channels.name AS channel_name,
    company_id,
    companies.name AS company_name,
    delivery_status,
    max_wave,
    straight_line_distance_order_hub_km,
    unique_order_shipping_cost AS custo_frete,
    order_tip_fee_prorate AS gorjeta_pedido,
    order_latitude,
    order_longitude,
    subtotal_sell_out AS gmv
FROM gold.fact_order
JOIN gold.dim_hubs ON dim_hubs.hub_id = fact_order.hub_id
JOIN bronze.companies ON companies.id = fact_order.company_id
JOIN bronze.channels ON channels.id = fact_order.channel_id
WHERE date_order_created_at_brt >= '2025-03-01'
AND unique_order_shipping_cost < 0
"""

df_pedidos = pd.read_sql(query1, conn)

query2 = """with df as(SELECT 
    order_hub_sent.hub_id,
    dim_hubs.best_hub_name,
    companies.name,
    'home' as "icon",
    dim_hubs.hub_latitude,
    dim_hubs.hub_longitude,
    date_format(hub_pending_at_brt,'%Y-%m') as "year_month",
    SUM(CASE WHEN final_action_treated IN ('accepted', 'refused', 'ignored') THEN 1 ELSE 0 END) AS "pedidos",
    SUM(CASE WHEN final_action_treated = 'accepted' THEN 1 ELSE 0 END) AS "aceitos",
    SUM(CASE WHEN final_action_treated IN ('accepted', 'refused') THEN 1 ELSE 0 END) AS "respondidos",
    SUM(CASE WHEN final_action_treated IN ('accepted', 'refused') THEN 1 ELSE 0 END) 
        / CAST(NULLIF(SUM(CASE WHEN final_action_treated IN ('accepted', 'refused', 'ignored') THEN 1 ELSE 0 END), 0) AS DOUBLE) AS "taxa_resposta",
    SUM(CASE WHEN final_action_treated = 'accepted' THEN 1 ELSE 0 END) 
        / CAST(NULLIF(SUM(CASE WHEN final_action_treated IN ('accepted', 'refused') THEN 1 ELSE 0 END), 0) AS DOUBLE) AS "taxa_aceite",
    AVG(CASE WHEN final_action_treated = 'accepted' THEN straight_line_distance_order_hub_km ELSE NULL END) AS "km_medio_aceite",
    AVG(CASE WHEN final_action_treated = 'accepted' AND wave = 1 THEN straight_line_distance_order_hub_km ELSE NULL END) AS "km_medio_aceite_wave_1"
FROM silver.order_hub_sent
join gold.dim_hubs on dim_hubs.hub_id=order_hub_sent.hub_id
JOIN bronze.companies ON companies.id = order_hub_sent.company_id_order
WHERE date_order_created_at_brt >= '2025-03-01'
GROUP BY 1,2,3,4,5,6,7
)

select *,taxa_resposta * taxa_aceite as score_hub from df where aceitos>0
"""
df_hubs = pd.read_sql(query2, conn)

df_hubs_clean = df_hubs.dropna(subset=['hub_latitude', 'hub_longitude']).replace([np.inf, -np.inf], np.nan).dropna()
df_pedidos_clean = df_pedidos.dropna(subset=['order_latitude', 'order_longitude']).replace([np.inf, -np.inf], np.nan).dropna()
df_pedidos_clean['ordered_at_brt']= df_pedidos_clean['ordered_at_brt'].astype(str)
df_pedidos_clean['time_first_pending_brt'] = df_pedidos_clean['time_first_pending_brt'].astype(str)

# Create the KeplerGl map
map_ = KeplerGl(height=600)

# Adding the 'pedidos' data to the map
map_.add_data(data=df_pedidos_clean, name="Pedidos")

# Adding the 'hubs' data to the map
map_.add_data(data=df_hubs_clean, name="Hubs")
# Configurando a camada de pedidos para colorir por GMV
map_.config = {
    "visState": {
        "layers": [
            {
                "id": "Pedidos",
                "type": "point",
                "config": {
                    "colorField": {"name": "gmv", "type": "real"},
                    "colorScale": "quantile",
                    "colorRange": {
                        "name": "Uber Viz Diverging",
                        "type": "diverging",
                        "category": "Uber",
                        "colors": ["#f3f0c2", "#ff4d3b", "#ff1a2e"]
                    },
                    "sizeField": {"name": "gmv", "type": "real"},
                    "sizeScale": "linear",
                    "sizeRange": [5, 20],
                    "visible": True
                }
            },
            {
                "id": "Hubs",
                "type": "point",
                "config": {
                    "colorField": {"name": "score_hub", "type": "real"},
                    "colorScale": "quantile",
                    "colorRange": {
                        "name": "Uber Viz Diverging",
                        "type": "diverging",
                        "category": "Uber",
                        "colors": ["#00d6b4", "#00468c", "#ff1a2e"]
                    },
                    "sizeField": {"name": "score_hub", "type": "real"},
                    "sizeScale": "linear",
                    "sizeRange": [5, 20],
                    "visible": True
                }
            }
        ]
    }
}

# Set Streamlit page layout to wide (this already works, keep it at the top of your script)
st.set_page_config(page_title="Monitoramento de Preços", layout="wide")

# Adjusting the layout with the container
with st.container():
    keplergl_static(map_,read_only=False)
