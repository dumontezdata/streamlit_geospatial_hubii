import streamlit as st
import pandas as pd
import boto3
from pyathena import connect
import folium
from folium.plugins import MarkerCluster

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
WHERE date_order_created_at_brt >= '2025-01-01'
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
WHERE date_order_created_at_brt >= '2025-01-01'
GROUP BY 1,2,3,4,5,6,7
)

select *,taxa_resposta * taxa_aceite as score_hub from df where aceitos>0
"""
df_hubs = pd.read_sql(query2, conn)

# Configuração inicial do Streamlit
st.title("Dashboard Geoespacial - Pedidos e Hubs")
st.markdown("Visualização de dados geoespaciais relacionados a pedidos e hubs de distribuição.")

# Mapa inicial
st.subheader("Mapa Interativo")
m = folium.Map(location=[-23.550520, -46.633308], zoom_start=12)  # Posição inicial (São Paulo, Brasil)

# Filtrar hubs que não possuem valores NaN para latitude e longitude
df_hubs_clean = df_hubs.dropna(subset=['hub_latitude', 'hub_longitude'])

# Adicionar marcadores para os hubs
hub_cluster = MarkerCluster().add_to(m)

for _, row in df_hubs_clean.iterrows():
    folium.Marker(
        location=[row['hub_latitude'], row['hub_longitude']],
        popup=f"Hub: {row['best_hub_name']} - Score: {row['score_hub']}",
        icon=folium.Icon(color="blue")
    ).add_to(hub_cluster)

# Exibir o mapa no Streamlit
st.components.v1.html(m._repr_html_(), height=500)

# Filtro de pedidos por hub
st.subheader("Pedidos por Hub")
selected_hub = st.selectbox("Escolha um Hub", df_hubs['best_hub_name'].unique())

# Filtrar os pedidos para o hub selecionado
df_filtered = df_pedidos[df_pedidos['best_hub_name'] == selected_hub]

# Filtrar pedidos que não possuem valores NaN para latitude e longitude
df_filtered_clean = df_filtered.dropna(subset=['order_latitude', 'order_longitude'])

# Exibir informações sobre os pedidos filtrados
st.write(f"Total de Pedidos para o Hub {selected_hub}: {df_filtered_clean.shape[0]}")
st.dataframe(df_filtered_clean[['order_id', 'ordered_at_brt', 'gmv', 'delivery_status']])

# Gráfico de distribuição de GMV
st.subheader(f"Distribuição de GMV - {selected_hub}")
st.bar_chart(df_filtered_clean.groupby('delivery_status')['gmv'].sum())

# Adicionar marcadores para pedidos no mapa
pedido_cluster = MarkerCluster().add_to(m)

for _, row in df_filtered_clean.iterrows():
    folium.Marker(
        location=[row['order_latitude'], row['order_longitude']],
        popup=f"Pedido ID: {row['order_id']} - GMV: {row['gmv']}",
        icon=folium.Icon(color="red")
    ).add_to(pedido_cluster)

# Atualizar mapa com pedidos
st.components.v1.html(m._repr_html_(), height=500)
