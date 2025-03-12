import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import boto3
from pyathena import connect

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_STAGING_DIR = os.getenv("S3_STAGING_DIR")


athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)

conn = connect(
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    s3_staging_dir=S3_STAGING_DIR,
    region_name=AWS_REGION,
)

query = "select * from gold.fact_order_item limit 5"
df = pd.read_sql(query, conn)




st.set_page_config(page_title="Monitoramento de Preços", layout="wide")
st.title("📊 Monitoramento de Preço - Hubii")

# Criando as abas no Streamlit
tab1, tab2 = st.tabs(["📌 Arquitetura de Preço Referência", "📊 Beleza em Casa iFood"])

# Tab 1: Arquitetura de Preço Referência**
with tab1:
    st.subheader("📌 Arquitetura de Preço - Referência da Indústria")
    
    col1, col2 = st.columns([0.25, 0.75])
    
    with col1:
        opcao_busca = st.radio("🔍 Buscar por:", ["EAN", "Marca", "Nome do Produto"], horizontal=True)
        desconto_sugerido = st.number_input("🔻 Desconto Sugerido (%)", min_value=0.0, max_value=100.0, value=10.0, step=0.5)

# tab2: Beleza em Casa iFood**
with tab2:
    st.subheader("📊 Beleza em Casa iFood - Monitoramento de Preços")
    st.write("### 📄 Comparação de Preços")
    st.dataframe(df)
    st.write("O Price Index calculado na tabela acima é o comparativo do valor praticano no ifood com a tabela de referência fornecida pelo Boticário.")