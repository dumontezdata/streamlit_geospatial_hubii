import streamlit as st
import pandas as pd
import boto3
from pyathena import connect
from keplergl import KeplerGl
from streamlit_keplergl import keplergl_static

# Your AWS setup code here

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

# Your queries and DataFrame loading here

# Create the KeplerGl map
map_ = KeplerGl()

# Adding the 'pedidos' data to the map
map_.add_data(data=df_pedidos, name="Pedidos")

# Adding the 'hubs' data to the map
map_.add_data(data=df_hubs, name="Hubs")

# Configuring map visualization
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

# Set the Streamlit page layout to wide
st.set_page_config(page_title="Monitoramento de Preços", layout="wide")

# Create a container for the map to make it occupy full screen
container = st.container()
with container:
    # Display the KeplerGl map with dynamic size
    keplergl_static(map_, height=container.height, width=container.width, read_only=False)