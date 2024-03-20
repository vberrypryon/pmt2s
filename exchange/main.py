from exchange import PryonExchange
from dotenv import load_dotenv
import os
import json

load_dotenv()

exchange = PryonExchange(
    client_id = os.getenv("CLIENT_ID"),
    client_secret = os.getenv("CLIENT_SECRET"),
    collection_id = os.getenv("COLLECTION_ID"),
    gcp_credentials = 'gcp_cred.json',
    # language = 'es',
    # translate = True,
)

exchange.get_data(
    query="What is the hair regulation?", 
    include_generative=True,
    )

exchange.lk_generative_dialogflow()