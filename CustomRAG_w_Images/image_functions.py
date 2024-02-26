import requests, json, os, csv, argparse, time
import pandas as pd
from PIL import Image, ImageDraw
from tqdm import tqdm
import subprocess
from google.cloud import storage
from config import ENVIRONMENT, KD_ID, CLIENT_ID, CLIENT_SECRET, MAX_OUTPUTS, AUTH_URL, EXCHANGE_ENDPOINT, API_BASE_URL
import image_functions as imgf
import json
from io import BytesIO



def LeoE2E(df_in):
    prompts = df_in['Question']

    auth_payload = json.loads(f'{{"audience":"https://pryon/api", "grant_type":"client_credentials", "client_id":"{CLIENT_ID}", "client_secret":"{CLIENT_SECRET}"}}')
    auth_response = requests.post(AUTH_URL, headers={'Content-Type':'application/json'}, json=auth_payload)
    access_token = auth_response.json()['access_token']
        
    exchange_responses = [get_exchange_response(prompt, access_token) for prompt in tqdm(prompts)]
    df_exchange = pd.DataFrame(exchange_responses)
    df_exchange.to_csv('tmp/exchange_responses.csv', index=False)

    total_exchange_response_time = sum(df_exchange['response_time_millis']) / 1000 #in seconds
    total_run_time = (pd.Timestamp(df_exchange.iloc[-1]['create_time']) - pd.Timestamp(df_exchange.iloc[0]['create_time'])).total_seconds() + df_exchange.iloc[-1]['response_time_millis']/1000

    print(f"Sum total API Exchange latency: {total_exchange_response_time} seconds")
    print(f"Overall runtime for this dataset: {total_run_time} seconds")


    return df_exchange, access_token

def upload_to_gcp(content_id, image_obj, bucket_name, credentials_json, ans):
    destination_blob_name = f'{ans}.png'
    storage_client = storage.Client.from_service_account_json(credentials_json)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    buffer = BytesIO()
    image_obj.save(buffer, format='PNG')
    buffer.seek(0)
    blob.upload_from_file(buffer, content_type='image/png')
    print(f"Image uploaded to {destination_blob_name}.")

def highlighted_contextimage(df_exchange, image, ans):
    df = df_exchange.applymap(lambda x: pd.to_numeric(x, errors='ignore'))

    # Assume 'image' is already a PIL Image object, so no need to load it again

    # Scale down the image
    scaling_factor = 0.3  # 30% of the original size
    new_size = (int(image.size[0] * scaling_factor), int(image.size[1] * scaling_factor))
    image = image.resize(new_size, Image.ANTIALIAS)

    # Adjusted scaling factors based on resized image dimensions
    scale_x = new_size[0] / df.iloc[0][f'ans{ans}_text_sentence_bbox_1_image_x']
    scale_y = new_size[1] / df.iloc[0][f'ans{ans}_text_sentence_bbox_1_image_y']

    # Create a transparent overlay on the resized image
    overlay = Image.new('RGBA', new_size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(overlay)

    # Define the light blue color with 20% transparency
    light_blue_transparent = (100, 149, 237, 125)  # Light blue with 20% opacity

    # Iterate over the rows of the dataframe
    for index, row in df.iterrows():
        # Check for each possible 'best_1_x1_N' and corresponding 'best_1_y1_N', 'best_1_x2_N', 'best_1_y2_N'
        for n in range(1, 11):  # Assuming 'N' goes up to 10
            x1_col = f'ans{ans}_text_sentence_bbox_{n}_x1'
            y1_col = f'ans{ans}_text_sentence_bbox_{n}_y1'
            x2_col = f'ans{ans}_text_sentence_bbox_{n}_x2'
            y2_col = f'ans{ans}_text_sentence_bbox_{n}_y2'
            # Check if these columns exist in the dataframe
            if x1_col in df.columns and y1_col in df.columns and x2_col in df.columns and y2_col in df.columns:
                # Apply adjusted scaling to the coordinates and draw the rectangle
                draw.rectangle(
                    [(row[x1_col] * scale_x, row[y1_col] * scale_y),
                     (row[x2_col] * scale_x, row[y2_col] * scale_y)],
                    fill=light_blue_transparent
                )

    # Apply the overlay to the resized image
    image_with_overlay = Image.alpha_composite(image.convert('RGBA'), overlay)

    return image_with_overlay  # Return the image object with highlighted areas


def get_exchange_response(prompt, access_token):
    exchange_payload = { 
        "conversation_id":"", 
        "input":
        {
            "raw_text":f"{prompt}", 
            "option": {
                "collection_id": f"{KD_ID}",
                "audio_output_disabled": True,
                "max_outputs": MAX_OUTPUTS,
            },
            "language_id": "en-US",
            "recommended_questions": "RELATED_AND_FOLLOWUP"
        }
    }
    
    response = requests.post(API_BASE_URL + EXCHANGE_ENDPOINT, 
        headers={'Authorization': 'Bearer ' + access_token},
        json=exchange_payload)

    response_json = response.json()
    # Specify the filename you want to save the data to
    filename = 'response_data.txt'

# Open the file in write mode ('w') and use the json.dump() function to write the JSON data
    with open(filename, 'w') as file:
        json.dump(response_json, file, indent=4)
    
    exchange_id = response_json['data'].get('exchange_id')

    flattened_response = {
        "create_time": response_json['metadata']['create_time'],
        "response_time_millis": response_json['metadata']['response_time_millis'],
        "uuid": response_json['metadata']['uuid'],
        "understood_text": response_json['data']['normalized_input']['understood_text'],
        "exchange_id": exchange_id  
    }
    
    for n, best_n_response in enumerate(response_json['data']['output']):
        try:
            flattened_response[f'best_{n+1}_text'] = best_n_response['text']
        except KeyError:
            flattened_response[f'best_{n+1}_text'] = None

        try:
            content_id = best_n_response['attachments']['content_id']['content']
            flattened_response[f'best_{n+1}_content_id'] = content_id
        except KeyError:
            flattened_response[f'best_{n+1}_content_id'] = None

        try:
            image_page = best_n_response['attachments']['start_page']['content']
            flattened_response[f'best_{n+1}_image_page'] = image_page
        except KeyError:
            flattened_response[f'best_{n+1}_image_page'] = None

        try:
            flattened_response[f'best_{n+1}_aic'] = best_n_response['attachments']['answer_in_context']['content']
        except KeyError:
            flattened_response[f'best_{n+1}_aic'] = None

        try:
            flattened_response[f'best_{n+1}_content_display_name'] = best_n_response['attachments']['content_display_name']['content']
        except KeyError:
            flattened_response[f'best_{n+1}_content_display_name'] = None

        try:
            flattened_response[f'best_{n+1}_content_group'] = best_n_response['attachments']['content_group_display_name']['content']
        except KeyError:
            flattened_response[f'best_{n+1}_content_group'] = None

        try:
            flattened_response[f'best_{n+1}_score'] = float(best_n_response['attachments']['score']['content'])
        except (KeyError, ValueError):
            flattened_response[f'best_{n+1}_score'] = None
        
        try:
            flattened_response[f'best_{n+1}_url'] = best_n_response['attachments']['content_source_location']['content']
        except (KeyError, ValueError):
            flattened_response[f'best_{n+1}_url'] = None
        
        try:
            flattened_response[f'best_{n+1}_page'] = best_n_response['attachments']['end_page']['content']
        except (KeyError, ValueError):
            flattened_response[f'best_{n+1}_page'] = None

        for n, best_n_response in enumerate(response_json['data']['output']):
            output_id = best_n_response['output_id']  # Extract the output_id for each best_n
            ans_prefix = output_id.replace('best_', 'ans') + '_'  # Create a prefix like "ans1_", "ans2_", etc.

        # Function to extract bbox data
        def extract_bbox_data(best_n_response, flattened_response, ans_identifier):
            # Define the base key for text_sentence_bbox based on the provided pattern
            base_bbox_key = 'best_1_text_sentence_bbox'
            # Determine the number of bbox keys to check, assuming a maximum of 10 for safety
            max_bbox_count = 10

            # Check for each bbox key in the attachments
            for i in range(1, max_bbox_count + 1):
                bbox_key = f'{base_bbox_key}_{i}'
                try:
                    bbox_content = best_n_response['attachments'][bbox_key]['content']
                    coords = bbox_content.split(',')
                    if len(coords) == 7:  # Expected format: "x1,y1,x2,y2,page,image_x,image_y"
                        x1, y1, x2, y2, page, image_x, image_y = coords
                        # Construct a unique key for each bbox and assign the coordinates
                        bbox_key_base = f'{ans_identifier}_text_sentence_bbox_{i}'
                        flattened_response[f'{bbox_key_base}_x1'] = x1
                        flattened_response[f'{bbox_key_base}_y1'] = y1
                        flattened_response[f'{bbox_key_base}_x2'] = x2
                        flattened_response[f'{bbox_key_base}_y2'] = y2
                        flattened_response[f'{bbox_key_base}_page'] = page
                        flattened_response[f'{bbox_key_base}_image_x'] = image_x
                        flattened_response[f'{bbox_key_base}_image_y'] = image_y
                except KeyError:
                    # Set None for all bbox components if the bbox key does not exist
                    bbox_key_base = f'{ans_identifier}_text_sentence_bbox_{i}'
                    flattened_response[f'{bbox_key_base}_x1'] = None
                    flattened_response[f'{bbox_key_base}_y1'] = None
                    flattened_response[f'{bbox_key_base}_x2'] = None
                    flattened_response[f'{bbox_key_base}_y2'] = None
                    flattened_response[f'{bbox_key_base}_page'] = None
                    flattened_response[f'{bbox_key_base}_image_x'] = None
                    flattened_response[f'{bbox_key_base}_image_y'] = None

        # Example usage within your existing loop structure:
        for n, best_n_response in enumerate(response_json['data']['output']):
            output_id = best_n_response['output_id']  # e.g., 'best_1', 'best_2', etc.
            ans_identifier = f'ans{n+1}'  # Construct the answer identifier based on the output_id, e.g., 'ans1', 'ans2', etc.
            extract_bbox_data(best_n_response, flattened_response, ans_identifier)

    return flattened_response

def get_image(access_token, content_id, image_page, email='UnknownEmail', uncompressed=0):
    url = f'https://api.pryon.net/api/knowledge/v1alpha1/contents/{content_id}/image?page={image_page}&uncompressed={uncompressed}'

    headers = {
        'Authorization' : "Bearer " + access_token,
        "Content-type": "application/json"
    }

    if email != 'UnknownEmail':
        headers.update({'x-pryon-authenticated-user': email})

    image_response = None

    try:
        response = requests.get(url, headers=headers)
        print (response)
        print(f'\tstatus: {response.status_code}')
        if str(response.status_code) == '400':
            return image_response

        image_response = response.content

    except Exception as e:
        print(f'\tget_image:Err {e}')

    finally:
        if image_response is None:
            print('\timage content is None!')
        else:
            print(f'\timage content found!')

        return image_response