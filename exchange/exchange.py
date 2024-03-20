import requests
import json
import os
from google.cloud import translate_v2 as translate
import pandas as pd
import re
import threading
import urllib

class PryonExchange:
    def __init__(
            self,
            client_id: str,
            client_secret: str,
            collection_id: str,
            gcp_credentials: str,

            response_count: int = 4,
            environment: str = 'dev',
            language: str = 'en',
            translate: bool = False
        ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.collection_id = collection_id
        self.gcp_credentials = gcp_credentials
        self.token = self._get_token()
        self.environment = environment
        self.response_count = response_count
        self.language = language
        self.detected_language = None
        self.question: str = None
        self.translate = translate
        
        self.extractive_data: dict = None
        self.translated_extractive_data: dict = None

        self.generative_data: dict = None
        self.translated_generative_data: dict = None


    def _get_token(self) -> str:
        resp = requests.post(
            url="https://login.pryon.net/oauth/token",
            data={
                "grant_type": "client_credentials",
                "audience" : "https://pryon/api",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            },
            headers={
                "Content-type": "application/x-www-form-urlencoded"
            }
        )
        return resp.json().get('access_token', '')
    
    def _translate_text(
            self, 
            text: str,
            mode: str = 'translate'
        ) -> str:

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.gcp_credentials
        client = translate.Client()
        if mode == 'detect':
            translation = client.detect_language(text)
            self.detected_language = translation.get('language')
            return translation.get('language')
        else:
            translate_conditions = [
                self.translate == True,
                self.detected_language,
                self.detected_language != self.language
            ]
            if all(translate_conditions): 
                return client.translate(text, source_language=self.detected_language, target_language=self.language).get('translatedText')
            else:
                print('something went wrong with translation')
                print('translate? ', self.translate)
                print('SELF.DETECTED_LANGUAGE: ', self.detected_language)
                print('SLEF.LANGUAGE: ', self.language)
                return text

    def _translate_extractive_data(self) -> None:
        translated_extractive_data = self.extractive_data

        output = translated_extractive_data.get('data').get('output')
        for obj in output:
            obj['text'] = self._translate_text(obj['text'])
            obj.get('attachments').get('answer_in_context')['content'] = self._translate_text(obj.get('attachments').get('answer_in_context')['content'])
            obj.get('attachments').get('best_1_sentence_text')['content'] = self._translate_text(obj.get('attachments').get('best_1_sentence_text')['content'])

        locations_to_translate = [
            ['data', 'normalized_input', 'raw_text'],
            ['data', 'normalized_input', 'understood_text'],
        ]
        current_data = translated_extractive_data
        for keychain in locations_to_translate:
            temp_data = current_data
            for key in keychain[:-1]:
                temp_data = temp_data.get(key, {})
            temp_data[keychain[-1]] = self._translate_text(temp_data.get(keychain[-1]))
        self.translated_extractive_data = translated_extractive_data
        
    def _translate_generative_data(self) -> None:
        translated_generative_data = self.generative_data

        output = translated_generative_data.get('exchange_response_complete').get('exchange_response_data').get('output')
        for obj in output:
            obj['text'] = self._translate_text(obj['text'])
            obj.get('attachments').get('answer_in_context')['content'] = self._translate_text(obj.get('attachments').get('answer_in_context')['content'])
            obj.get('attachments').get('best_1_sentence_text')['content'] = self._translate_text(obj.get('attachments').get('best_1_sentence_text')['content'])

        locations_to_translate = [
            ['generative_exchange_input_complete', 'exchange_input_text'],
            ['exchange_response_complete', 'exchange_response_data', 'normalized_input', 'raw_text'],
            ['exchange_response_complete', 'exchange_response_data', 'normalized_input', 'understood_text'],
            ['generative_exchange_response_complete', 'generative_exchange_conversation_data', 'data', 'text']
        ]
        current_data = translated_generative_data
        for keychain in locations_to_translate:
            temp_data = current_data
            for key in keychain[:-1]:
                temp_data = temp_data.get(key, {})
            temp_data[keychain[-1]] = self._translate_text(temp_data.get(keychain[-1]))
        self.translated_generative_data = translated_generative_data

    def lk_extractive_dialogflow(self) -> None:
        response_json = self.extractive_data
        query = self.question

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

        # return flattened_response
        df_exchange = pd.DataFrame([flattened_response])

        answer = str(df_exchange.get('best_1_text', default=pd.Series([None]))[0])
        answer2 = str(df_exchange.get('best_2_text', default=pd.Series([None]))[0])
        answer3 = str(df_exchange.get('best_3_text', default=pd.Series([None]))[0])
        answer4 = str(df_exchange.get('best_4_text', default=pd.Series([None]))[0])
        answer5 = str(df_exchange.get('best_5_text', default=pd.Series([None]))[0])

        source1_title = str(df_exchange.get('best_1_content_display_name', pd.Series([None])).iloc[0])
        source2_title = str(df_exchange.get('best_2_content_display_name', pd.Series([None])).iloc[0])
        source3_title = str(df_exchange.get('best_3_content_display_name', pd.Series([None])).iloc[0])
        source4_title = str(df_exchange.get('best_4_content_display_name', pd.Series([None])).iloc[0])
        source5_title = str(df_exchange.get('best_5_content_display_name', pd.Series([None])).iloc[0])


        contextans = str(df_exchange.get('best_1_aic', pd.Series([None])).iloc[0])
        contextans2 = str(df_exchange.get('best_2_aic', pd.Series([None])).iloc[0])
        contextans3 = str(df_exchange.get('best_3_aic', pd.Series([None])).iloc[0])
        contextans4 = str(df_exchange.get('best_4_aic', pd.Series([None])).iloc[0])
        contextans5 = str(df_exchange.get('best_5_aic', pd.Series([None])).iloc[0])


        conf_1_value = df_exchange.get('best_1_score', pd.Series([None])).iloc[0]
        conf_2_value = df_exchange.get('best_2_score', pd.Series([None])).iloc[0]
        conf_3_value = df_exchange.get('best_3_score', pd.Series([None])).iloc[0]
        conf_4_value = df_exchange.get('best_4_score', pd.Series([None])).iloc[0]
        conf_5_value = df_exchange.get('best_5_score', pd.Series([None])).iloc[0]

        conf_1 = float(conf_1_value) if conf_1_value is not None else None
        conf_2 = float(conf_2_value) if conf_2_value is not None else None
        conf_3 = float(conf_3_value) if conf_3_value is not None else None
        conf_4 = float(conf_4_value) if conf_4_value is not None else None
        conf_5 = float(conf_5_value) if conf_5_value is not None else None


        URL = str(df_exchange.get('best_1_url', pd.Series([None])).iloc[0])
        URL2 = str(df_exchange.get('best_2_url', pd.Series([None])).iloc[0])
        URL3 = str(df_exchange.get('best_3_url', pd.Series([None])).iloc[0])
        URL4 = str(df_exchange.get('best_4_url', pd.Series([None])).iloc[0])
        URL5 = str(df_exchange.get('best_5_url', pd.Series([None])).iloc[0])

        content_id_1 = df_exchange.get('best_1_content_id', pd.Series([None])).iloc[0]
        image_page_1 = df_exchange.get('ans1_text_sentence_bbox_1_page', pd.Series([None])).iloc[0]

        content_id_2 = df_exchange.get('best_2_content_id', pd.Series([None])).iloc[0]
        image_page_2 = df_exchange.get('ans2_text_sentence_bbox_1_page', pd.Series([None])).iloc[0]

        content_id_3 = df_exchange.get('best_3_content_id', pd.Series([None])).iloc[0]
        image_page_3 = df_exchange.get('ans3_text_sentence_bbox_1_page', pd.Series([None])).iloc[0]

        content_id_4 = df_exchange.get('best_4_content_id', pd.Series([None])).iloc[0]
        image_page_4 = df_exchange.get('ans4_text_sentence_bbox_1_page', pd.Series([None])).iloc[0]

        content_id_5 = df_exchange.get('best_5_content_id', pd.Series([None])).iloc[0]
        image_page_5 = df_exchange.get('ans5_text_sentence_bbox_1_page', pd.Series([None])).iloc[0]

        page1 = image_page_1
        page2 = image_page_2
        page3 = image_page_3
        page4 = image_page_4
        page5 = image_page_5

        # Append page number to URL if it's a PDF
        if URL.endswith('.pdf'):
            Source1 = f"{URL}#page={page1}"
        else:
            Source1 = URL

        # Append page number to URL2 if it's a PDF
        if URL2.endswith('.pdf'):
            Source2 = f"{URL2}#page={page2}"
        else:
            Source2 = URL2

        # Append page number to URL3 if it's a PDF
        if URL3.endswith('.pdf'):
            Source3 = f"{URL3}#page={page3}"
        else:
            Source3 = URL3

        # Append page number to URL4 if it's a PDF
        if URL4.endswith('.pdf'):
            Source4 = f"{URL4}#page={page4}"
        else:
            Source4 = URL4

        # Append page number to URL5 if it's a PDF
        if URL5.endswith('.pdf'):
            Source5 = f"{URL5}#page={page5}"
        else:
            Source5 = URL5

        # Create image URLs based on the query if the source URLs are PDFs
        query_letters_only = ''.join([char for char in query if char.isalpha()])

        if URL.endswith('.pdf'):
            URL = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}1.png'
        else:
            URL = URL

        if URL2.endswith('.pdf'):
            URL2 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}2.png'
        else:
            URL2 = URL2

        if URL3.endswith('.pdf'):
            URL3 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}3.png'
        else:
            URL3 = URL3

        if URL4.endswith('.pdf'):
            URL4 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}4.png'
        else:
            URL4 = URL4

        if URL5.endswith('.pdf'):
            URL5 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}5.png'
        else:
            URL5 = URL5

        # Dictionary of URLs
        urls = {
            "1": URL,
            "2": URL2,
            "3": URL3,
            "4": URL4,
            "5": URL5,
        }


        # answer = lf.translate_from_english(answer, source_lang)
        # answer2 = lf.translate_from_english(answer2, source_lang)
        # answer3 = lf.translate_from_english(answer3, source_lang)
        answer = self._translate_text(answer)
        answer2 = self._translate_text(answer2)
        answer3 = self._translate_text(answer3)

        # print("Answer: ",answer)
        formatted_html_1 = f"""
        <div><p>{answer}<br><span style='color: grey; font-size: 9px;'>{source1_title}<br> <a href='{Source1}' target="_blank">Source Link</a>&nbsp;<a href='{URL}' target="_blank">Context View</a></span></p></div>
        """
        formatted_html_2 = f"""
        <div><p>{answer2}<br><span style='color: grey; font-size: 9px;'>{source2_title}<br> <a href='{Source2}' target="_blank">Source Link</a>&nbsp;<a href='{URL2}' target="_blank">Context View</a></span></p></div>
        """
        formatted_html_3 = f"""
        <div><p>{answer3}<br><span style='color: grey; font-size: 9px;'>{source3_title}<br> <a href='{Source3}' target="_blank">Source Link</a>&nbsp;<a href='{URL3}' target="_blank">Context View</a></span></p></div>
        """
        # print(formatted_html_1)
        # print(formatted_html_2)
        # print(formatted_html_3)

        
        #Adding a custom OOD function here... to change response. 
        if source1_title.startswith("VA:"):
            dialogflow_cx_response ={
                "fulfillment_response": {
                    "messages": [
                    {
                        "text": {
                        "text": [answer]
                        }
                    }
                    ]
                }
            }
        elif conf_1 < 0.45 or source1_title == "OOD": #Abiility to dynamically change the confidence threshold here without rebuilding collection
            dialogflow_cx_response ={
                "fulfillment_response": {
                    "messages": [
                    {
                        "text": {
                        "text": ["Sorry, I don't have a great answer for you! It might not be in my knowledge base. Maybe try rephrasing? Perhaps I can help with something else?"]
                        }
                    }
                    ]
                },
                "sessionInfo": {
                    "parameters": {
                    "paramName": "paramValue"
                    }
                },
                "targetPage": "projects/df-cx-dto/locations/us-east1/agents/9b598888-87d1-4ec3-9f88-13bda29a15c9/flows/00000000-0000-0000-0000-000000000000/pages/START_PAGE",
                "triggerEvent": "OOD_EVENT"
                }
        else:
            # richContent is where you can modify dialogflow messenger data. 
            dialogflow_cx_response = {
                "fulfillment_response": {
                    "messages": [
                        #{
                        #    "text": {
                        #        "text": [answer] ##CHANGE THIS to g_answer FOR GENERATIVE
                        #    }
                        #},
                        {
                            "payload": {
                                "richContent": [[
                                        #HTML TEST
                                        {
                                            "type": "html",
                                            "html": [formatted_html_1]
                                        },
                                        {
                                            "type": "html",
                                            "html": [formatted_html_2]
                                        },
                                                                            {
                                            "type": "html",
                                            "html": [formatted_html_3]
                                        }
                                    ]
                                ]
                            }
                        }
                    ]
                }
            }

        # print("Dialogflow CX Response:", dialogflow_cx_response)
        return dialogflow_cx_response

    def lk_generative_dialogflow(self) -> None:
        def check_and_append_markers(g_answer):
            # Define the markers to look for
            markers = ["[1]", "[2]", "[3]", "[4]", "[5]"]
            
            # Check if any of the markers are in g_answer
            marker_found = any(marker in g_answer for marker in markers)
            
            # If no markers found, append "[1] [2]"
            if not marker_found:
                g_answer += " [1] [2]"
            
            return g_answer

        def clean_sources(text):
            cleaned_text = re.sub(r'[^A-Za-z ]+', '', text)
            return cleaned_text

        def clean_text(text):
            # Remove non-ASCII characters
            cleaned_text = re.sub(r'[^\x00-\x7F]+', '', text)
            return cleaned_text

        def clean_query(query):
            query_no_quotes = query.replace('"', '').replace("'", "")
            query_single_line = query_no_quotes.replace('\n', '').replace('\r', '')
            return query_single_line

        def replace_with_links(text, urls):
            # Regular expression to find the numbered references
            pattern = r"\[(\d+)\]"
            
            # Replacement function
            def repl(match):
                number = match.group(1)  # Extract the number from the match
                url = urls.get(number, "#")  # Get the URL for the number, default to "#" if not found
                return f'<a href="{url}" target="_blank" style="font-size: 10px;">{number}</a>'  # Return the HTML link
            
            # Replace all occurrences in the text
            return re.sub(pattern, repl, text)

        def send_to_another_webhook(query):
            # Define the URL of the second webhook
            second_webhook_url = 'https://us-east4-dto-pryon-chatbot-dkuv.cloudfunctions.net/pryon-sandbox-image-call-dev'

            # Prepare the data to send
            data = {
                'text': query
            }

            # Make the POST request in a separate thread
            def post_data():
                try:
                    requests.post(second_webhook_url, json=data)
                    print("Data sent to the second webhook successfully.")
                except Exception as e:
                    print(f"Failed to send data to the second webhook: {e}")

            # Start the thread
            thread = threading.Thread(target=post_data)
            thread.start()

        rows = []
        query = clean_query(self.question)
        df = pd.DataFrame({"Question": [query]})
        for i in range(len(df)):
            row = df.iloc[i]
            output = self.generative_data.get('exchange_response_complete').get('exchange_response_data').get('output')
            answers = [[a for a in output if a['output_id'] == f'best_{i}'][0] for i in range(1, len(output) + 1)]


        urls = {}
        docs = {}

        for a in answers:
            if 'content_source_location' in a['attachments']:
                urls[a['output_id']] = a['attachments']['content_source_location']['content']
                docs[a['output_id']] = urllib.parse.unquote(
                    a['attachments']['content_display_name']['content'].split('/')[-1])

            elif a['attachments']['level']['content'] == 'REJECT':
                urls[a['output_id']] = ""
                docs[a['output_id']] = "OOD"

            if a['attachments'].get('answer_approval_id') != None:
                docs[a['output_id']] = "VA: {}".format(a['attachments'].get('answer_approval_id')['content'])
                urls[a['output_id']] = "VA: {}".format(a['attachments'].get('answer_approval_id')['content'])

        # print("Urls:" , urls)
        # print("Docs:" , docs)


        answerobjs = [
            {
                'output_id': a.get('output_id', ''),
                'text': a.get('text', ''),
                'url': urls.get(a.get('output_id', ''), ''),
                'doc_name': docs.get(a.get('output_id', ''), ''),
                'aic': a.get('attachments', {}).get('answer_in_context', {}).get('content', ''),
                'confidence': a.get('attachments', {}).get('score', {}).get('content', '0.0'),
                'page_num': a.get('attachments', {}).get('end_page', {}).get('content', None),
                'context': a.get('context', ''),
                'content_id': a.get('attachments', {}).get('content_id', {}).get('content', '')
            }
            for a in answers
        ]
        pred_answers = answerobjs[:self.response_count]
        for n, pred_answer in enumerate(pred_answers[:self.response_count]):
            row[f'Pred_Answer_{n + 1}'] = str(pred_answer['text'])
            row[f'Pred_Context_{n + 1}'] = str(pred_answer['aic'])
            row[f'Pred_URL_{n + 1}'] = str(pred_answer['url'])
            row[f'Pred_Doc_{n + 1}'] = str(pred_answer['doc_name'])
            row[f'Pred_Conf_{n + 1}'] = pred_answer['confidence']
            row[f'Pred_EndPage_{n + 1}'] = pred_answer['page_num']
            row[f'content_id_{n + 1}'] = pred_answer['content_id']
            # row[f'Pred_Options_{n + 1}'] = "na"
            ### add approval ID as sep column for VA tracking? ~kg
            if pred_answer.get('context'):
                row[f'Pred_Options_{n + 1}'] = pred_answer.get('context').get('augmentation')
        generation = self.generative_data.get('generative_exchange_response_complete').get('generative_exchange_conversation_data').get('data').get('text')
        row[f'Generative_Answer'] = str(generation)
        # row[f'Exchange Duration'] = str(ex_duration)
        # row[f'Generative Duration'] = str(gen_duration)
        rows.append(row)
        df_outputs = pd.DataFrame(rows)

        answer = str(df_outputs.get('Pred_Answer_1', default=pd.Series([None]))[0])
        print("Answer variable: ", answer)

        source1_title = clean_sources(str(df_outputs.get('Pred_Doc_1', default=pd.Series([None]))[0]))
        source2_title = clean_sources(str(df_outputs.get('Pred_Doc_2', default=pd.Series([None]))[0]))
        source3_title = clean_sources(str(df_outputs.get('Pred_Doc_3', default=pd.Series([None]))[0]))
        source4_title = clean_sources(str(df_outputs.get('Pred_Doc_4', default=pd.Series([None]))[0]))
        source5_title = clean_sources(str(df_outputs.get('Pred_Doc_5', default=pd.Series([None]))[0]))

        contextans = str(df_outputs.get('Pred_Context_1', default=pd.Series([None]))[0])
        conf_1 = float(df_outputs.get('Pred_Conf_1', default=pd.Series([None]))[0])

        URL = str(df_outputs.get('Pred_URL_1', default=pd.Series([None]))[0])
        URL2 = str(df_outputs.get('Pred_URL_2', default=pd.Series([None]))[0])
        URL3 = str(df_outputs.get('Pred_URL_3', default=pd.Series([None]))[0])
        URL4 = str(df_outputs.get('Pred_URL_4', default=pd.Series([None]))[0])
        URL5 = str(df_outputs.get('Pred_URL_5', default=pd.Series([None]))[0])

        page1 = str(df_outputs.get('Pred_EndPage_1', default=pd.Series([None]))[0])
        page2 = str(df_outputs.get('Pred_EndPage_2', default=pd.Series([None]))[0])
        page3 = str(df_outputs.get('Pred_EndPage_3', default=pd.Series([None]))[0])
        page4 = str(df_outputs.get('Pred_EndPage_4', default=pd.Series([None]))[0])
        page5 = str(df_outputs.get('Pred_EndPage_5', default=pd.Series([None]))[0])



        # Determine if URLs are PDFs and append page numbers if they are
        if URL.endswith('.pdf'):
            Source1 = f"{URL}#page={page1}"
        else:
            Source1 = URL

        if URL2.endswith('.pdf'):
            Source2 = f"{URL2}#page={page2}"
        else:
            Source2 = URL2

        if URL3.endswith('.pdf'):
            Source3 = f"{URL3}#page={page3}"
        else:
            Source3 = URL3

        # Extend logic to URL4 and URL5
        if URL4.endswith('.pdf'):
            Source4 = f"{URL4}#page={page4}"
        else:
            Source4 = URL4

        if URL5.endswith('.pdf'):
            Source5 = f"{URL5}#page={page5}"
        else:
            Source5 = URL5

        # Only alphanumeric characters in query for image URLs
        query_letters_only = ''.join([char for char in query if char.isalpha()])

        # Construct URLs for images
        URL = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}1.png'
        URL2 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}2.png'
        URL3 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}3.png'
        URL4 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}4.png'
        URL5 = f'https://storage.googleapis.com/dto_pryon_images/{query_letters_only}5.png'

        # Create a dictionary mapping keys to URLs, now including 4 and 5
        urls = {
            "1": URL,
            "2": URL2,
            "3": URL3,
            "4": URL4,
            "5": URL5,
        }


        g_answer = str(df_outputs.loc[0, 'Generative_Answer']) #UNCOMMENT THIS FOR GENERATIVE
        g_answer = clean_text(g_answer)
        g_answer = check_and_append_markers(g_answer)
        g_answer = self._translate_text(g_answer)
        
        gen_ood=""
        if "sorry" in g_answer.lower():
            gen_ood = "Sorry, but that seems outside my knowledge database. Pryon focuses on extractive answers only, so I am unable to answer your question effectively with the source material in this collection. I can answer any question about Other Transactions and information from the Strategic Institute"
    
        print("Generative Response:", g_answer)
        print("OOD:", gen_ood)
        
        formatted_generative = replace_with_links(g_answer, urls)


        sources_string = ""
        source_order = []

        # Find all occurrences of [1], [2], [3], [4], and [5] and their order
        for marker in ["[1]", "[2]", "[3]", "[4]", "[5]"]:
            if marker in g_answer:
                source_order.append((g_answer.index(marker), marker))

        # Sort by order of occurrence
        source_order.sort()

        # Initialize an empty string to accumulate source references
        sources_string = ''

        # Append sources to the string based on their order of appearance
        for _, marker in source_order:
            if marker == "[1]":
                sources_string += f'<a href="{Source1}" target="_blank" style="font-size: 6px;">SOURCE 1</a><span style="font-size: 10px; color: grey;">  {source1_title}</span><br>'
            elif marker == "[2]":
                sources_string += f'<a href="{Source2}" target="_blank" style="font-size: 6px;">SOURCE 2</a><span style="font-size: 10px; color: grey;">  {source2_title}</span><br>'
            elif marker == "[3]":
                sources_string += f'<a href="{Source3}" target="_blank" style="font-size: 6px;">SOURCE 3</a><span style="font-size: 10px; color: grey;">  {source3_title}</span><br>'
            elif marker == "[4]":
                sources_string += f'<a href="{Source4}" target="_blank" style="font-size: 6px;">SOURCE 4</a><span style="font-size: 10px; color: grey;">  {source4_title}</span><br>'
            elif marker == "[5]":
                sources_string += f'<a href="{Source5}" target="_blank" style="font-size: 6px;">SOURCE 5</a><span style="font-size: 10px; color: grey;">  {source5_title}</span><br>'

        # Remove the last <br> tag if sources_string is not empty
        if sources_string.endswith("<br>"):
            sources_string = sources_string[:-4]

        # The final HTML formatted string of sources
        formatted_html = sources_string

        print("Sources String", sources_string)




        #Adding a custom OOD function here... to change response. 
        if source1_title.startswith("VA:"):
            dialogflow_cx_response ={
                "fulfillment_response": {
                    "messages": [
                    {
                        "text": {
                        "text": [answer]
                        }
                    }
                    ]
                }
            }
        elif "sorry" in g_answer.lower():
            dialogflow_cx_response ={
                "fulfillment_response": {
                    "messages": [
                    {
                        "text": {
                        "text": [gen_ood]
                        }
                    }
                    ]
                }
            }
        else:
            # richContent is where you can modify dialogflow messenger data. 
            dialogflow_cx_response = {
                "fulfillment_response": {
                    "messages": [
                        {
                            "payload": {
                                "richContent": [[
                                        #HTML TEST
                                        {
                                            "type": "html",
                                            "html": [formatted_generative]
                                        },
                                        {
                                            "type": "html",
                                            "html": [formatted_html]
                                        }
                                    ]
                                ]
                            }
                        }
                    ]
                }
            }


        print("Dialogflow CX Response:", dialogflow_cx_response)
        return dialogflow_cx_response
    
    def get_data(
            self,
            query: str,
            include_generative: bool = False,
        ) -> None:
        self.question = query
        query = {
            "input": {
                "option": {
                    "collection_id": self.collection_id,
                    "audio_output_disabled": True,
                    "max_outputs": self.response_count
                },
                "raw_text": self.question
            }
        }

        if self.environment == 'prod':
            exchange_url = "https://api.pryon.net/api/conversation/v1alpha1/exchange"
        else:
            exchange_url = "https://api.pryon.dev/api/conversation/v1alpha1/exchange"

        resp = requests.post(
            url=exchange_url,
            json=query,
            headers={'Authorization': 'Bearer ' + self.token}
        )
        self.extractive_data = resp.json()

        self._translate_text(self.question, mode='detect')

        if self.translate and self.language != self.detected_language:
            self._translate_extractive_data()
        else:
            self.translated_extractive_data = self.extractive_data

        if include_generative:
            query = '''
                {{
                    "input": {{
                        "option": {{
                            "max_outputs": {},
                            "audio_output_disabled": true,
                            "collection_id": "{}"
                        }},
                        "raw_text": "{}"
                    }}
                }}
            '''.format(self.response_count, self.collection_id, self.question)

            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'
            }

            if self.environment == "prod":
                conv_url = "https://api.pryon.net/api/conversation/v1alpha1/exchange-events/sse"
            else:
                conv_url = "https://api.pryon.dev/api/conversation/v1alpha1/exchange-events/sse"

            resp = requests.post(
                url = conv_url,
                data = query,
                headers = headers
            )

            generative_data = {}
            for line in resp.text.split('\n'):
                if line.strip() != "":
                    line = json.loads(line.lstrip('data: '))
                    if 'metadata' in line.keys():
                        name = line.get('state').lower().strip()
                        generative_data[name] = line
            self.generative_data = generative_data
            if self.translate and self.language != self.detected_language:
                self._translate_generative_data()
            else:
                self.translated_generative_data = self.generative_data