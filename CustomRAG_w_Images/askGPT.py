import requests

def askGPT(question_asked, pryon_data, default_prompt):
    openai_endpoint = "https://api.openai.com/v1/chat/completions"
    
    api_key = "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" 


    full_prompt = default_prompt.replace("{QUESTION}", question_asked).replace("{CONTEXT}", pryon_data)

    try:
        response = requests.post(
            openai_endpoint,
            json={
                "model": "gpt-4-0125-preview",
                "messages": [
                    {"role": "user", "content": full_prompt}
                ],
                "max_tokens": 300,
                "presence_penalty": 0,
                "temperature": 1,
                "top_p": 1
            },
            headers={
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
        )

        response_data = response.json()
        if response_data['choices'] and response_data['choices'][0]['message']['content']:
            return response_data['choices'][0]['message']['content'].strip()
        else:
            return "No response from GPT-3.5 Turbo"

    except Exception as e:
        print('Error in ask_gpt: ', e)
        return "An error occurred while processing your request."