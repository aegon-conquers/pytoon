import requests

# URL of the API endpoint
api_url = "https://example.com/api/endpoint"

# Sample JSON request body
json_data = {
    "attribute1": "value1",
    "attribute2": "value2"
}

# Send POST request with JSON data
response = requests.post(api_url, json=json_data)

# Check the response status code
if response.status_code == 200:
    print("Request successful!")
    print("Response:", response.text)
else:
    print("Request failed. Status code:", response.status_code)
