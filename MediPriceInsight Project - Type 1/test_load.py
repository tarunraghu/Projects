import requests
import json

def test_load_split_files():
    # URL for the load-split-files endpoint
    url = 'http://localhost:5000/load-split-files'
    
    try:
        # Make POST request to load split files
        response = requests.post(url)
        
        # Parse the response
        result = response.json()
        
        # Print the result
        print("Response:", json.dumps(result, indent=2))
        
        if result.get('success'):
            print("\nSuccessfully loaded data from split files!")
        else:
            print("\nError loading data:", result.get('error'))
            
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server. Make sure the Flask app is running.")
    except Exception as e:
        print("Error:", str(e))

if __name__ == '__main__':
    test_load_split_files() 