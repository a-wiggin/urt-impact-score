import os
import time
import requests

def download_json(url, save_dir="downloads", filename="data.json", delay_seconds=1):
    """
    Downloads a JSON file from a given URL and saves it to a specified directory,
    with a delay to respect rate limits.

    Args:
        url (str): The URL to fetch the JSON from.
        save_dir (str): Directory where the file will be saved.
        filename (str): Name of the file to save.
        delay_seconds (int | float): Delay in seconds before the request.
    """
    # Rate limiting
    print(f"Waiting for {delay_seconds} seconds to respect rate limit...")
    time.sleep(delay_seconds)

    try:
        print(f"Fetching from {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        data = response.json()

        # Ensure the directory exists
        os.makedirs(save_dir, exist_ok=True)
        file_path = os.path.join(save_dir, filename)

        # Save the JSON data to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            import json
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"JSON data saved to {file_path}")

    except requests.RequestException as e:
        print(f"Error downloading JSON: {e}")
    except ValueError as e:
        print(f"Error parsing JSON: {e}")


if __name__ == "__main__":
    for game_number in range(118, 19007): #18385
        url = f"https://ftwgl.net/api/match-round/{game_number}/stats"  # Replace with your target URL
        try:
            download_json(url, save_dir="my_jsons", filename=f"{game_number}.json", delay_seconds=0.1)
        except Exception as err:
            print(game_number)
            print(str(err))
            pass
