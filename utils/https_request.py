import requests
import os
import time
from typing import Callable, Dict, Any
from colorama import init, Fore, Style
# Initialize colorama for Windows compatibility
init()

class HTTPSUtils:

    @staticmethod
    def with_api_retry(url: str, payload: Dict, process_response: Callable[[requests.Response], Dict], max_retries: int = 10, delay: int = 1) -> Dict:
        """
        Generic function to make an API call with retries and custom response processing.

        Args:
            url: The API endpoint URL.
            payload: The JSON payload to send in the POST request.
            process_response: A function to process the API response.
            max_retries: Maximum number of retries on timeout.
            delay: Delay between retries in seconds.

        Returns:
            A dictionary based on the processed response or a default value on failure.
        """
        retries = 0 
        while retries < max_retries:
            try:
                response = requests.post(url, json=payload)
                response.raise_for_status()  # Raise an exception for HTTP errors
                return process_response(response)  # Call the passed processing function
            
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(delay)
            except requests.exceptions.RequestException as e:
                print(f"{Fore.RED}Request error:{Style.RESET_ALL} {e}")
                break  # Exit for non-retryable errors

        return None
    

    @staticmethod
    def with_api_retry_GET(url: str, process_response: Callable[[requests.Response], Dict], max_retries: int = 10, delay: int = 1) -> Dict:
        """
        Generic function to make an API call with retries and custom response processing.

        Args:
            url: The API endpoint URL.
            payload: The JSON payload to send in the POST request.
            process_response: A function to process the API response.
            max_retries: Maximum number of retries on timeout.
            delay: Delay between retries in seconds.

        Returns:
            A dictionary based on the processed response or a default value on failure.
        """
        retries = 0 
        while retries < max_retries:
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise an exception for HTTP errors
                return process_response(response)  # Call the passed processing function
            
            except requests.exceptions.Timeout:
                retries += 1
                time.sleep(delay)
            except requests.exceptions.RequestException as e:
                print(f"{Fore.RED}Request error:{Style.RESET_ALL} {e}")
                break  # Exit for non-retryable errors

        return None
    
