#!/usr/bin/env python3
"""Valid Python file for testing."""

import sys
import logging

def main():
    """Main function with proper syntax."""
    logging.basicConfig(level=logging.INFO)
    
    try:
        print("This is a valid Python file")
        test_list = [1, 2, 3, 4, 5]
        
        for item in test_list:
            logging.info(f"Processing item: {item}")
        
        return 0
    
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
