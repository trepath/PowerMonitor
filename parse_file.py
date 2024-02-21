import re

def summarize_log(file_path, key_phrases):
    # Regular expression to match datetime stamps in log entries
    # Adjust the regex pattern according to your log file's datetime format
    datetime_pattern = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')

    # Initialize a dictionary to hold the counts of unique log entries (without datetime) for each key phrase
    unique_entries = {phrase: {} for phrase in key_phrases}

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                for phrase in key_phrases:
                    if phrase in line:
                        # Remove datetime from the line to normalize it
                        normalized_line = datetime_pattern.sub('', line).strip()
                        # Count the occurrence of the normalized line
                        if normalized_line in unique_entries[phrase]:
                            unique_entries[phrase][normalized_line] += 1
                        else:
                            unique_entries[phrase][normalized_line] = 1
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    # Output the aggregated summary
    for phrase, entries in unique_entries.items():
        print(f"\nUnique entries for '{phrase}':")
        for entry, count in entries.items():
            print(f"{entry}\nCount: {count}")



# Example usage
file_path = '\\\\uatjaws1.servpoint.net\\logs\\tomcat8-stderr.2024-02-14.log'
key_phrases = ['Jaws Error - relay read from remote service - java.net.SocketTimeoutException', 'getConnectionNamed', 'sendClientUpdateEmail', 'Exception', 'Failed']
summarize_log(file_path, key_phrases)
