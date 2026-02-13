
class FileAppender:

    def __init__(self, filename):
        # Open the file in append mode
        self.file = open(filename, 'a')
        self.filename = filename  # Store for reference (optional)


    def append_to_file(self, text):
        # Write text with a newline using the stored file object
        self.file.write(text + '\n')
        # Optionally flush to ensure data is written immediately
        # self.file.flush()


    def append_and_print(self, text): 
        # Append the text to the file
        self.append_to_file(text)

        # Print the text to the console
        print(text)


    def log_stdout(self, text): 
        self.append_and_print(text)


    def close(self):
        # Close the file explicitly
        if hasattr(self, 'file') and not self.file.closed:
            self.file.close()


    def __del__(self):
        # Ensure the file is closed when the object is garbage-collected
        self.close()
