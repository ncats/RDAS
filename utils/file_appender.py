
import os


class FileAppender:

    def __init__(self, filename):
        '''
        Open the file in append mode. If filename exists, append log messages to
        it; otherwise create a new file and append log messages to the new file.
        '''
        dirname = os.path.dirname(filename)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        self.file = open(filename, 'a')
        ''' Store the filename for optional reference. '''
        self.filename = filename


    def append_to_file(self, text):
        ''' Write text with a newline using the stored file object. '''
        self.file.write(text + '\n')
        ''' Call self.file.flush() here if immediate disk writes are needed. '''


    def append_and_print(self, text): 
        ''' Append the text to the file. '''
        self.append_to_file(text)

        ''' Print the text to the console. '''
        print(text)


    def log_stdout(self, text): 
        self.append_and_print(text)


    def close(self):
        ''' Close the file explicitly. '''
        if hasattr(self, 'file') and not self.file.closed:
            self.file.close()


    def __del__(self):
        ''' Ensure the file is closed when the object is garbage-collected. '''
        self.close()
