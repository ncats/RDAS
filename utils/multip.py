from multiprocessing import Pool, cpu_count

class MultiprocessingManager:

    def __init__(self, num_processes=None):
        # If num_processes is not provided, use a default (e.g., all CPU cores)
        # It's good practice to leave some cores free if your main thread
        # is also doing work, or if other processes are running.
        if not num_processes:
             self._num_processes = cpu_count()-1
        else:
             self._num_processes = min(num_processes, cpu_count()-1)

        self.pool = None


    def __enter__(self):
        # __enter__ does not take 'num_tasks' as an argument from the 'with' statement.
        # It takes 'self' only. The number of processes should be decided during init.
        print(f"Entering context: Initializing Pool with {self._num_processes} processes.")
        self.pool = Pool(processes=self._num_processes)
        print("Pool initialized.")

        return self.pool # The object returned here is assigned to the 'as' variable


    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.pool:
            print("Exiting context: Closing and joining Pool.")
            self.pool.close() # Prevents new tasks from being submitted
            self.pool.join()  # Waits for all worker processes to terminate
            self.pool = None
        # Returning False (or implicitly None) propagates exceptions.
        # Returning True would suppress exceptions. Generally, let them propagate.

