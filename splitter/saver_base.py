
import csv
import os
import os.path
import pathlib
import threading

class SaverBase (threading.Thread):
    """
    Base class inherited by Saver classes.
    """

    def __init__(self, **kwargs):
        """
        Initialize a SaverBase by applying the keyword arguments,
        and setting instance variables used when writing files. 
        """

        threading.Thread.__init__(self)
        self.__dict__.update(kwargs)

        self.threadName = type(self).__name__
        self.fileRoot = self.threadName.replace("Saver", "").replace("Reader","" ).lower()
        self.csvfile = None
        self.maxRecs = 50000
        self.fileNum = 1

    def getField(self):
        """
        Return a dictionary of output field names and Salsa field names.  The
        list of output fields specifies the order that fields should appear
        in the CSV output.
        """
        sys.exit(f"Error: you MUST override SaverBase.fieldMap in {self.threadName}")
        
    def openFile(self):
        """
        Open a new CSV file.  The filename contains the thread ID and
        a current file serial number.
        """

        while True:
            fn = f"{self.fileRoot}_{self.threadID:02d}_{self.fileNum:02d}.csv"
            fn = os.path.join(self.outputDir, fn)
            self.fileNum = self.fileNum + 1
            p = pathlib.Path(fn)
            if not p.is_file():
                break

        d = os.path.dirname(fn)
        if not os.path.exists(d):
            os.makedirs(d)
        if self.csvfile != None:
            self.csvfile.flush()
            self.csvfile.close()
        self.csvfile = open(fn, "w")
        fieldnames = []
        for k, v in self.getFieldMap().items():
            if v:
                fieldnames.append(k)
        self.writer = csv.DictWriter(self.csvfile, fieldnames=fieldnames)
        self.writer.writeheader()

    def run(self):
        """
        Run the thread.  Overrides Threading.run() to process the data and
        close the CSV file.
        """

        self.log.info(("Starting " + self.threadName))
        self.process_data()
        self.csvfile.flush()
        self.csvfile.close()
        self.log.info(("Ending  " + self.threadName))

    def process_data(self):
        """
        Read records from a queue and write them to a CSV file.  You *must* 
        override this method.
        """

        sys.exit(f"Error: you MUST override SaverBase.process_data in {self.threadName}")
