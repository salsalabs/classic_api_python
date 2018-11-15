"""Export Salsa Classic data to Engage.

This package is an application to export Salsa Classic data in a form that can
easily be imported into Salsa Engage.  

The general process is to read all supporters, filter out the ones that don't
need to be exported, then export the rest as a CSV.  This app also exports 
donations made by the supporters groups and the supporter belonged to.

The application starts from the command line.  The user provides a file
containing Salsa campaign manager credentials.  The user may also set
the directory where CSV files will be written.

python3 splitter/app.py --help
usage: app.py [-h] [--login LOGINFILE] [--dir OUTPUTDIR]

Export supporters, groups and donations for export to Engage.

optional arguments:
  -h, --help         show this help message and exit
  --login LOGINFILE  YAML file with login credentials
  --dir OUTPUTDIR    Store export files here
  --start START      Start extraction at this offset

 - LOGINFILE is a YAML formatted file that holds the credentials necessary
   to use the Salsa Classic API.  Here's an example.

host: salsa4.salsalabs.com
email: campaign-manager@whatever.com
password: Extra-super-secret-password

 - OUTPUTDIR will be created if it does not already exist.

 - START is the number of records to skip before reading from Salsa.
 
Note:

This is a Python version 3 app.  It will *not* run using Python version 2.

"""
