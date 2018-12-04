"""Export supporters and tags to CSV files.

This package is an application that reads a Classic instance and saves
supporters and their assigned tags.  Output is supporter info with the
tag data as a comma-separated value.

The general process is to read all supporters, tag_data and tags.  The
data is formatted as a CSV and written.

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
 
Note:

This is a Python version 3 app.  It will *not* run using Python version 2.

"""
