# Tag aggregator

## Description
This application reads supporters and their associated tags.  An output CSV is created using the supporter record and the tags.

The tags are split up into three distinct groups:

* Action
* Event
* Other

Each tag group is catenated together with commas.
With that done, the supporter record is written to a
CSV file.

## Prerequisites

1. Python 3.6 or better.  This application +will not+ run in Python 2.
1. requests: library to handle HTTP requests.
1. [SQLite](https://www.sqlite.org/index.html): a fully SQL-compliant database that doesn't need a server.  Very nice!

## Credentials

This application require Salsa Classic campaign manager credentials.  These are retrieved from a YAML
file that you specify when you run the program.  Here's a sample of a YAML configuraiotn file.

```text
host: salsa4.salsalabs.com
email: your_email@whatever.zzz
password: your-salsa-password
```

The `email` and `password` are the ones that you use to log into Salsa Classic.  You can find the `host`
by [clicking here](https://help.salsalabs.com/hc/en-us/articles/115000341773-Salsa-Application-Program-Interface-API-#api_host).

It's a pretty good idea to create a directory named `logins` to use to store credentials.  That puts them out of the way of the source and makes it easy to use more than one set of credentials.  It's also a good idea to make sure that the file's name ends in ".yaml".  

# Execution

Execution starts in the `classic_api_python` directory.  Use this command to see the help text.

`python3 tags/app.py --help`

You should see something like this on the console.

```
usage: app.py [-h] [--login LOGINFILE] [--dir OUTPUTDIR] [--offset OFFSET]

Export supporters and their tags from Salsa Classic.

optional arguments:
  -h, --help         show this help message and exit
  --login LOGINFILE  YAML file with login credentials
  --dir OUTPUTDIR    Store export files here
  --offset OFFSET    Start extraction at this offset
  ```

# Process

The client that asked for this app needed to have a CSV file of supporters and the three tag groups described in the Backgroun section.  There are the steps to make that happen.

1. Remove the `data` directory.  This is important.

```rm -rf data```

1. Execute the app.

```python3 tags/app.py --login my_login.yaml```

1. You'll see a log file like this.

```
2018-12-04 15:54:22,185: SupporterTagReader_01 INFO     reading 500 from       0
2018-12-04 15:54:22,526: SupporterTagReader_01 INFO     reading 500 from     500
2018-12-04 15:54:22,779: SupporterTagReader_01 INFO     reading 500 from    1000
2018-12-04 15:54:23,032: SupporterTagReader_01 INFO     reading 500 from    1500
2018-12-04 15:54:23,286: SupporterTagReader_01 INFO     reading 500 from    2000
2018-12-04 15:54:23,558: SupporterTagReader_01 INFO     reading 500 from    2500
2018-12-04 15:54:23,817: SupporterTagReader_01 INFO     reading 500 from    3000
2018-12-04 15:54:24,033: SupporterTagReader_01 INFO     reading 500 from    3500
2018-12-04 15:54:24,234: SupporterTagReader_01 INFO     reading 500 from    4000
2018-12-04 15:54:24,451: SupporterTagReader_01 INFO     reading 500 from    4500
2018-12-04 15:54:24,701: SupporterTagReader_01 INFO     reading 500 from    5000
2018-12-04 15:54:24,952: SupporterTagReader_01 INFO     reading 500 from    5500
2018-12-04 15:54:25,204: SupporterTagReader_01 INFO     reading 500 from    6000
2018-12-04 15:54:25,425: SupporterTagReader_01 INFO     reading 500 from    6500
2018-12-04 15:54:25,666: SupporterTagReader_01 INFO     reading 500 from    7000
2018-12-04 15:54:25,990: SupporterTagReader_01 INFO     reading 500 from    7500
2018-12-04 15:54:26,267: SupporterTagReader_01 INFO     reading 500 from    8000
2018-12-04 15:54:26,516: SupporterTagReader_01 INFO     reading 500 from    8500
2018-12-04 15:54:26,884: SupporterTagReader_01 INFO     reading 500 from    9000
2018-12-04 15:54:27,156: SupporterTagReader_01 INFO     reading 500 from    9500
    .
    .
    .
2018-12-04 15:57:09,242: SupporterTagReader_01 INFO     Ending  SupporterTagReader
```

1. Manuallly terminate the app when the logging stops.  Type control-C repeatedly until you see the shell prompt. (TODO: Make the danged app stop itself.)

1. Go to the data directory.

```cd data```

1. Merge all of the supporter_tag files sinto a single file named `data.csv`.  This, too, is important.

```cat supportertag*.csv > data.csv```

1. Use SQLIte3 to create the result CSV file.  

```sqlite3 --init ../tags/ncwarn_export_prep.sql```

1. You'll see something like this.

```
-- Loading resources from ../tags/ncwarn_export_prep.sql
58867
10500
SQLite version 3.19.3 2017-06-27 16:48:08
Enter ".help" for usage hints.
sqlite>
```

1. Type `.exit` and tap the enter key.  This is important.

1. The result file will appear in the data directory as `results.csv`.  Here's a sample of the contents.

```
supporter_KEY,Email,ActionTags,EventTags,OtherTags
60261272,,,,"Event:30th Anniversary Celebration"
60261278,,"action:EMA resolution signon",,
60261311,lunaace@gmail.com,,,"Event:2018-11-17 CP25 conference,Event:25th Anniv. RSVP yes"
60261322,bongobongo@kmail.com,"action:Petition Signer,action:Petition Signer-CARH,action:Rate Hikes 2013",,"Event:2012-12 Annual Meeting,Event:25th Anniv. RSVP no"
60261354,,,,"Event:25th Anniv. RSVP yes"
60261357,congocongo@kmail.com,,,
60261367,dongodongo@kisax.com,"action:Cooper ECR Complaint",,"Event:Climate Roadshow - Pittsboro 11-12-15"
60261373,eongoengo@lotmail.com,,,"Event:2013-02-11 Raleigh IRP hearing"
60261375,fongofongo@chicago.brt.com,,,
```

#Questions?  Comments?
Submit questions and comments via the [Issues](https://github.com/salsalabs/classic_api_python/issues) link at the top of the `classic_api_python` repository.  Don't waste your time contacting Salsalabs Support.  They get surly during the mating season.
