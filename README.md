## Salsa Classic API in Python

This is a repository of samples getting and saving data from Salsa Classic using
Python3 to manipulate the Classic API.  These are samples and are not guaranteed
to be excellent Python.

## Installation

There are two ways to install.

The first is to use git to clone the repository.  Generally, developers find this to
be a helpful activity since their versions of this software can be updated when this
library changes.

``` git clone https://github.com/salsalabs/classic_api_python```

The second is to download a zip of the repository.  There's a very good article [here](https://stackoverflow.com/questions/6466945/fastest-way-to-download-a-github-project/19528227) that
demonstrates how to download a ZIP of any archive.

Once the zip is downloaded, unzip it to see the contents.

## Configuration

The sample apps were written to involve as few external libraries as necessary.  At this
writing, the only libraries that the examples needs are `requests` to read from the web and
`pyyaml` for reading login information .

We'll presume that you are using Python 2.7.x.  Open a console and type

```pip```

If the command does not exist, then use the steps [on this page](https://gist.github.com/haircut/14705555d58432a5f01f9188006a04ed)
to install it.  Hint:  Use `--user` as the author recommends.

Now to install `requests`.
```
pip install requests --user
pip install pyyaml --user
```
A lot of text will go by and the library will be installed.

## Credentials

These applications require Salsa Classic campaign manager credentials.  The apps read from a YAML file.
Here's a sample YAML credentials file.

```yaml
host: salsa4.salsalabs.com
email: your_email@whatever.zzz
password: your-salsa-password
```

The `email` and `password` are the ones that you use to log into Salsa Classic.  You can find the `host`
by [clicking here](https://help.salsalabs.com/hc/en-us/articles/115000341773-Salsa-Application-Program-Interface-API-#api_host).

It's a pretty good idea to create a directory named `logins` to use to store credentials.  That puts them out of the way of the source and makes it easy to use more than one set of credentials.  It's also a good idea to make sure that the file's name ends in ".yaml".  

## Execution

The samples generally use a form like this.

```bash
python app.py --login YAML_FILE
```

where `YAML_FILE` is the file that contains your login credentials.

For example, let's say that your login information is in `logins/my_login.yaml` and you want to run the sample app that reads supporters.  Here's a command that you can use:

```bash
python read_supporters.py --login logins/my_login.yaml
```
And here's an example of the output.

```
('Authentication: ', {u'status': u'success', u'message': u'Successful Login', u'jsessionid': u'F735247AF8E35073AF6FDE1FC23D1AE2-n2'})
58945835  Bob        Mintlet     Bmint99@mintyfresh.bizi
58825855  Bob        Trelbeck   Bob@trelbeck.bizi
```

## Questions?  Comments?

Please use the `Issues` tab at the top of this page.
