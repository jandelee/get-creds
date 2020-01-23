"""Helper functions and classes for Chargeback Financial Model (cfm).

Functions
---------
get_config_value(config_key, filename=None, suppress_key_not_found=False)
    Scan a configuration file for the provided key, and returns the value(s) of that key.
#is_billed_service(service_name)
#    Determine if the specified service is a billed service ().
licensed_service
    Indicate if the specified service is licensed (i.e., begins with one of the names in LICENSED_SERVICES).
vcap_service_present(service_name)
    Determine if the specified service is present in VCAP_SERVICES.
get_service_from_vcap_services(service_name)
    Retrieve the credentials for the specified service from VCAP_SERVICES.
days_in_month(month_string)
    Provide the number of days in a numeric month, e.g. returns 31 for 3 (March).
get_total_from_csv
    Return the sum of the values in the key_column field of the file specified by csv_filename.
build_dict_from_csv
    Build a dictionary using the data from the provided csv_filename.
generate_csv
    Generate a csv file using the supplied record_list and formatting according to the provided column_list.

Classes
-------
S3Initializer
    Parent class that sets up S3 bucket access for a bound S3 service or bucket defined in aws.cfg.
FileManager
    FileManager provides a way to manage files, and in the background the files are read/written locally or to S3.
S3Reader
    S3Reader behaves very much like a file object, except it reads the file from an S3 bucket (if defined).
CsvReader
    Provides an abstraction for reading csv files with column headers.
S3Writer
    S3Writer behaves very much like a file object, except when closed it writes the file to an S3 bucket.
CsvWriter
    CsvWriter provides an abstraction for writing csv files.
#TextReader
#    TextReader reads in text from a file.  Lines starting with # are skipped.  Leading/trailing blanks removed.

"""

import os
import json
import datetime
import shutil
from collections import defaultdict
import boto3
from botocore.exceptions import ClientError
import re


def file_exists(filename):
    return os.path.exists(filename) and os.path.isfile(filename)


def get_language(buildpack_name):
    """Determine the language used by the app based on the name of the buildpack."""
    # strip off trailing _n (as many times as it appears)
    language = re.sub("_\d+", "", buildpack_name)

    # strip off trailing -n (as many times as it appears)
    language = re.sub("-\d+", "", language)

    # if buildpack starts_with https://github.com/cloudfoundry/ :
    key = 'https://github.com/cloudfoundry/'
    if language.startswith(key):

        # strip off leading https://github.com/cloudfoundry/
        language = language[len(key):]
        #    Find -buildpack string in result; buildpack name is text up to -buildpack
        pos = re.search("-buildpack", language)
        return language[:pos.start()]

    # else if buildpack starts with https://github.com/heroku/heroku-buildpack-
    key = 'https://github.com/heroku/heroku-buildpack-'
    if language.startswith(key):

        # strip off leading https://github.com/heroku/heroku-buildpack-
        language = language[len(key):]
        #    Find .git string in result; buildpack name is text up to -buildpack
        pos = re.search(".git", language)
        return language[:pos.start()]

    # else if buildpack starts with https://gitlab.gs.mil/ :
    #    buildpack name is "CUSTOM"
    if language.startswith('https://gitlab.gs.mil/'):
        return 'Unknown'

    # else if buildpack ends with buildpack
    key = '_buildpack'
    if language.endswith(key):

        # remove trailing buildpack_offline; result is language
        return language[:-len(key)]

    # else if buildpack ends with buildpack
    key = '-buildpack'
    if language.endswith(key):

        # remove trailing buildpack_offline; result is language
        return language[:-len(key)]

    # else if buildpack ends with buildpack_offline
    key = '_buildpack_offline'
    if language.endswith(key):

        # remove trailing buildpack_offline; result is language
        return language[:-len(key)]

    # else
    #    unrecognized buildpack name (or 'None')
    return language


def get_useful_lines(filename):
    """Return useful lines from a file.

    Useful lines are those that are not blank and not comments.  Trailing whitespace and newlines are removed.
    """
    if not os.path.exists(filename) or not os.path.isfile(filename):
        print('Could not find file:', filename)
        exit(1)
        # This test and exit was added to ensure that if the file is not present, it gets reported properly,
        # especially if the script was executed as a job in pcf scheduler - we want to status to show as failed, not success.

    with open(filename, 'r') as input_file:
        for line in input_file.readlines():
            if not line.startswith('#') and len(line) > 0:
                yield line.rstrip()


def get_config_value(config_key, filename=None, suppress_key_not_found=False):
    """Scan a configuration file for the provided key, and returns the value(s) of that key.

    If the key is not found, an error message is printed and execution terminates, unless suppress_key_not_found is truthy.
    Uses a format very similar to yaml.  If no filename is specified, uses the filename PlatformChargeback.cfg.
    """
    if filename is None:
        filename = "PlatformChargeback.cfg"

    result = ""
    results = {}
    for line in get_useful_lines(filename):
        # print(line)
        # If this line contains the ':' character, it contains at least a key, and possibly a value
        if ':' in line and not line.startswith(' '):
            # If there is a previous result
            if len(result) > 0:
                results[key] = result
                result = ""
            key, value = line.strip().split(':', 1)
            value = value.strip()
            # If the value of the key was specified on this line
            if len(value) > 0:
                result = value
            else:
                result = []
        # Else this line only contains a value
        else:
            result.append(line.strip())
    # If there is a previous result
    if len(result) > 0:
        results[key] = result
        result = ""
    if config_key in results:
        return results[config_key]
    else:
        if suppress_key_not_found:
            return 0
        else:
            print('Did not find key ' + config_key + ' in ' + filename)
            exit()


def licensed_service(service_name):
    """Indicate if the specified service is licensed (i.e., begins with one of the names in LICENSED_SERVICES)."""
    licensed = False
    for licensed_service in get_config_value('LICENSED_SERVICES'):
        if service_name.startswith(licensed_service):
            licensed = True
    return licensed


def vcap_service_present(service_name):
    """Determine if the specified service is present in VCAP_SERVICES."""
    if os.environ.get('VCAP_SERVICES') is not None:
        env_vars = os.environ['VCAP_SERVICES']
        env_vars_json = json.loads(env_vars)
        if service_name in env_vars_json:
            return True
    return False


def get_service_from_vcap_services(service_name):
    """Retrieve the attributes for the specified service from VCAP_SERVICES."""
    env_vars = os.environ['VCAP_SERVICES']
    env_vars_json = json.loads(env_vars)
    if service_name not in env_vars_json:
        print('No', service_name, 'service bound to app...exiting')
        exit()
    return env_vars_json[service_name][0]


def days_in_month(month_string):
    """Provide the number of days in a numeric month, e.g. returns 31 for 3 (March)."""
    month = int(month_string)
    if month == 4 or month == 6 or month == 9 or month == 11:
        return 30
    elif month == 2:
        return 28
    elif month < 1 or month > 12:
        print('Invalid month of', month_string, 'specified in datestring')
        exit()
    else:
        return 31


def get_total_from_csv(csv_filename, key_column):
    """Return the sum of the values in the key_column field of the file specified by csv_filename.

    key_column can either be a single column, or x*y in order to generate a sum of the product of the values in the x and y columns
    """
    total = 0.0
    with CsvReader(csv_filename) as csv_reader:
        if not csv_reader.column_present(key_column):
            print('Did not find column', key_column, 'in file', csv_filename)
            exit()
        for line in csv_reader.readlines():
            if '*' in key_column:
                words = key_column.split('*')
                total += float(csv_reader.column(words[0])) * float(csv_reader.column(words[1]))
            else:
                total += float(csv_reader.column(key_column))
    return total


def build_dict_from_csv(csv_filename, key_columns, value_columns):
    """Build a dictionary using the data from the provided csv_filename.

    key_columns indicates which column(s) in the csv should be used as the dictionary key.
    value_columns indicates which column(s) in the csv should provide the value(s) for the dictionary.
    """
    result = {}
    with CsvReader(csv_filename) as csv_reader:
        for line in csv_reader.readlines():

            # build the key
            key = csv_reader.build_key(key_columns)
            value = csv_reader.build_key(value_columns)
            result[key] = value

    return result


def billing_datestring(month_offset=None):
    """Determine a default datestring for the billing file.

    month_offset is the number of months to go back in time, and defaults to 1 month
    The billing datestring is based on the previous month, and is in yyyymm format.
    """
    if month_offset is None:
        month_offset = 1
    today = datetime.date.today()
    year, month = today.year, today.month
    month = month - month_offset
    if month < 1:
        month = month + 12
        year = year - 1
    return "%d%02d" % (year, month)


def find_line(search_line, lines):
    """Find a line in a list of lines.

    search_line contains the text to be searched for.
    lines contains the list of lines to be searched.
    """
    for line in lines:
        if search_line in line:
            return line
    return ""


def get_data_from_file(filename, config_key):
    """Extract values from a file, as specified by the instructions in the config_key.

    filename is the name of the text file to read from
    config_key is a key in PlatformChargeback.cfg that defines how to extract the data

    Each line defined in the config_key specifies how to extract one data value;
    The format is x,y where x is the text that will appear in the line of the file
    that has the desired data value, and y identifies wich word in that file line
    contains the desired data value.  y can be either a number (1-based)
    or the text "last" to indicate that the last word contains the desired data value.
    """
    instructions = get_config_value(config_key)

    # open the file and store in memory, since we'll need to process it once for each instruction
    with S3Reader(filename, s3_only=True, suppress_file_not_found=True) as file:
        if file is None:
            return None
        lines = file.readlines()

    results = []
    for instruction in instructions:
        (search_text, word) = instruction.split(',')
        desired_line = find_line(search_text, lines)
        if desired_line == "":
            print('Could not find line containing', search_text, 'in file', filename)
            exit(1)
        words = desired_line.split(' ')
        if word == 'last':
            results.append(words[-1])
        else:
            word_number = int(word)
            if word_number < 1 or word_number > len(words):
                print('Could not find word #', word, 'in line', desired_line, 'of file', filename)
                exit(1)
            results.append(words[word_number-1])
    return results


def build_idp_data():
    """Build the idp data from a bound SSO service."""
    sso_service = get_service_from_vcap_services('p-identity')
    credentials = sso_service['credentials']
    idp = dict()
    uri = credentials['auth_domain']
    idp['client'] = credentials['client_id']
    idp['secret'] = credentials['client_secret']
    idp['authn_uri'] = uri + '/oauth/authorize'
    idp['token_uri'] = uri + '/oauth/token'
    idp['token_info_uri'] = uri + '/userinfo'
    envs = dict(os.environ)
    apps = json.loads(envs['VCAP_APPLICATION'])
    app_uri = apps['uris'][0]
    idp['return_uri'] = 'https://' + app_uri + '/oauthcallback'
    return idp


def generate_csv(csv_filename, record_list, column_list, generate_totals=None):
    """Generate a csv file using the supplied record_list and formatting according to the provided column_list."""
    with S3Writer(csv_filename) as file:

        totals = defaultdict(float)  # dictionary of totals of float fields

        # write out the header
        for column in column_list:
            (column_name, column_heading, column_type) = column
            print(column_heading, end='', file=file)

            if column != column_list[-1]:  # if not last column
                print(',', end='', file=file)
            else:  # last column
                print('', file=file)

        for record in record_list:
            for column in column_list:
                (column_name, column_heading, column_type) = column

                if column_type == 'float':
                    print("%.2f" % float(record[column_name]), end='', file=file)
                    totals[column_name] += float(record[column_name])
                else:
                    print("%s" % record[column_name], end='', file=file)

                if column != column_list[-1]:  # if not last column
                    print(',', end='', file=file)
                else:  # last column
                    print('', file=file)

        if generate_totals:
            for (column_name, column_heading, column_type) in column_list:
                if column_type == 'float':
                    print("%.2f" % float(totals[column_name]), end='', file=file)
                elif column_name == column_list[0][0]:  # if the first column
                    print('Total', end='', file=file)

                if column_name != column_list[-1][0]:  # if not last column
                    print(',', end='', file=file)
                else:  # last column
                    print('', file=file)


class TableBuilder:
    """Class for building a table that can be populated in an html page."""

    def __init__(self):
        """Initialize the class."""
        self.data = []

    def add(self, column, cell=None):
        """Add a column to the table.  column is a list of values to be added."""
        if cell is None:
            column_to_add = column
        else:
            column_to_add = list(column)
            column_to_add.insert(0, cell)

        if self.data == []:
            for item in column_to_add:
                self.data.append([item])
        else:
            for index, item in enumerate(column_to_add):
                self.data[index].append(item)

    def data(self):
        """Return the table."""
        return self.data


class S3Initializer:
    """Parent class that sets up S3 bucket access for a bound S3 service or bucket defined in aws.cfg."""

    def __init__(self, s3_only):
        """Initialize the class."""
        self.s3_only = s3_only
        if os.environ.get('VCAP_SERVICES') is not None:  # not running locally
            s3_service = get_service_from_vcap_services('aws-s3')
            credentials = s3_service['credentials']
            self.access_key_id = credentials['access_key_id']
            self.bucket = credentials['bucket']
            self.secret_access_key = credentials['secret_access_key']
            if 'kms_key_arn' in credentials:
                kms_key_arn = credentials['kms_key_arn']
                words = kms_key_arn.split('/')
                self.kms_key_id = words[1]
            else:
                self.kms_key_id = ''
            # print('Caching credentials from VCAP_SERVICES for S3 bucket', self.bucket)
        elif os.path.isfile('aws.cfg'):
            self.access_key_id = get_config_value('access_key_id', 'aws.cfg')
            self.secret_access_key = get_config_value('secret_access_key', 'aws.cfg')
            self.bucket = get_config_value('bucket', 'aws.cfg')
            self.kms_key_id = get_config_value('bucket', 'aws.cfg', suppress_key_not_found=True)
            # print('Caching credentials from aws.cfg for S3 bucket', self.bucket)
        elif s3_only:
            print('Expected to find VCAP_SERVICES or aws.cfg, but found neither')
            exit(1)

    def get_s3_client(self):
        """Return a boto3 s3 client."""
        return boto3.client('s3',
                            aws_access_key_id=self.access_key_id,
                            aws_secret_access_key=self.secret_access_key)


class FileManager(S3Initializer):
    """FileManager provides a way to manage files, and in the background the files are read/written locally or to S3."""

    def __init__(self, s3_only=None):
        """Initialize the class."""
        super().__init__(s3_only)

    def files(self, prefix=None):
        """Return a list of the files in the current directory, or in the current S3 bucket (generator)."""
        if hasattr(self, 'bucket'):  # If we have access to an S3 bucket
            s3_client = self.get_s3_client()
            if prefix:
                kwargs = {'Bucket': self.bucket, 'Prefix': prefix}
            else:
                kwargs = {'Bucket': self.bucket}

            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(**kwargs):

                if 'Contents' not in page:  # if no files in the bucket
                    return
                # print('Generating stream of', len(page['Contents']), 'files for bucket', self.bucket, 'in the current page of results')
                for s3_object in page['Contents']:
                    yield s3_object['Key']

        else:  # No S3 bucket, so provide a list of files in the current directory
            files = os.listdir('.')
            for file in files:
                yield file

    def files_present(self, prefix=None):
        """Indicate if any files are present in the S3 bucket.  The prefix is applied if supplied."""
        if hasattr(self, 'bucket'):  # If we have access to an S3 bucket
            s3_client = self.get_s3_client()
            try:
                if prefix:
                    response = s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                else:
                    response = s3_client.list_objects_v2(Bucket=self.bucket)
            except ClientError as e:
                print('**ERROR** - ClientError:', e.response)
                exit()

            if 'Contents' in response:
                return len(response['Contents'])
            else:
                return 0

    def delete(self, filename):
        """Delete the file with name 'filename'."""
        s3_client = self.get_s3_client()
        s3_client.delete_object(Bucket=self.bucket, Key=filename)


class S3Reader(S3Initializer):
    """S3Reader behaves very much like a file object, except it reads the file from an S3 bucket (if defined)."""

    def __init__(self, filename, s3_only=None, suppress_file_not_found=None):
        """Initialize the class by saving the filename."""
        self.filename = filename
        self.suppress_file_not_found = suppress_file_not_found
        super().__init__(s3_only)
        if not hasattr(self, 'bucket'):  # running locally
            print('VCAP_SERVICES not defined, aws.cfg not present...reading file', self.filename, 'locally.')

    def __enter__(self):
        """Open a connection to S3, download the file locally, and open the file."""
        if hasattr(self, 'bucket'):  # Make sure the bucket atttribute is present
            # make sure the requested file is not already present
            if not os.path.exists(self.filename) or not os.path.isfile(self.filename) or self.s3_only:
                print('Reading file', self.filename, 'from S3 bucket', self.bucket)
                s3_client = self.get_s3_client()

                try:
                    s3_client.head_object(Bucket=self.bucket, Key=self.filename)
                except ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        print('Did not find file', self.filename, 'locally or in S3 bucket', self.bucket)
                        if self.suppress_file_not_found:
                            return None
                        else:
                            exit(1)
                    else:
                        raise

                s3_client.download_file(self.bucket, self.filename, self.filename)
        self.file = open(self.filename, 'r')
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        """When the context manager is closed, close the file."""
        if hasattr(self, 'file'):
            if self.file:
                self.file.close()


class CsvReader(S3Initializer):
    """Provides an abstraction for reading csv files with column headers."""

    def __init__(self, csv_filename):
        """Call the parent initializer, and if we are using S3, open a connection and download the file."""
        super().__init__(s3_only=None)
        self.filename = csv_filename
        if hasattr(self, 'bucket'):  # Make sure the bucket atttribute is present
            if not os.path.exists(self.filename) or not os.path.isfile(self.filename):
                print('Copying file', self.filename, 'from S3 bucket', self.bucket)
                s3_client = self.get_s3_client()
                s3_client.download_file(self.bucket, self.filename, self.filename)

    def __enter__(self):
        """Open the file for reading; read and process the header line."""
        self.file = open(self.filename, 'r')
        self.header = self.file.readline().strip()
        self.column_names = self.header.split(',')
        self.column_index = {}
        for num, column_name in enumerate(self.header.split(',')):
            self.column_index[column_name] = num
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the file."""
        if self.file:
            self.file.close()

    def headers(self):
        """Return a list of the column names."""
        return self.column_names

    def readlines(self):
        """Return a stream of the lines in the csv file (generator)."""
        for line in self.file.readlines():
            current_line = line.strip()
            self.current_line = current_line
            self.words = current_line.split(',')
            yield current_line

    def columns(self):
        """Returns the columns for the current line."""
        return self.words

    def column(self, column_name):
        """Return the data field in the current line of the csv file that matches column_name."""
        if column_name not in self.column_index:
            print('Did not find column name', column_name, 'in csv file', self.filename)
        return self.words[self.column_index[column_name]]

    def column_by_number(self, column_number):
        """Return the data field in the current line of the csv file that matches column_number."""
        if int(column_number) > len(self.words):
            print('Did not find column number', column_number, 'in csv file', self.filename)
            exit()
        return self.words[int(column_number)-1]

    def column_present(self, column_name):
        """Indicate if column_name is present in the csv file."""
        return column_name in self.column_index

    def build_key(self, columns, separator=None):
        """Build a key based on the values of columns defined in columns, using the current record."""
        # If no separator defined, default to a comma
        if not separator:
            separator = ','
        # build the key
        if type(columns) is list:
            key = ''
            for column in columns:
                if key == '':
                    key = self.column(column)
                else:
                    key = key + separator + self.column(column)
        else:
            key = self.column(columns)
        return key


class S3Writer(S3Initializer):
    """S3Writer behaves very much like a file object, except when closed it writes the file to an S3 bucket."""

    def __init__(self, filename, s3_only=None):
        """Initialize the class by saving the filename."""
        self.filename = filename
        super().__init__(s3_only)
        if not hasattr(self, 'bucket'):  # running locally
            print('VCAP_SERVICES not defined, aws.cfg not present...will write', self.filename, 'locally.')

    def __enter__(self):
        """Open the file for writing."""
        if os.path.exists(self.filename):
            print('Copying', self.filename, 'to', self.filename + '.sav')
            shutil.copy2(self.filename, self.filename + '.sav')
        self.file = open(self.filename, 'w')
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the file, and write to S3 if a bucket is defined."""
        if self.file:
            self.file.close()
            if hasattr(self, 'bucket'):  # Make sure the bucket atttribute is present
                print('Writing file', self.filename, 'to S3 bucket', self.bucket)
                if hasattr(self, 'kms_key_arn'):
                    extraArgs = {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyID': 'self.kms_key_arn'}
                else:
                    extraArgs = None
                s3_client = self.get_s3_client()
                s3_client.upload_file(self.filename, self.bucket, self.filename, ExtraArgs=extraArgs)


class CsvWriter(S3Initializer):
    """CsvWriter provides an abstraction for writing csv files."""

    def __init__(self, filename, header=None):
        """Initialize the class."""
        self.filename = filename
        self.current_line = ''
        self.lines = []
        self.header = header
        super().__init__(s3_only=None)

    def __enter__(self):
        """Open the file for writing."""
        self.file = open(self.filename, 'w')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Write the file, and upload to S3 if bucket is defined."""
        if self.file:
            if self.header:
                print(self.header, file=self.file)
            for line in self.lines:
                print(line, file=self.file)
            self.file.close()
            if hasattr(self, 'bucket'):  # Make sure the bucket atttribute is present
                print('Writing file', self.filename, 'to S3 bucket', self.bucket)
                s3_client = self.get_s3_client()
                s3_client.upload_file(self.filename, self.bucket, self.filename)

    def add_value(self, value):
        """Add a single value to the current line, prepending a comma if this is not the first value."""
        # if the current line is empty
        if self.current_line == '':
            # put the value in the current line
            self.current_line = value
        else:  # current line is not empty
            # append a comma to the current line, followed by the value
            self.current_line = self.current_line + ',' + value

    def add_values(self, value_list):
        """Add a list of values to the current line."""
        if len(value_list) > 0:
            for value in value_list:
                self.add_value(str(value))

    def new_line(self):
        """Add a new line to the csv file."""
        self.lines.append(self.current_line)
        self.current_line = ''
