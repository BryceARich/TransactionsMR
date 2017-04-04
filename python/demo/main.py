#!/usr/bin/env python
#
# Copyright 2011 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" This is a sample application that tests the MapReduce API.

It does so by allowing users to upload a zip file containing plaintext files
and perform some kind of analysis upon it. Currently three types of MapReduce
jobs can be run over user-supplied input data: a WordCount MR that reports the
number of occurrences of each word, an Index MR that reports which file(s) each
word in the input corpus comes from, and a Phrase MR that finds statistically
improbably phrases for a given input file (this requires many input files in the
zip file to attain higher accuracies)."""

__author__ = """aizatsky@google.com (Mike Aizatsky), cbunch@google.com (Chris
Bunch)"""

# Using opensource naming conventions, pylint: disable=g-bad-name

import datetime
import jinja2
import logging
import re
import urllib
import webapp2
import itertools

from google.appengine.ext import blobstore
from google.appengine.ext import db

from google.appengine.ext.webapp import blobstore_handlers

from google.appengine.api import app_identity
from google.appengine.api import taskqueue
from google.appengine.api import users

from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import shuffler


class FileMetadata(db.Model):
  """A helper class that will hold metadata for the user's blobs.

  Specifially, we want to keep track of who uploaded it, where they uploaded it
  from (right now they can only upload from their computer, but in the future
  urlfetch would be nice to add), and links to the results of their MR jobs. To
  enable our querying to scan over our input data, we store keys in the form
  'user/date/blob_key', where 'user' is the given user's e-mail address, 'date'
  is the date and time that they uploaded the item on, and 'blob_key'
  indicates the location in the Blobstore that the item can be found at. '/'
  is not the actual separator between these values - we use '..' since it is
  an illegal set of characters for an e-mail address to contain.
  """

  __SEP = ".."
  __NEXT = "./"

  owner = db.UserProperty()
  filename = db.StringProperty()
  uploadedOn = db.DateTimeProperty()
  source = db.StringProperty()
  blobkey = db.StringProperty()
  songpairs_link = db.StringListProperty()

  @staticmethod
  def getFirstKeyForUser(username):
    """Helper function that returns the first possible key a user could own.

    This is useful for table scanning, in conjunction with getLastKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the earliest possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__SEP)

  @staticmethod
  def getLastKeyForUser(username):
    """Helper function that returns the last possible key a user could own.

    This is useful for table scanning, in conjunction with getFirstKeyForUser.

    Args:
      username: The given user's e-mail address.
    Returns:
      The internal key representing the last possible key that a user could
      own (although the value of this key is not able to be used for actual
      user data).
    """

    return db.Key.from_path("FileMetadata", username + FileMetadata.__NEXT)

  @staticmethod
  def getKeyName(username, date, blob_key):
    """Returns the internal key for a particular item in the database.

    Our items are stored with keys of the form 'user/date/blob_key' ('/' is
    not the real separator, but __SEP is).

    Args:
      username: The given user's e-mail address.
      date: A datetime object representing the date and time that an input
        file was uploaded to this app.
      blob_key: The blob key corresponding to the location of the input file
        in the Blobstore.
    Returns:
      The internal key for the item specified by (username, date, blob_key).
    """

    sep = FileMetadata.__SEP
    return str(username + sep + str(date) + sep + blob_key)


class IndexHandler(webapp2.RequestHandler):
  """The main page that users will interact with, which presents users with
  the ability to upload new data or run MapReduce jobs on their existing data.
  """

  template_env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"),
                                    autoescape=True)

  def get(self):
    user = users.get_current_user()
    username = user.nickname()

    first = FileMetadata.getFirstKeyForUser(username)
    last = FileMetadata.getLastKeyForUser(username)

    q = FileMetadata.all()
    q.filter("__key__ >", first)
    q.filter("__key__ < ", last)
    results = q.fetch(10)

    items = [result for result in results]
    length = len(items)

    bucket_name = app_identity.get_default_gcs_bucket_name()
    upload_url = blobstore.create_upload_url("/upload",
                                             gs_bucket_name=bucket_name)

    self.response.out.write(self.template_env.get_template("index.html").render(
        {"username": username,
         "items": items,
         "length": length,
         "upload_url": upload_url}))

  def post(self):
    filekey = self.request.get("filekey")
    blob_key = self.request.get("blobkey")

    if self.request.get("song_pairs"):
      pipeline = SongPairsPipeline(filekey, blob_key)

    pipeline.start()
    self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

def split_into_purchases(s):
  """Split the text into a list of purchases"""
  return re.split(r"\r|\n",s)

def split_into_attributes(s):
  """Split each purchase into a list of its attributes"""
  return re.split(r"\t",s)

def split_into_songs(s):
  """Split the text into a list of purchases"""
  s = re.split(r"\t",s)
  for song in s:
    if '\n' in song:
      song = song.rstrip('\n')
  s.pop(0)
  for i in range(0,len(s)):
    s[i] = s[i].strip();
  s[-1] = s[-1].strip();
  return s

def transaction_sets_map(data):
  """Word count map function."""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Got %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      tran = a[0] + ", " + a[1];
      song = a[2] + ", " + a[3] + ", " + a[4]
      yield(tran, song)
    elif len(a) == 7:
      tran = a[0] + ", " + a[1];
      song = a[2] + ", " + a[3]
      yield(tran, song)

def transaction_sets_reduce(key, values):
  """Word count reduce function."""
  out = "%s" % (key)
  for val in values:
    if "\n" in val:
      val = val.strip('\n')
    out += "\t %s" % (val)
  yield "%s\n" % (out)

def song_pairs_map(data):
  """Word count map function."""
  text = data
  for purchase in data:
    for formatted_purchase in purchase.split("\n"):
      s = split_into_songs(purchase)
      if len(s) > 1:
        pair_of_songs = itertools.permutations(s, 2)
        for p in pair_of_songs:
          yield (p[0] + ";" + p[1], "")

def song_pairs_reduce(key, values):
  """Word count reduce function."""
  yield "%s; %d\n" % (key, len(values)/2)

def top_pair_map(data):
  """Word count map function."""
  text = data
  for purchase in text:
      s = purchase.split(";")
      if len(s) == 3:
        yield(s[0], s[2] + ";" + s[1])

def top_pair_reduce(key, values):
  """Word count reduce function."""
  max_song = None
  max_val = None
  for val in values:
    num, song2 = val.split(";")
    num = int(num)
    if max_song == None:
      max_song = song2
      max_val = num
    elif num > max_val:
      max_song = song2
      max_val = num
  yield "%s; %s; %d\n" % (key, max_song, max_val)

class GCSMapperParams(base_handler.PipelineBase): 
  def run(self, GCSPath): 
    bucket_name = app_identity.get_default_gcs_bucket_name()
    return { 
    "input_reader": { 
    "bucket_name": bucket_name, 
    "objects": [path.split('/', 2)[2] for path in GCSPath], 
    } 
    } 

class SongPairsPipeline(base_handler.PipelineBase):
  """A pipeline to run Word count demo.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    transactions = yield mapreduce_pipeline.MapreducePipeline(
        "transaction_sets",
        "main.transaction_sets_map",
        "main.transaction_sets_reduce",
        "mapreduce.input_readers.BlobstoreZipInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params={
            "blob_key": blobkey,
        },
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    pair_of_songs = yield mapreduce_pipeline.MapreducePipeline(
        "song_pairs",
        "main.song_pairs_map",
        "main.song_pairs_reduce",
        "mapreduce.input_readers.GoogleCloudStorageInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params=(
            yield GCSMapperParams(transactions)),
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    top_pair = yield mapreduce_pipeline.MapreducePipeline(
        "top_pair",
        "main.top_pair_map",
        "main.top_pair_reduce",
        "mapreduce.input_readers.GoogleCloudStorageInputReader",
        "mapreduce.output_writers.GoogleCloudStorageOutputWriter",
        mapper_params=(
            yield GCSMapperParams(pair_of_songs)),
        reducer_params={
            "output_writer": {
                "bucket_name": bucket_name,
                "content_type": "text/plain",
            }
        },
        shards=16)
    yield StoreOutput("SongPairs", filekey, top_pair)
    # yield StoreOutput("SongPairs", filekey, transactions)


class StoreOutput(base_handler.PipelineBase):
  """A pipeline to store the result of the MapReduce job in the database.

  Args:
    mr_type: the type of mapreduce job run (e.g., WordCount, Index)
    encoded_key: the DB key corresponding to the metadata of this job
    output: the gcs file path where the output of the job is stored
  """

  def run(self, mr_type, encoded_key, output):
    logging.debug("output is %s" % str(output))
    key = db.Key(encoded=encoded_key)
    m = FileMetadata.get(key)

    url_path = []

    for o in output:
      blobstore_filename = "/gs" + o
      blobstore_gs_key = blobstore.create_gs_key(blobstore_filename)
      curr_path = "/blobstore/" + blobstore_gs_key
      url_path += [curr_path]

    if mr_type == "SongPairs":
      m.songpairs_link = url_path

    m.put()

class UploadHandler(blobstore_handlers.BlobstoreUploadHandler):
  """Handler to upload data to blobstore."""

  def post(self):
    source = "uploaded by user"
    upload_files = self.get_uploads("file")
    blob_key = upload_files[0].key()
    name = self.request.get("name")

    user = users.get_current_user()

    username = user.nickname()
    date = datetime.datetime.now()
    str_blob_key = str(blob_key)
    key = FileMetadata.getKeyName(username, date, str_blob_key)

    m = FileMetadata(key_name = key)
    m.owner = user
    m.filename = name
    m.uploadedOn = date
    m.source = source
    m.blobkey = str_blob_key
    m.put()

    self.redirect("/")


class DownloadHandler(blobstore_handlers.BlobstoreDownloadHandler):
  """Handler to download blob by blobkey."""

  def get(self, key):
    key = str(urllib.unquote(key)).strip()
    logging.debug("key is %s" % key)
    self.send_blob(key)


app = webapp2.WSGIApplication(
    [
        ('/', IndexHandler),
        ('/upload', UploadHandler),
        (r'/blobstore/(.*)', DownloadHandler),
    ],
    debug=True)
