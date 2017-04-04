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
  purchasecount_link = db.StringListProperty()
  revenuepersong_link = db.StringListProperty()
  artistsongcount_link = db.StringListProperty()
  revenueperartist_link = db.StringListProperty()
  purchasecountrock_link = db.StringListProperty()
  revenuepersongrock_link = db.StringListProperty()
  artistsongcountrock_link = db.StringListProperty()
  revenueperartistrock_link = db.StringListProperty()

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

    if self.request.get("purchase_count"):
      pipeline = PurchaseCountPipeline(filekey, blob_key)
    elif self.request.get("revenue_per_song"):
      pipeline = RevenuePerSongPipeline(filekey, blob_key)
    elif self.request.get("artist_song_count"):
      pipeline = ArtistSongCountPipeline(filekey, blob_key)
    elif self.request.get("revenue_per_artist"):
      pipeline = RevenuePerArtistPipeline(filekey, blob_key)
    elif self.request.get("purchase_count_rock"):
      pipeline = PurchaseCountRockPipeline(filekey, blob_key)
    elif self.request.get("revenue_per_song_rock"):
      pipeline = RevenuePerSongRockPipeline(filekey, blob_key)
    elif self.request.get("artist_song_count_rock"):
      pipeline = ArtistSongCountRockPipeline(filekey, blob_key)
    elif self.request.get("revenue_per_artist_rock"):
      pipeline = RevenuePerArtistRockPipeline(filekey, blob_key)

    pipeline.start()
    self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)


def split_into_purchases(s):
  """Split the text into a list of purchases"""
  return re.split(r"\r|\n",s)

def split_into_attributes(s):
  """Split each purchase into a list of its attributes"""
  return re.split(r"\t",s)

def purchase_count_map(data):
  """Purchase Count Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      song = a[2].strip() + ", " + a[3].strip() + ", " + a[4].strip()
      yield (song, "")
    elif len(a) == 7:
      song = a[2].strip() + ", " + a[3].strip()
      yield (song, "")


def purchase_count_reduce(key, values):
  """Purchase count reduce function"""
  yield "%s; %d\n" % (key, len(values))

def revenue_per_song_map(data):
  """Revenue Per Song Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      song = a[2] + ", " + a[3] + ", " + a[4]
      yield (song, a[len(a)-1])
    elif len(a) == 7:
      song = a[2] + ", " + a[3]
      yield (song, a[len(a)-1])


def revenue_per_song_reduce(key, values):
  """Revenue Per Song reduce function"""
  total = 0.0
  for v in values:
    dec = float(v)
    total += dec
  yield "%s; %.2f\n" % (key, total)

def artist_song_count_map(data):
  """Artist Song Count Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      artist =a[3]
      yield (artist, "")
    elif len(a) == 7:
      artist = a[3]
      yield (artist, "")


def artist_song_count_reduce(key, values):
  """Artist Song Count reduce function"""
  yield "%s; %d\n" % (key, len(values))

def revenue_per_artist_map(data):
  """Revenue Per Artist Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      artist = a[3]
      yield (artist, a[len(a)-1])
    elif len(a) == 7:
      artist = a[3]
      yield (artist, a[len(a)-1])


def revenue_per_artist_reduce(key, values):
  """Revenue Per Artist reduce function"""
  total = 0.0
  for v in values:
    dec = float(v)
    total += dec
  yield "%s; %.2f\n" % (key, total)

def purchase_count_rock_map(data):
  """Purchase Count Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      if a[5] == "Rock":
        song = a[2] + ", " + a[3] + ", " + a[4]
        yield (song, "")
    elif len(a) == 7:
      if a[4] == "Rock":
        song = a[2] + ", " + a[3]
        yield (song, "")


def purchase_count_rock_reduce(key, values):
  """Purchase count reduce function"""
  yield "%s; %d\n" % (key, len(values))

def revenue_per_song_rock_map(data):
  """Revenue Per Song Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      if a[5] == "Rock":
        song = a[2] + ", " + a[3] + ", " + a[4]
        yield (song, a[len(a)-1])
    elif len(a) == 7:
      if a[4] == "Rock":
        song = a[2] + ", " + a[3]
        yield (song, a[len(a)-1])


def revenue_per_song_rock_reduce(key, values):
  """Revenue Per Song reduce function"""
  total = 0.0
  for v in values:
    dec = float(v)
    total += dec
  yield "%s; %.2f\n" % (key, total)

def artist_song_count_rock_map(data):
  """Artist Song Count Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      if a[5] == "Rock":
        artist =a[3]
        yield (artist, "")
    elif len(a) == 7:
      if a[4] == "Rock":
        artist = a[3]
        yield (artist, "")


def artist_song_count_rock_reduce(key, values):
  """Artist Song Count reduce function"""
  yield "%s; %d\n" % (key, len(values))

def revenue_per_artist_rock_map(data):
  """Revenue Per Artist Map Functions"""
  (entry, text_fn) = data
  text = text_fn()

  logging.debug("Received %s", entry.filename)
  for s in split_into_purchases(text):
    a = split_into_attributes(s)
    if len(a) == 8:
      if a[5] == "Rock":
        artist = a[3]
        yield (artist, a[len(a)-1])
    elif len(a) == 7:
      if a[4] == "Rock":
        artist = a[3]
        yield (artist, a[len(a)-1])


def revenue_per_artist_rock_reduce(key, values):
  """Revenue Per Artist reduce function"""
  total = 0.0
  for v in values:
    dec = float(v)
    total += dec
  yield "%s; %.2f\n" % (key, total)

class PurchaseCountPipeline(base_handler.PipelineBase):
  """A pipeline to run Purchase count of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "purchase_count",
        "main.purchase_count_map",
        "main.purchase_count_reduce",
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
        shards=2)
    yield StoreOutput("PurchaseCount", filekey, output)

class RevenuePerSongPipeline(base_handler.PipelineBase):
  """A pipeline to run Revenue Per artist of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "revenue_per_song",
        "main.revenue_per_song_map",
        "main.revenue_per_song_reduce",
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
        shards=2)
    yield StoreOutput("RevenuePerSong", filekey, output)

class ArtistSongCountPipeline(base_handler.PipelineBase):
  """A pipeline to run Artist Song Count of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "artist_song_count",
        "main.artist_song_count_map",
        "main.artist_song_count_reduce",
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
        shards=2)
    yield StoreOutput("ArtistSongCount", filekey, output)

class RevenuePerArtistPipeline(base_handler.PipelineBase):
  """A pipeline to run Revenue Per Song of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "revenue_per_artist",
        "main.revenue_per_artist_map",
        "main.revenue_per_artist_reduce",
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
        shards=2)
    yield StoreOutput("RevenuePerArtist", filekey, output)

class PurchaseCountRockPipeline(base_handler.PipelineBase):
  """A pipeline to run Purchase count of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "purchase_count_rock",
        "main.purchase_count_rock_map",
        "main.purchase_count_rock_reduce",
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
        shards=2)
    yield StoreOutput("PurchaseCountRock", filekey, output)

class RevenuePerSongRockPipeline(base_handler.PipelineBase):
  """A pipeline to run Revenue Per artist of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "revenue_per_song_rock",
        "main.revenue_per_song_rock_map",
        "main.revenue_per_song_rock_reduce",
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
        shards=2)
    yield StoreOutput("RevenuePerSongRock", filekey, output)

class ArtistSongCountRockPipeline(base_handler.PipelineBase):
  """A pipeline to run Artist Song Count of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "artist_song_count_rock",
        "main.artist_song_count_rock_map",
        "main.artist_song_count_rock_reduce",
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
        shards=2)
    yield StoreOutput("ArtistSongCountRock", filekey, output)

class RevenuePerArtistRockPipeline(base_handler.PipelineBase):
  """A pipeline to run Revenue Per Song of each unique song.

  Args:
    blobkey: blobkey to process as string. Should be a zip archive with
      text files inside.
  """

  def run(self, filekey, blobkey):
    logging.debug("filename is %s" % filekey)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    output = yield mapreduce_pipeline.MapreducePipeline(
        "revenue_per_artist_rock",
        "main.revenue_per_artist_rock_map",
        "main.revenue_per_artist_rock_reduce",
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
        shards=2)
    yield StoreOutput("RevenuePerArtistRock", filekey, output)

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



    if mr_type == "PurchaseCount":
      m.purchasecount_link = url_path
    elif mr_type == "RevenuePerSong":
      m.revenuepersong_link = url_path
    elif mr_type == "ArtistSongCount":
      m.artistsongcount_link = url_path
    elif mr_type == "RevenuePerArtist":
      m.revenueperartist_link = url_path
    elif mr_type == "PurchaseCountRock":
      m.purchasecountrock_link = url_path
    elif mr_type == "RevenuePerSongRock":
      m.revenuepersongrock_link = url_path
    elif mr_type == "ArtistSongCountRock":
      m.artistsongcountrock_link = url_path
    elif mr_type == "RevenuePerArtistRock":
      m.revenueperartistrock_link = url_path

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
