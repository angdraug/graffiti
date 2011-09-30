# Graffiti RDF Store
# (originally written for Samizdat project)
#
#   Copyright (c) 2002-2009  Dmitry Borodaenko <angdraug@debian.org>
#
#   This program is free software.
#   You can distribute/modify this program under the terms of
#   the GNU General Public License version 3 or later.
#
# see doc/rdf-storage.txt for introduction and Graffiti Squish definition;
# see doc/storage-impl.txt for explanation of implemented algorithms
#
# vim: et sw=2 sts=2 ts=8 tw=0

require 'syncache'
require 'graffiti/exceptions'
require 'graffiti/debug'
require 'graffiti/rdf_config'
require 'graffiti/squish'

module Graffiti

# API for the RDF storage access similar to DBI or Sequel
#
class Store

  # initialize class attributes
  #
  # _db_ is a Sequel database handle
  #
  # _config_ is a hash of configuraiton options for RdfConfig
  #
  def initialize(db, config)
    @db = db
    @config = RdfConfig.new(config)

    # cache parsed Squish SELECT queries
    @select_cache = SynCache::Cache.new(nil, 1000)
  end

  # storage configuration in an RdfConfig object
  #
  attr_reader :config

  # replace schema uri with a prefix from the configured namespaces
  #
  def ns_shrink(uriref)
    SquishQuery.ns_shrink(uriref, @config.ns)
  end

  # get value of subject's property
  #
  def get_property(subject, property)
    fetch(%{SELECT ?object WHERE (#{property} :subject ?object)},
          :subject => subject).get(:object)
  end

  def fetch(query, params={})
    @db.fetch(select(query), params)
  end

  # get one query answer (similar to DBI#select_one)
  #
  def select_one(query, params={})
    fetch(query, params).first
  end

  # get all query answers (similar to DBI#select_all)
  #
  def select_all(query, limit=nil, offset=nil, params={}, &p)
    ds = fetch(query, params).limit(limit, offset)
    if block_given?
      ds.all(&p)
    else
      ds.all
    end
  end

  # accepts String or pre-parsed SquishQuery object, caches SQL by String
  #
  def select(query)
    query.kind_of?(String) and
      query = @select_cache.fetch_or_add(query) { SquishSelect.new(@config, query) }
    query.kind_of?(SquishSelect) or raise ProgrammingError,
      "String or SquishSelect expected"
    query.to_sql
  end

  # merge Squish query into RDF database
  #
  # returns list of new ids assigned to blank nodes listed in INSERT section
  #
  def assert(query, params={})
    @db.transaction do
      SquishAssert.new(@config, query).run(@db, params)
    end
  end
end

end
