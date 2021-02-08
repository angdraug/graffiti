#!/usr/bin/env ruby
#
# Graffiti RDF Store tests
#
#   Copyright (c) 2002-2009  Dmitry Borodaenko <angdraug@debian.org>
#
#   This program is free software.
#   You can distribute/modify this program under the terms of
#   the GNU General Public License version 3 or later.
#
# vim: et sw=2 sts=2 ts=8 tw=0

require 'test/unit'
require 'yaml'
require 'sequel'
require 'graffiti'

include Graffiti

class TC_Storage < Test::Unit::TestCase

  def setup
    config = File.open(
      File.join(
        File.dirname(File.dirname(__FILE__)),
        'doc', 'examples', 'samizdat-rdf-config.yaml'
      )
    ) {|f| YAML.load(f.read) }
    @db = create_mock_db
    @store = Store.new(@db, config)
    @ns = @store.config.ns
  end

  def test_query_select
    squish = %{
SELECT ?msg, ?title, ?name, ?date, ?rating
WHERE (dc::title ?msg ?title)
      (dc::creator ?msg ?creator)
      (s::fullName ?creator ?name)
      (dc::date ?msg ?date)
      (rdf::subject ?stmt ?msg)
      (rdf::predicate ?stmt dc::relation)
      (rdf::object ?stmt s::Quality)
      (s::rating ?stmt ?rating)
LITERAL ?rating >= -1
ORDER BY ?rating DESC
USING PRESET NS}

    sql = "SELECT DISTINCT b.id AS msg, b.title AS title, a.full_name AS name, c.published_date AS date, d.rating AS rating
FROM member AS a
INNER JOIN message AS b ON (b.creator = a.id)
INNER JOIN resource AS c ON (b.id = c.id)
INNER JOIN statement AS d ON (b.id = d.subject)
INNER JOIN resource AS e ON (d.predicate = e.id) AND (e.uriref = 't' AND e.label = 'http://purl.org/dc/elements/1.1/relation')
INNER JOIN resource AS f ON (d.object = f.id) AND (f.uriref = 't' AND f.label = 'http://www.nongnu.org/samizdat/rdf/schema#Quality')
WHERE (c.published_date IS NOT NULL)
AND (a.full_name IS NOT NULL)
AND (d.id IS NOT NULL)
AND (b.title IS NOT NULL)
AND (d.rating >= -1)
ORDER BY d.rating DESC"

    test_squish_select(squish, sql) do |query|
      assert_equal %w[?msg ?title ?name ?date ?rating], query.nodes
      assert query.pattern.include?(["#{@ns['dc']}title", "?msg", "?title", nil, false])
      assert_equal '?rating >= -1', query.literal
      assert_equal '?rating', query.order
      assert_equal 'DESC', query.order_dir
      assert_equal @ns['s'], query.ns['s']
    end

    assert_equal [], @store.select_all(squish)
  end

  def test_query_assert
    # initialize
    query_text = %{
INSERT ?msg
UPDATE ?title = 'Test Message', ?content = 'Some ''text''.'
WHERE (dc::creator ?msg 1)
      (dc::title ?msg ?title)
      (s::content ?msg ?content)
USING dc FOR #{@ns['dc']}
      s FOR #{@ns['s']}}
    query = assert_nothing_raised do
      SquishAssert.new(@store.config, query_text)
    end

    # query parser
    assert_equal ['?msg'], query.insert
    assert_equal({'?title' => "'0'", '?content' => "'1'"}, query.update)
    assert query.pattern.include?(["#{@ns['dc']}title", "?msg", "?title", nil, false])
    assert_equal @ns['s'], query.ns['s']
    assert_equal "'Test Message'", query.substitute_literals("'0'")
    assert_equal "'Some ''text''.'", query.substitute_literals("'1'")

    # mock db
    ids = @store.assert(query_text)
    assert_equal [1], ids
    assert_equal 'Test Message', @db[:Message][:id => 1][:title]

    id2 = @store.assert(query_text)
    query_text = %{
UPDATE ?rating = :rating
WHERE (rdf::subject ?stmt :related)
      (rdf::predicate ?stmt dc::relation)
      (rdf::object ?stmt 1)
      (s::voteProposition ?vote ?stmt)
      (s::voteMember ?vote :member)
      (s::voteRating ?vote ?rating)}
    params = {:rating => -1, :related => 2, :member => 3}
    ids = @store.assert(query_text, params)
    assert_equal [], ids
    assert vote = @db[:vote].order(:id).last
    assert_equal -1, vote[:rating].to_i

    params[:rating] = -2
    @store.assert(query_text, params)
    assert vote2 = @db[:vote].order(:id).last
    assert_equal -2, vote2[:rating].to_i
    assert_equal vote[:id], vote2[:id]
  end

  def test_query_assert_expression
    query_text = %{
UPDATE ?rating = 2 * :rating
WHERE (rdf::subject ?stmt :related)
      (rdf::predicate ?stmt dc::relation)
      (rdf::object ?stmt 1)
      (s::voteProposition ?vote ?stmt)
      (s::voteMember ?vote :member)
      (s::voteRating ?vote ?rating)}
    params = {:rating => -1, :related => 2, :member => 3}
    @store.assert(query_text, params)
    assert vote = @db[:vote].order(:id).last
    assert_equal -2, vote[:rating].to_i
  end
  private :test_query_assert_expression

  def test_dangling_blank_node
    squish = %{
SELECT ?msg
WHERE (s::inReplyTo ?msg ?parent)
USING s FOR #{@ns['s']}}

    sql = "SELECT DISTINCT a.id AS msg
FROM resource AS a
INNER JOIN resource AS b ON (a.part_of_subproperty = b.id) AND (b.uriref = 't' AND b.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
WHERE (a.id IS NOT NULL)"

    test_squish_select(squish, sql) do |query|
      assert_equal %w[?msg], query.nodes
      assert query.pattern.include?(["#{@ns['s']}inReplyTo", "?msg", "?parent", nil, false])
      assert_equal @ns['s'], query.ns['s']
    end
  end

  def test_external_resource_no_self_join
    squish = %{SELECT ?id WHERE (s::id tag::Translation ?id)}

    sql = "SELECT DISTINCT a.id AS id
FROM resource AS a
WHERE (a.id IS NOT NULL)
AND ((a.uriref = 't' AND a.label = 'http://www.nongnu.org/samizdat/rdf/tag#Translation'))"

    test_squish_select(squish, sql) do |query|
      assert_equal %w[?id], query.nodes
      assert query.pattern.include?(["#{@ns['s']}id", "#{@ns['tag']}Translation", "?id", nil, false])
      assert_equal @ns['s'], query.ns['s']
    end
  end

  #def test_internal_resource
  #end

  #def test_external_subject_internal_property
  #end

  def test_except
    squish = %{
SELECT ?msg
WHERE (dc::date ?msg ?date)
EXCEPT (s::inReplyTo ?msg ?parent)
       (dct::isVersionOf ?msg ?version_of)
       (dc::creator ?version_of 1)
ORDER BY ?date DESC}

    sql = "SELECT DISTINCT a.id AS msg, a.published_date AS date
FROM resource AS a
LEFT JOIN (
    SELECT a.id AS _field_c
    FROM message AS b
    INNER JOIN resource AS a ON (a.part_of = b.id)
    INNER JOIN resource AS c ON (a.part_of_subproperty = c.id) AND (c.uriref = 't' AND c.label = 'http://purl.org/dc/terms/isVersionOf')
    WHERE (b.creator = 1)
) AS _subquery_a ON (a.id = _subquery_a._field_c)
LEFT JOIN resource AS d ON (a.part_of_subproperty = d.id) AND (d.uriref = 't' AND d.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
WHERE (a.published_date IS NOT NULL)
AND (a.id IS NOT NULL)
AND (_subquery_a._field_c IS NULL)
AND (d.id IS NULL)
ORDER BY a.published_date DESC"

    test_squish_select(squish, sql)
  end

  def test_except_group_by
    squish = %{
SELECT ?msg
WHERE (rdf::predicate ?stmt dc::relation)
      (rdf::subject ?stmt ?msg)
      (rdf::object ?stmt ?tag)
      (dc::date ?stmt ?date)
      (s::rating ?stmt ?rating FILTER ?rating >= 1.5)
      (s::hidden ?msg ?hidden FILTER ?hidden = 'f')
EXCEPT (dct::isPartOf ?msg ?parent)
GROUP BY ?msg
ORDER BY max(?date) DESC}

    sql = "SELECT DISTINCT c.subject AS msg, max(d.published_date)
FROM message AS a
INNER JOIN statement AS c ON (c.subject = a.id) AND (c.rating >= 1.5)
INNER JOIN resource AS b ON (c.subject = b.id)
INNER JOIN resource AS d ON (c.id = d.id)
INNER JOIN resource AS e ON (c.predicate = e.id) AND (e.uriref = 't' AND e.label = 'http://purl.org/dc/elements/1.1/relation')
WHERE (d.published_date IS NOT NULL)
AND (a.hidden IS NOT NULL)
AND (b.part_of IS NULL)
AND (c.rating IS NOT NULL)
AND (c.object IS NOT NULL)
AND ((a.hidden = 'f'))
GROUP BY c.subject
ORDER BY max(d.published_date) DESC"

    test_squish_select(squish, sql)
  end

  def test_optional
    squish = %{
SELECT ?date, ?creator, ?lang, ?parent, ?version_of, ?hidden, ?open
WHERE (dc::date 1 ?date)
OPTIONAL (dc::creator 1 ?creator)
         (dc::language 1 ?lang)
         (s::inReplyTo 1 ?parent)
         (dct::isVersionOf 1 ?version_of)
         (s::hidden 1 ?hidden)
         (s::openForAll 1 ?open)}

    sql = "SELECT DISTINCT a.published_date AS date, b.creator AS creator, b.language AS lang, select_subproperty(a.part_of, d.id) AS parent, select_subproperty(a.part_of, c.id) AS version_of, b.hidden AS hidden, b.open AS open
FROM resource AS a
INNER JOIN message AS b ON (a.id = b.id)
LEFT JOIN resource AS c ON (a.part_of_subproperty = c.id) AND (c.uriref = 't' AND c.label = 'http://purl.org/dc/terms/isVersionOf')
LEFT JOIN resource AS d ON (a.part_of_subproperty = d.id) AND (d.uriref = 't' AND d.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
WHERE (a.published_date IS NOT NULL)
AND ((a.id = 1))"

    test_squish_select(squish, sql)
  end

  def test_except_optional_transitive
    squish = %{
SELECT ?msg
WHERE (rdf::subject ?stmt ?msg)
      (rdf::predicate ?stmt dc::relation)
      (rdf::object ?stmt ?tag)
      (s::rating ?stmt ?rating FILTER ?rating > 0)
      (dc::date ?msg ?date)
EXCEPT (dct::isPartOf ?msg ?parent)
OPTIONAL (dct::isPartOf ?tag ?supertag TRANSITIVE)
LITERAL ?tag = 1 OR ?supertag = 1
ORDER BY ?date DESC}

    sql = "SELECT DISTINCT b.subject AS msg, a.published_date AS date
FROM resource AS a
INNER JOIN statement AS b ON (b.subject = a.id) AND (b.rating > 0)
INNER JOIN resource AS d ON (b.predicate = d.id) AND (d.uriref = 't' AND d.label = 'http://purl.org/dc/elements/1.1/relation')
LEFT JOIN part AS c ON (b.object = c.id)
WHERE (a.published_date IS NOT NULL)
AND (a.part_of IS NULL)
AND (b.rating IS NOT NULL)
AND (b.id IS NOT NULL)
AND (b.object = 1 OR c.part_of = 1)
ORDER BY a.published_date DESC"

    test_squish_select(squish, sql)
  end

  def test_optional_connect_by_object
    squish = %{
SELECT ?event
WHERE (ical::dtstart ?event ?dtstart FILTER ?dtstart >= 'now')
      (ical::dtend ?event ?dtend)
OPTIONAL (s::rruleEvent ?rrule ?event)
         (ical::until ?rrule ?until FILTER ?until IS NULL OR ?until > 'now')
LITERAL ?dtend > 'now' OR ?rrule IS NOT NULL
ORDER BY ?event DESC}

    sql = "SELECT DISTINCT b.id AS event
FROM event AS b
LEFT JOIN recurrence AS a ON (b.id = a.event) AND (a.until IS NULL OR a.until > 'now')
WHERE (b.dtstart IS NOT NULL)
AND ((b.dtstart >= 'now'))
AND (b.dtend > 'now' OR a.id IS NOT NULL)
ORDER BY b.id DESC"

    test_squish_select(squish, sql)
  end
  private :test_optional_connect_by_object

  def test_many_to_many
    # pretend that Vote is a many-to-many relation table
    squish = %{
SELECT ?p, ?date
WHERE (s::voteRating ?p ?vote1 FILTER ?vote1 > 0)
      (s::voteRating ?p ?vote2 FILTER ?vote2 < 0)
      (dc::date ?p ?date)
ORDER BY ?date DESC}

    sql = "SELECT DISTINCT a.id AS p, c.published_date AS date
FROM vote AS a
INNER JOIN vote AS b ON (a.id = b.id) AND (b.rating < 0)
INNER JOIN resource AS c ON (a.id = c.id)
WHERE (c.published_date IS NOT NULL)
AND (a.rating IS NOT NULL)
AND (b.rating IS NOT NULL)
AND ((a.rating > 0))
ORDER BY c.published_date DESC"

    test_squish_select(squish, sql)
  end

  def test_update_null_and_subproperty
    query_text =
      %{INSERT ?msg
      UPDATE ?parent = :parent
      WHERE (dct::isPartOf ?msg ?parent)}
    @store.assert(query_text, :id => 1, :parent => 3)
    assert_equal 3, @db[:resource].filter(:id => 1).get(:part_of)

    # check that subproperty is set
    query_text =
      %{UPDATE ?parent = :parent
      WHERE (s::subTagOf :id ?parent)}
    @store.assert(query_text, :id => 1, :parent => 3)
    assert_equal 3, @db[:resource].filter(:id => 1).get(:part_of)
    assert_equal 2, @db[:resource].filter(:id => 1).get(:part_of_subproperty)

    # check that NULL is handled correctly and that subproperty is unset
    query_text =
      %{UPDATE ?parent = NULL
      WHERE (dct::isPartOf :id ?parent)}
    @store.assert(query_text, :id => 1)
    assert_equal nil, @db[:resource].filter(:id => 1).get(:part_of)
    assert_equal nil, @db[:resource].filter(:id => 1).get(:part_of_subproperty)
  end

  private

  def test_squish_select(squish, sql)
    query = assert_nothing_raised do
      SquishSelect.new(@store.config, squish)
    end

    yield query if block_given?

    # query result
    sql1 = assert_nothing_raised do
      @store.select(query)
    end
    sql2 = assert_nothing_raised do
      @store.select(squish)
    end
    assert sql1 == sql2

    # transform result
    assert_equal normalize(sql), normalize(sql1),
      "Query doesn't match. Expected:\n#{sql}\nReceived:\n#{sql1}"
  end

  def normalize(sql)
    sql
  end

  def create_mock_db
    db = Sequel.sqlite(:quote_identifiers => false, :integer_booleans => false)

    db.create_table(:resource) do
      primary_key :id
      Time :published_date
      Integer :part_of
      Integer :part_of_subproperty
      Integer :part_sequence_number
      TrueClass :literal
      TrueClass :uriref
      String :label
    end

    db.create_table(:statement) do
      primary_key :id
      Integer :subject
      Integer :predicate
      Integer :object
      BigDecimal :rating, :size => [4, 2]
    end

    db.create_table(:member) do
      primary_key :id
      String :login
      String :full_name
      String :email
    end

    db.create_table(:message) do
      primary_key :id
      String :title
      Integer :creator
      String :format
      String :language
      TrueClass :open
      TrueClass :hidden
      TrueClass :locked
      String :content
      String :html_full
      String :html_short
    end

    db.create_table(:vote) do
      primary_key :id
      Integer :proposition
      Integer :member
      BigDecimal :rating, :size => 2
    end

    db
  end
end
