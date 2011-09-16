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
    @store = Store.new(nil, config)
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

    sql = "SELECT DISTINCT c.id, c.title, b.full_name, d.published_date, a.rating
FROM Statement AS a
INNER JOIN Message AS c ON (c.id = a.subject)
INNER JOIN Member AS b ON (c.creator = b.id)
INNER JOIN Resource AS d ON (c.id = d.id)
INNER JOIN Resource AS e ON (a.object = e.id) AND (e.literal = 'false' AND e.uriref = 'true' AND e.label = 'http://www.nongnu.org/samizdat/rdf/schema#Quality')
INNER JOIN Resource AS f ON (a.predicate = f.id) AND (f.literal = 'false' AND f.uriref = 'true' AND f.label = 'http://purl.org/dc/elements/1.1/relation')
WHERE (a.id IS NOT NULL)
AND (d.published_date IS NOT NULL)
AND (b.full_name IS NOT NULL)
AND (c.title IS NOT NULL)
AND (a.rating >= -1)
ORDER BY a.rating DESC"

    test_squish_select(squish, sql) do |query|
      assert_equal %w[?msg ?title ?name ?date ?rating], query.nodes
      assert query.pattern.include?(["#{@ns['dc']}title", "?msg", "?title", nil, false])
      assert_equal '?rating >= -1', query.literal
      assert_equal '?rating', query.order
      assert_equal 'DESC', query.order_dir
      assert_equal @ns['s'], query.ns['s']
    end
  end

  def test_query_assert
    # initialize
    query_text = %{
INSERT ?msg
UPDATE ?title = 'Test Message', ?content = 'Some text.'
WHERE (dc::creator ?msg 1)
      (dc::title ?msg ?title)
      (s::content ?msg ?content)
USING dc FOR #{@ns['dc']}
      s FOR #{@ns['s']}}
    begin
      query = SquishAssert.new(nil, @store.config, query_text)
    rescue
      assert false, "SquishAssert initialization raised #{$!.class}: #{$!}"
    end

    # query parser
    assert_equal [['?msg'], {'?title' => "'0'", '?content' => "'1'"}],
      query.nodes
    assert query.pattern.include?(["#{@ns['dc']}title", "?msg", "?title", nil, false])
    assert_equal @ns['s'], query.ns['s']
    assert_equal "'Test Message'", query.substitute_literals("'0'")
    assert_equal "'Some text.'", query.substitute_literals("'1'")

    # SqlMapper
    sql = ''
    #assert_equal sql, query.to_sql.first.gsub(/\s+/, ' ')

    # todo: check assert against MockDb
  end

  def test_dangling_blank_node
    squish = %{
SELECT ?msg
WHERE (s::inReplyTo ?msg ?parent)
USING s FOR #{@ns['s']}}

    sql = "SELECT DISTINCT a.id
FROM Resource AS a
INNER JOIN Resource AS b ON (a.part_of_subproperty = b.id) AND (b.literal = 'false' AND b.uriref = 'true' AND b.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
WHERE (a.id IS NOT NULL)"

    test_squish_select(squish, sql) do |query|
      assert_equal %w[?msg], query.nodes
      assert query.pattern.include?(["#{@ns['s']}inReplyTo", "?msg", "?parent", nil, false])
      assert_equal @ns['s'], query.ns['s']
    end
  end

  def test_external_resource_no_self_join
    squish = %{SELECT ?id WHERE (s::id tag::Translation ?id)}

    sql = "SELECT DISTINCT a.id
FROM Resource AS a
WHERE (a.id IS NOT NULL)
AND ((a.literal = 'false' AND a.uriref = 'true' AND a.label = 'http://www.nongnu.org/samizdat/rdf/tag#Translation'))"

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

    sql = "SELECT DISTINCT b.id, b.published_date
FROM Resource AS b
LEFT JOIN (
    SELECT b.id AS _field_f
    FROM Message AS a
    INNER JOIN Resource AS b ON (b.part_of = a.id)
    INNER JOIN Resource AS c ON (b.part_of_subproperty = c.id) AND (c.literal = 'false' AND c.uriref = 'true' AND c.label = 'http://purl.org/dc/terms/isVersionOf')
    WHERE (a.creator = 1)
) AS _subquery_a ON (b.id = _subquery_a._field_f)
LEFT JOIN Resource AS d ON (b.part_of_subproperty = d.id) AND (d.literal = 'false' AND d.uriref = 'true' AND d.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
WHERE (b.published_date IS NOT NULL)
AND (b.id IS NOT NULL)
AND (_subquery_a._field_f IS NULL)
AND (d.id IS NULL)
ORDER BY b.published_date DESC"

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
      (s::hidden ?msg ?hidden FILTER ?hidden = 'false')
EXCEPT (dct::isPartOf ?msg ?parent)
GROUP BY ?msg
ORDER BY max(?date) DESC}

    sql = "SELECT DISTINCT a.subject, max(b.published_date)
FROM Statement AS a
INNER JOIN Resource AS b ON (a.id = b.id)
INNER JOIN Message AS c ON (a.subject = c.id) AND (c.hidden = 'false')
INNER JOIN Resource AS d ON (a.subject = d.id)
INNER JOIN Resource AS e ON (a.predicate = e.id) AND (e.literal = 'false' AND e.uriref = 'true' AND e.label = 'http://purl.org/dc/elements/1.1/relation')
WHERE (c.hidden IS NOT NULL)
AND (b.published_date IS NOT NULL)
AND (a.object IS NOT NULL)
AND (a.rating IS NOT NULL)
AND (d.part_of IS NULL)
AND ((a.rating >= 1.5))
GROUP BY a.subject
ORDER BY max(b.published_date) DESC"

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

    sql = "SELECT DISTINCT a.published_date, b.creator, b.language, select_subproperty(a.part_of, d.id), select_subproperty(a.part_of, c.id), b.hidden, b.open
FROM Resource AS a
INNER JOIN Message AS b ON (a.id = b.id)
LEFT JOIN Resource AS c ON (a.part_of_subproperty = c.id) AND (c.literal = 'false' AND c.uriref = 'true' AND c.label = 'http://purl.org/dc/terms/isVersionOf')
LEFT JOIN Resource AS d ON (a.part_of_subproperty = d.id) AND (d.literal = 'false' AND d.uriref = 'true' AND d.label = 'http://www.nongnu.org/samizdat/rdf/schema#inReplyTo')
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

    sql = "SELECT DISTINCT a.subject, b.published_date
FROM Statement AS a
INNER JOIN Resource AS b ON (a.subject = b.id)
INNER JOIN Resource AS d ON (a.predicate = d.id) AND (d.literal = 'false' AND d.uriref = 'true' AND d.label = 'http://purl.org/dc/elements/1.1/relation')
LEFT JOIN Part AS c ON (a.object = c.id)
WHERE (a.id IS NOT NULL)
AND (b.published_date IS NOT NULL)
AND (a.rating IS NOT NULL)
AND (b.part_of IS NULL)
AND ((a.rating > 0))
AND (a.object = 1 OR c.part_of = 1)
ORDER BY b.published_date DESC"

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

    sql = "SELECT DISTINCT b.id
FROM Event AS b
LEFT JOIN Recurrence AS a ON (b.id = a.event) AND (a.until IS NULL OR a.until > 'now')
WHERE (b.dtstart IS NOT NULL)
AND ((b.dtstart >= 'now'))
AND (b.dtend > 'now' OR a.id IS NOT NULL)
ORDER BY b.id DESC"

    test_squish_select(squish, sql)
  end

  def test_many_to_many
    # pretend that Vote is a many-to-many relation table
    squish = %{
SELECT ?p, ?date
WHERE (s::voteRating ?p ?vote1 FILTER ?vote1 > 0)
      (s::voteRating ?p ?vote2 FILTER ?vote2 < 0)
      (dc::date ?p ?date)
ORDER BY ?date DESC}

    sql = "SELECT DISTINCT a.id, c.published_date
FROM Vote AS a
INNER JOIN Vote AS b ON (a.id = b.id) AND (b.rating < 0)
INNER JOIN Resource AS c ON (a.id = c.id)
WHERE (c.published_date IS NOT NULL)
AND (a.rating IS NOT NULL)
AND (b.rating IS NOT NULL)
AND ((a.rating > 0))
ORDER BY c.published_date DESC"

    test_squish_select(squish, sql)
  end

  private

  def test_squish_select(squish, sql)
    begin
      query = SquishSelect.new(@store.config, squish)
    rescue
      assert false, "SquishSelect initialization raised #{$!.class}: #{$!}"
    end

    yield query if block_given?

    # query result
    begin
      sql1 = @store.select(query)
    rescue
      assert false, "select with pre-parsed query raised #{$!.class}: #{$!}"
    end
    begin
      sql2 = @store.select(squish)
    rescue
      assert false, "select with query text raised #{$!.class}: #{$!}"
    end
    assert sql1 == sql2

    # transform result
    assert_equal normalize(sql), normalize(sql1.first),
      "Query doesn't match. Expected:\n#{sql}\nReceived:\n#{sql1.first}"
  end

  def normalize(sql)
    # alias labels and where conditions may be reordered, but the query string
    # length should remain the same
    sql.size
  end
end
