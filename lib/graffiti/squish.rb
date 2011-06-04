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

require 'graffiti/exceptions'
require 'graffiti/sql_mapper'

module Graffiti

# parse Squish query and translate triples to relational conditions
#
# provides access to internal representation of the parsed query and utility
# functions to deal with Squish syntax
#
class SquishQuery
  # regexp for internal resource reference
  INTERNAL = Regexp.new(/\A([[:digit:]]+)\z/).freeze

  # regexp for blank node mark and name
  BN = Regexp.new(/\A\?([[:alnum:]_]+)\z/).freeze

  # regexp for scanning blank nodes inside a string
  BN_SCAN = Regexp.new(/\?[[:alnum:]_]+?\b/).freeze

  # regexp for parametrized value
  PARAMETER = Regexp.new(/\A:([[:alnum:]_]+)\z/).freeze

  # regexp for replaced string literal
  LITERAL = Regexp.new(/\A'\d+'\z/).freeze

  # regexp for number
  NUMBER = Regexp.new(/\A-?[[:digit:]]+(\.[[:digit:]]+)?\z/).freeze

  # regexp for operator
  OPERATOR = Regexp.new(/\A(\+|-|\*|\/|\|\||<|<=|>|>=|=|!=|@@|to_tsvector|to_tsquery|I?LIKE|NOT|AND|OR|IN|IS|NULL)\z/i).freeze

  # regexp for aggregate function
  AGGREGATE = Regexp.new(/\A(avg|count|max|min|sum)\z/i).freeze

  QUERY = Regexp.new(/\A\s*(SELECT|INSERT|UPDATE)\b\s*(.*?)\s*
        \bWHERE\b\s*(.*?)\s*
        (?:\bEXCEPT\b\s*(.*?))?\s*
        (?:\bOPTIONAL\b\s*(.*?))?\s*
        (?:\bLITERAL\b\s*(.*?))?\s*
        (?:\bGROUP\s+BY\b\s*(.*?))?\s*
        (?:\bORDER\s+BY\b\s*(.*?)\s*(ASC|DESC)?)?\s*
        (?:\bUSING\b\s*(.*?))?\s*\z/mix).freeze

  # extract common Squish query sections, perform namespace substitution,
  # generate query pattern graph, call transform_pattern,
  # determine query type and parse nodes section accordingly
  #
  def initialize(config, query)
    query.nil? and raise ProgrammingError, "SquishQuery: query can't be nil"
    if query.kind_of? Hash   # pre-parsed query (used by SquishAssert)
      @type = query[:type]
      @nodes = query[:nodes]
      @pattern = query[:pattern]
      @negative = query[:negative]
      @optional = query[:optional]
      @strings = query[:strings]
      @literal = @group = @order = ''
      @sql_mapper = SqlMapper.new(config, @pattern)
      return self
    elsif not query.kind_of? String
      raise ProgrammingError,
        "Bad query initialization parameter class: #{query.class}"
    end

    @query = query   # keep original string
    query = query.dup

    # replace string literals with 'n' placeholders (also see #substitute_literals)
    @strings = []
    query.gsub!(/'(''|[^'])*'/m) do
      @strings.push $&
      "'" + (@strings.size - 1).to_s + "'"
    end

    match = QUERY.match(query) or raise ProgrammingError,
      "Malformed query: are keywords SELECT, INSERT, UPDATE or WHERE missing?"
    match, @key, @nodes, @pattern, @negative, @optional, @literal,
      @group, @order, @order_dir, @ns = match.to_a.collect {|m| m.to_s }
    match = nil
    @key.upcase!
    @order_dir.upcase!

    # namespaces
    # todo: validate ns
    @ns = (@ns.empty? or /\APRESET\s+NS\z/ =~ @ns) ? config.ns :
      Hash[*@ns.gsub(/\b(FOR|AS|AND)\b/i, '').scan(/\S+/)]
    @pattern = parse_pattern(@pattern)
    @optional = parse_pattern(@optional)
    @negative = parse_pattern(@negative)

    # validate SQL expressions
    validate_expression(@literal)
    @group.split(/\s*,\s*/).each {|group| validate_expression(group) }
    validate_expression(@order)

    @sql_mapper = SqlMapper.new(
      config, @pattern, @negative, @optional, @literal)

    # check that all variables can be bound
    @variables = query.scan(BN_SCAN)
    @variables.each {|node| @sql_mapper.bind(node) }

    # determine query type, parse and validate nodes section
    if 'SELECT' == @key
      @type = :select
      @nodes = @nodes.split(/\s*,\s*/).collect {|node|
        validate_expression(node)
        node
      }

    else
      @type = :assert

      if 'UPDATE' == @key
        insert = ''
        update = @nodes

      elsif 'INSERT' == @key and @nodes =~ /\A\s*(.*?)\s*(?:\bUPDATE\b\s*(.*?))?\s*\z/
        insert, update = $1, $2.to_s

      else
        raise ProgrammingError,
          "Query doesn't start with one of SELECT, INSERT, or UPDATE"
      end

      insert = insert.split(/\s*,\s*/).each {|s|
        s =~ BN or raise ProgrammingError,
          "Blank node expected in INSERT section instead of '#{s}'"
      }

      update = update.empty? ? {} : Hash[*update.split(/\s*,\s*/).collect {|s|
        s.split(/\s*=\s*/)
      }.each {|node, value|
        node =~ BN or raise ProgrammingError,
          "Blank node expected on the left side of UPDATE assignment instead of '#{bn}'"
        validate_expression(value)
      }.flatten!]

      @nodes = [insert, update]
    end

    return self
  end

  # blank variables control section
  attr_reader :nodes

  # query pattern graph as array of triples [ [p, s, o], ... ]
  attr_reader :pattern

  # literal SQL expression
  attr_reader :literal

  # SQL GROUP BY expression
  attr_reader :group

  # SQL order expression
  attr_reader :order

  # direction of order, ASC or DESC
  attr_reader :order_dir

  # query namespaces mapping
  attr_reader :ns

  # list of variables defined in the query
  attr_reader :variables

  # returns original string passed in for parsing
  #
  def to_s
    @query
  end

  # replace 'n' substitutions with query string literals (see #new, #LITERAL)
  #
  def substitute_literals(s)
    return s unless s.kind_of? String
    s.gsub(/'(\d+)'/) do
      @strings[$1.to_i] or $&
    end
  end

  # replace schema uri with namespace prefix
  #
  def SquishQuery.uri_shrink!(uriref, prefix, uri)
    uriref.gsub!(/\A#{uri}([^\/#]+)\z/) {"#{prefix}::#{$1}"}
  end

  # replace schema uri with a prefix from a supplied namespaces hash
  #
  def SquishQuery.ns_shrink(uriref, namespaces)
    u = uriref.dup or return nil
    namespaces.each {|p, uri| SquishQuery.uri_shrink!(u, p, uri) and break }
    return u
  end

  # replace schema uri with a prefix from query namespaces
  #
  def ns_shrink(uriref)
    SquishQuery.ns_shrink(uriref, @ns)
  end

  # validate expression
  #
  # expression := value [ operator expression ]
  #
  # value := blank_node | literal_string | number | '(' expression ')'
  #
  # whitespace between tokens (except inside parentheses) is mandatory
  #
  def validate_expression(string)
    # todo: lexical analyser
    string.split(/[\s(),]+/).collect do |token|
      case token
      when '', BN, PARAMETER, LITERAL, NUMBER, OPERATOR, AGGREGATE
      else
        raise ProgrammingError, "Bad token '#{token}' in expression"
      end
    end
  end

  private

  PATTERN_SCAN = Regexp.new(/\A\((\S+)\s+(\S+)\s+(.*?)(?:\s+FILTER\b\s*(.*?)\s*)?(?:\s+(TRANSITIVE)\s*)?\)\z/).freeze

  # parse query pattern graph out of a string, expand URI namespaces
  #
  def parse_pattern(pattern)
    pattern.scan(/\(.*?\)(?=\s*(?:\(|\z))/).collect do |c|
      match, predicate, subject, object, filter, transitive = c.match(PATTERN_SCAN).to_a
      match = nil

      [predicate, subject, object].each do |u|
        u.sub!(/\A(\S+?)::/) do
          @ns[$1] or raise ProgrammingError, "Undefined namespace prefix #{$1}"
        end
      end

      validate_expression(filter.to_s)

      [predicate, subject, object, filter, 'TRANSITIVE' == transitive]
    end
  end

  # replace RDF query parameters in SQL query with '?' marks,
  # return resultant query and array of parameter values
  #
  def substitute_parameters(sql, params={})
    values = []
    sql.gsub!(/\B:([[:alnum:]_]+)/) do   # see #PARAMETER
      name = $1.to_sym
      params.has_key?(name) or raise ProgrammingError,
        "Missing value for :#{name} in parametrized query"
      values.push(params[name])
      '?'
    end
    [sql, values]
  end
end


class SquishSelect < SquishQuery
  # translate Squish SELECT query to SQL,
  # return SQL query and a list of parameter values in proper order
  #
  def to_sql(params={})
    raise ProgrammingError, "Wrong query type: select expected" unless
      @type == :select

    where = @sql_mapper.where

    select = @nodes.dup
    select.push(@order) unless @order.empty? or @nodes.include?(@order)

    # now put it all together
    sql = %{SELECT DISTINCT #{select.join(', ')}\nFROM #{@sql_mapper.from}}
    sql << %{\nWHERE #{where}} unless where.empty?
    sql << %{\nGROUP BY #{@group}} unless @group.empty?
    sql << %{\nORDER BY #{@order} #{@order_dir}} unless @order.empty?

    # replace blank node names with bindings
    sql.gsub!(BN_SCAN) {|node| @sql_mapper.bind(node) }
    sql =~ /\?/ and raise ProgrammingError,
      "Unexpected '?' in translated query (probably, caused by unmapped blank node): #{sql.gsub(/\s+/, ' ')};"

    sql, values = substitute_parameters(sql, params)
    [substitute_literals(sql), *values]
  end
end


class SquishAssert < SquishQuery
  def initialize(db, config, query)
    @db = db
    @config = config

    super(@config, query)
  end

  def assert(params={})
    raise ProgrammingError, 'Wrong query type: assert expected' unless
      @type == :assert

    insert, update = @nodes

    # Stage 1: Resources
    v = {}   # v: node -> value
    new = {} # new[node] if node was inserted into Resource at this stage and
             # needs to be inserted into an internal resource table later
    @sql_mapper.nodes.each do |node, n|

      if node =~ INTERNAL   # internal resource
        v[node] = $1   # resource id

      elsif node =~ PARAMETER or node =~ LITERAL
        v[node] = node   # pass parametrized value or string literal as is

      elsif node =~ BN
        subject_position = n[:positions].collect {|p|
          :subject == p[:role] ? p : nil
        }.compact.first

        if subject_position.nil?   # blank node occuring only in object position
          v[node] = update[node] or raise ProgrammingError,
            %{Blank node #{node} is undefined (drop it or set its value in UPDATE section)}

        else   # resource blank node
          unless insert.include?(node)
            s = SquishSelect.new(
              @config, {
                :type => :select,
                :nodes => [node],
                :pattern => subgraph(node),
                :strings => @strings
              }
            )
            v[node], = @db.select_one(*s.to_sql(params))
          end

          if v[node].nil?
            # fixme: special case for table == Resource
            @db.do "INSERT INTO Resource (label) VALUES (?)",
              @sql_mapper.clauses[ subject_position[:clause] ][:map].table
            v[node], = @db.select_one "SELECT MAX(id) FROM Resource"
            new[node] = true
          end
        end

      else   # external resource
        v[node], = @db.select_one "SELECT id FROM Resource
          WHERE literal = 'false' AND uriref = 'true' AND label = ?", node

        if v[node].nil?
          @db.do "INSERT INTO Resource (uriref, label) VALUES ('true', ?)", node
          v[node], = @db.select_one "SELECT MAX(id) FROM Resource"
        end
      end

      # by this point v[node] must be set
    end

    update.keys.each do |node|
      @sql_mapper.nodes[node][:positions].each do |p|
        next unless :object == p[:role]

        map = @sql_mapper.clauses[ p[:clause] ][:map]
        if map.subproperty_of
          # when subproperty value is updated, update the qualifier as well
          update[map.property] = v[map.property]
        end
      end
    end

    # Stage 2: Properties
    a = {}   # a: alias -> positions*
    @sql_mapper.clauses.each_with_index do |clause, i|
      a[ clause[:alias] ] ||= []
      a[ clause[:alias] ].push(clause)
    end

    statements = [] # [ { :key_node, :references, :sql, :values }, ... ]

    a.each do |alias_, clauses|
      key_node = clauses.first[:subject][:node]
      table = clauses.first[:map].table

      data = []   # [ [ field, value ], ... ]
      references = []
      clauses.each do |clause|
        node = clause[:object][:node]
        if new[key_node] or update[node]
          data.push([
            clause[:object][:field],
            substitute_literals(v[node])
          ])

          references.push(node) if new[node]
        end
      end

      if new[key_node] and table != 'Resource'
        data.unshift [ 'id', v[key_node] ]
        # when id is inserted, insert_resource() trigger does nothing
        sql = "INSERT INTO #{table} ("+
          data.collect {|field, value| field }.join(', ')+') VALUES ('+
          data.collect {|field, value| value }.join(', ')+')'

      elsif data.length > 0
        sql = "UPDATE #{table} SET "+
          data.collect {|field, value| field+' = '+value.to_s }.join(', ')+
          " WHERE id = #{v[key_node]}"
      end

      if sql
        sql, values = substitute_parameters(sql, params)
        statements.push({
          :key_node => key_node,
          :references => references,
          :sql => sql,
          :values => values
        })
      end
    end

    run_ordered_statements(statements)

    return insert.collect {|node| v[node] }
  end

  private

  # calculate subgraph of query pattern that is reachable from _node_
  #
  # fixme: make it work with optional sub-patterns
  #
  def subgraph(node)
    subgraph = [node]
    w = []
    begin
      stop = true
      @pattern.each do |triple|
        if subgraph.include? triple[1] and not w.include? triple
          subgraph.push triple[2]
          w.push triple
          stop = false
        end
      end
    end until stop
    return w
  end

  # make sure mutually referencing records are inserted in the right order
  #
  def run_ordered_statements(statements)
    statements = statements.sort_by {|s| s[:references].size }
    inserted = []

    progress = true
    until statements.empty? or not progress
      progress = false

      0.upto(statements.size - 1) do |i|
        s = statements[i]
        if (s[:references] - inserted).empty?
          @db.do(s[:sql], *s[:values])
          inserted.push(s[:key_node])
          statements.delete_at(i)
          progress = true
          break
        end
      end
    end

    progress or raise ProgrammingError,
      "Failed to resolve mutual references of inserted resources: " +
      statements.collect {|s| s[:sql] + ' -- ' + s[:references].join(', ') }.join('; ')
  end
end

end
