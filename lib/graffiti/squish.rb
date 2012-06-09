# Graffiti RDF Store
# (originally written for Samizdat project)
#
#   Copyright (c) 2002-2011  Dmitry Borodaenko <angdraug@debian.org>
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
  include Debug

  # regexp for internal resource reference
  INTERNAL = Regexp.new(/\A([[:digit:]]+)\z/).freeze

  # regexp for blank node mark and name
  BN = Regexp.new(/\A\?([[:alnum:]_]+)\z/).freeze

  # regexp for scanning blank nodes inside a string
  BN_SCAN = Regexp.new(/\?[[:alnum:]_]+?\b/).freeze

  # regexp for parametrized value
  PARAMETER = Regexp.new(/\A:([[:alnum:]_]+)\z/).freeze

  # regexp for replaced string literal
  LITERAL = Regexp.new(/\A'(\d+)'\z/).freeze

  # regexp for scanning replaced string literals in a string
  LITERAL_SCAN = Regexp.new(/'(\d+)'/).freeze

  # regexp for scanning query parameters inside a string
  PARAMETER_AND_LITERAL_SCAN = Regexp.new(/\B:([[:alnum:]_]+)|'(\d+)'/).freeze

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

    debug { 'SquishQuery ' + query }
    @query = query   # keep original string
    query = query.dup

    # replace string literals with 'n' placeholders (also see #substitute_literals)
    @strings = []
    query.gsub!(/'((?:''|[^'])*)'/m) do
      @strings.push $1.gsub("''", "'")   # keep unescaped string
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
    s.gsub(LITERAL_SCAN) do
      get_literal_value($1.to_i)
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
    string.split(/[\s(),]+/).each do |token|
      case token
      when '', BN, PARAMETER, LITERAL, NUMBER, OPERATOR, AGGREGATE
      else
        raise ProgrammingError, "Bad token '#{token}' in expression"
      end
    end
    string
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

  # replace RDF query parameters with their values
  #
  def expression_value(expr, params={})
    case expr
    when 'NULL'
      nil
    when PARAMETER
      get_parameter_value($1, params)
    when LITERAL
      @strings[$1.to_i]
    else
      expr.gsub(PARAMETER_AND_LITERAL_SCAN) do
        if $1   # parameter
          get_parameter_value($1, params)
        else   # literal
          get_literal_value($2.to_i)
        end
      end
      # fixme: make Sequel treat it as SQL expression, not a string value
    end
  end

  def get_parameter_value(name, params)
    key = name.to_sym
    params.has_key?(key) or raise ProgrammingError,
      'Unknown parameter :' + name
    params[key]
  end

  def get_literal_value(i)
    "'" + @strings[i].gsub("'", "''") + "'"
  end
end


class SquishSelect < SquishQuery
  def initialize(config, query)
    super(config, query)

    if @key   # initialized from a String, not a Hash
      'SELECT' == @key or raise ProgrammingError,
        'Wrong query type: SELECT expected intead of ' + @key

      @nodes = @nodes.split(/\s*,\s*/).map {|node|
        validate_expression(node)
      }
    end
  end

  # translate Squish SELECT query to SQL
  #
  def to_sql
    where = @sql_mapper.where

    select = @nodes.dup
    select.push(@order) unless @order.empty? or @nodes.include?(@order)

    # now put it all together
    sql = %{\nFROM #{@sql_mapper.from}}
    sql << %{\nWHERE #{where}} unless where.empty?
    sql << %{\nGROUP BY #{@group}} unless @group.empty?
    sql << %{\nORDER BY #{@order} #{@order_dir}} unless @order.empty?

    select = select.map do |expr|
      bind_blank_nodes(expr) + (BN.match(expr) ? (' AS ' + $1) : '')
    end
    sql = 'SELECT DISTINCT ' << select.join(', ') << bind_blank_nodes(sql)

    sql =~ /\?/ and raise ProgrammingError,
      "Unexpected '?' in translated query (probably, caused by unmapped blank node): #{sql.gsub(/\s+/, ' ')};"

    substitute_literals(sql)
  end

  private

  # replace blank node names with bindings
  #
  def bind_blank_nodes(sql)
    sql.gsub(BN_SCAN) {|node| @sql_mapper.bind(node) }
  end
end


class SquishAssert < SquishQuery
  def initialize(config, query)
    @config = config
    super(@config, query)

    if 'UPDATE' == @key
      @insert = ''
      @update = @nodes

    elsif 'INSERT' == @key and @nodes =~ /\A\s*(.*?)\s*(?:\bUPDATE\b\s*(.*?))?\s*\z/
      @insert, @update = $1, $2.to_s

    else
      raise ProgrammingError,
        "Wrong query type: INSERT or UPDATE expected instead of " + @key
    end

    @insert = @insert.split(/\s*,\s*/).each {|s|
      s =~ BN or raise ProgrammingError,
        "Blank node expected in INSERT section instead of '#{s}'"
    }

    @update = @update.empty? ? {} : Hash[*@update.split(/\s*,\s*/).collect {|s|
      s.split(/\s*=\s*/)
    }.each {|node, value|
      node =~ BN or raise ProgrammingError,
        "Blank node expected on the left side of UPDATE assignment instead of '#{bn}'"
      validate_expression(value)
    }.flatten!]
  end

  def run(db, params={})
    values = resource_values(db, params)

    statements = []
    alias_positions.each do |alias_, clauses|
      statement = SquishAssertStatement.new(clauses, values)
      statements.push(statement) if statement.action
    end
    SquishAssertStatement.run_ordered_statements(db, statements)

    return @insert.collect {|node| values[node].value }
  end

  attr_reader :insert, :update

  private

  def resource_values(db, params)
    values = {}
    @sql_mapper.nodes.each do |node, n|
      new = false

      if node =~ INTERNAL   # internal resource
        value = $1.to_i   # resource id

      elsif node =~ PARAMETER
        value = get_parameter_value($1, params)

      elsif node =~ LITERAL
        value = @strings[$1.to_i]

      elsif node =~ BN
        subject_position = n[:positions].select {|p| :subject == p[:role] }.first

        if subject_position.nil?   # blank node occuring only in object position
          value = @update[node] or raise ProgrammingError,
            %{Blank node #{node} is undefined (drop it or set its value in UPDATE section)}
          value = expression_value(value, params)

        else   # resource blank node
          unless @insert.include?(node)
            s = SquishSelect.new(
              @config, {
                :nodes => [node],
                :pattern => subgraph(node),
                :strings => @strings
              }
            )
            debug { 'resource_values ' + db[s.to_sql, params].select_sql }
            found = db.fetch(s.to_sql, params).first
          end

          if found
            value = found.values.first

          else
            table = @sql_mapper.clauses[ subject_position[:clause] ][:map].table
            value = db[:resource].insert(:label => table)
            debug { 'resource_values ' + db[:resource].insert_sql(:label => table) }
            new = true unless 'resource' == table
          end
        end

      else   # external resource
        uriref = { :uriref  => true, :label   => node }
        found = db[:resource].filter(uriref).first
        if found
          value = found[:id]
        else
          value = db[:resource].insert(uriref)
          debug { 'resource_values ' + db[:resource].insert_sql(uriref) }
        end
      end

      debug { 'resource_values ' + node + ' = ' + value.inspect }
      v = SquishAssertValue.new(value, new, @update.has_key?(node))
      values[node] = v
    end

    debug { 'resource_values ' + 'resource_values ' + values.inspect }
    values
  end

  def alias_positions
    a = {}
    @sql_mapper.clauses.each_with_index do |clause, i|
      a[ clause[:alias] ] ||= []
      a[ clause[:alias] ].push(clause)
    end
    a
  end

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
end


class SquishAssertValue
  def initialize(value, new, updated)
    @value = value
    @new = new
    @updated = updated
  end

  attr_reader :value

  # true if node was inserted into resource during value generation and a
  # corresponding record should be inserted into an internal resource table
  # later
  #
  def new?
    @new
  end

  # true if the node value is set in the UPDATE section of the Squish statement
  #
  def updated?
    @updated
  end
end


class SquishAssertStatement
  include Debug

  def initialize(clauses, values)
    @key_node = clauses.first[:subject][:node]
    @table = clauses.first[:map].table.to_sym

    key = values[@key_node]

    @params = {}
    @references = []
    clauses.each do |clause|
      node = clause[:object][:node]
      v = values[node]

      if key.new? or v.updated?
        field = clause[:object][:field]
        @params[field.to_sym] = v.value

        # when subproperty value is updated, update the qualifier as well
        map = clause[:map]
        if map.subproperty_of
          @params[ RdfPropertyMap.qualifier_field(field).to_sym ] = values[map.property].value
        elsif map.superproperty?
          @params[ RdfPropertyMap.qualifier_field(field).to_sym ] = nil
        end

        @references.push(node) if v.new?
      end
    end

    if key.new? and @table != :resource
      # when id is inserted, insert_resource() trigger does nothing
      @action = :insert
      @params[:id] = key.value

    elsif not @params.empty?
      @action = :update
      @filter = {:id => key.value}
    end

    debug { 'SquishAssertStatement ' + self.inspect }
  end

  attr_reader :key_node, :references, :action

  def run(db)
    if @action
      ds = db[@table]
      ds = ds.filter(@filter) if @filter
      debug { :insert == @action ? ds.insert_sql(@params) : ds.update_sql(@params) }
      ds.send(@action, @params)
    end
  end

  # make sure mutually referencing records are inserted in the right order
  #
  def SquishAssertStatement.run_ordered_statements(db, statements)
    statements = statements.sort_by {|s| s.references.size }
    inserted = []

    progress = true
    until statements.empty? or not progress
      progress = false

      0.upto(statements.size - 1) do |i|
        s = statements[i]
        if (s.references - inserted).empty?
          s.run(db)
          inserted.push(s.key_node)
          statements.delete_at(i)
          progress = true
          break
        end
      end
    end

    statements.empty? or raise ProgrammingError,
      "Failed to resolve mutual references of inserted resources: " +
      statements.collect {|s| s.key_node + ' -- ' + s.references.join(', ') }.join('; ')
  end
end

end
