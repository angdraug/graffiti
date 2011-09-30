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

require 'delegate'
require 'uri/common'
require 'graffiti/rdf_property_map'
require 'graffiti/squish'

module Graffiti

class SqlNodeBinding
  def initialize(table_alias, field)
    @alias = table_alias
    @field = field
  end

  attr_reader :alias, :field

  def to_s
    @alias + '.' + @field
  end

  alias :inspect :to_s

  def eql?(binding)
    @alias == binding.alias and @field == binding.field
  end

  alias :'==' :eql?

  def hash
    self.to_s.hash
  end
end


class SqlExpression < DelegateClass(Array)
  def initialize(*parts)
    super parts
  end

  def to_s
    '(' << self.join(' ') << ')'
  end

  alias :to_str :to_s

  def traverse(&block)
    self.each do |part|
      case part
      when SqlExpression
        part.traverse(&block)
      else
        yield
      end
    end
  end

  def rebind!(rebind, &block)
    self.each_with_index do |part, i|
      case part
      when SqlExpression
        part.rebind!(rebind, &block)
      when SqlNodeBinding
        if rebind[part]
          self[i] = rebind[part] 
          yield part if block_given?
        end
      end
    end
  end

  alias :eql? :'=='

  def hash
    self.to_s.hash
  end
end


# Transform RDF query pattern graph into a relational join expression.
#
class SqlMapper
  include Debug

  def initialize(config, pattern, negative = [], optional = [], global_filter = '')
    @config = config
    @global_filter = global_filter

    check_graph(pattern)
    negative.empty? or check_graph(pattern + negative)
    optional.empty? or check_graph(pattern + optional)

    map_predicates(pattern, negative, optional)
    transform
    generate_tables_and_conditions

    @jc = @aliases = @ac = @global_filter = nil
  end

  # map clause position to table, field, and table alias
  #
  #  position => {
  #    :subject => {
  #      :node => node,
  #      :field => field
  #    },
  #    :object => {
  #      :node => node,
  #      :field => field
  #    },
  #    :map => RdfPropertyMap,
  #    :bind_mode => < :must_bind | :may_bind | :must_not_bind >,
  #    :alias => alias
  #  }
  #
  attr_reader :clauses

  # map node to list of positions in clauses
  #
  #  node => {
  #    :positions => [
  #      { :clause => position, :role => < :subject | :object > }
  #    ],
  #    :bind_mode => < :must_bind | :may_bind | :must_not_bind >,
  #    :colors => { color1 => bind_mode1, ... },
  #    :ground => < true | false >
  #  }
  #
  attr_reader :nodes

  # list of tables for FROM clause of SQL query
  attr_reader :from

  # conditions for WHERE clause of SQL query
  attr_reader :where

  # return node's binding, raise exception if the node isn't bound
  #
  def bind(node)
    (@nodes[node] and @bindings[node] and (binding = @bindings[node].first)
    ) or raise ProgrammingError,
      "Node '#{node}' is not bound by the query pattern"

    @nodes[node][:positions].each do |p|
      if :object == p[:role] and @clauses[ p[:clause] ][:map].subproperty_of

        property = @clauses[ p[:clause] ][:map].property
        return %{select_subproperty(#{binding}, #{bind(property)})}
      end
    end

    binding
  end

  private

  # Check whether pattern is not a disjoint graph (all nodes are
  # undirectionally reachable from one node).
  #
  def check_graph(pattern)
    nodes = pattern.transpose[1, 2].flatten.uniq   # all nodes

    seen = [ nodes.shift ]
    found_more = true

    while found_more and not nodes.empty?
      found_more = false

      pattern.each do |predicate, subject, object|

        if seen.include?(subject) and nodes.include?(object)
          seen.push(object)
          nodes.delete(object)
          found_more = true

        elsif seen.include?(object) and nodes.include?(subject)
          seen.push(subject)
          nodes.delete(subject)
          found_more = true
        end
      end
    end

    nodes.empty? or raise ProgrammingError, "Query pattern is a disjoint graph"
  end

  # Stage 1: Predicate Mapping (storage-impl.txt).
  #
  def map_predicates(pattern, negative, optional)
    @nodes = {}
    @clauses = []

    map_pattern(pattern, :must_bind)
    map_pattern(negative, :must_not_bind)
    map_pattern(optional, :may_bind)

    @color_counter = @must_bind_nodes = nil

    refine_ambiguous_properties

    debug do
      @nodes.each do |node, n|
        debug %{#{node}: #{n[:bind_mode]} #{n[:colors].inspect}}
      end
    end
  end

  # Label every connected component of the pattern with a different color.
  #
  # Pattern clause positions:
  #
  #  0. predicate
  #  1. subject
  #  2. object
  #  3. filter
  #
  # Returns hash of node colors.
  #
  # Implements the {Two-pass Connected Component Labeling algorithm}
  # [http://en.wikipedia.org/wiki/Connected_Component_Labeling#Two-pass]
  # with an added special case to exclude _alien_nodes_ from neighbor lists.
  #
  # The special case ensures that parts of a may-bind or must-not-bind
  # subpattern that are only connected through a must-bind node do not connect.
  #
  def label_pattern_components(pattern, alien_nodes, augment_alien_nodes = false)
    return {} if pattern.empty?

    color = {}
    color_eq = []   # [ [ smaller, larger ], ... ]
    nodes = pattern.transpose[1, 2].flatten.uniq
    alien_nodes_here = nodes & alien_nodes

    @color_counter = @color_counter ? @color_counter.next : 0
    color[ nodes[0] ] = @color_counter

    # first pass
    1.upto(nodes.size - 1) do |i|
      node = nodes[i]

      pattern.each do |predicate, subject, object, filter|
        if node == subject
          neighbor = object
        elsif node == object
          neighbor = subject
        end
        next if neighbor.nil? or color[neighbor].nil? or
          alien_nodes_here.include?(neighbor)

        if color[node].nil?
          color[node] = color[neighbor]
        elsif color[node] != color[neighbor]   # record color equivalence
          color_eq |= [ [ color[node], color[neighbor] ].sort ]
        end
      end

      color[node] ||= (@color_counter += 1)
    end

    # second pass
    nodes.each do |node|
      while eq = color_eq.rassoc(color[node])
        color[node] = eq[0]
      end
    end

    alien_nodes.push(*nodes).uniq! if augment_alien_nodes

    color
  end

  def map_pattern(pattern, bind_mode = :must_bind)
    pattern = pattern.dup
    @must_bind_nodes ||= []
    color = label_pattern_components(pattern, @must_bind_nodes, :must_bind == bind_mode)

    pattern.each do |predicate, subject, object, filter, transitive|

      # validate the triple
      predicate =~ URI::URI_REF or raise ProgrammingError,
        "Valid uriref expected in predicate position instead of '#{predicate}'"

      [subject, object].each do |node|
        node =~ SquishQuery::INTERNAL or
          node =~ SquishQuery::BN or
          node =~ URI::URI_REF or
          raise ProgrammingError,
            "Resource or blank node name expected instead of '#{node}'"
      end

      # list of possible mappings into internal tables
      map = @config.map[predicate]

      if transitive and map.transitive_closure.nil?
        raise ProgrammingError,
          "No transitive closure is defined for #{predicate} property"
      end

      if map and
        (subject =~ SquishQuery::BN or
         subject =~ SquishQuery::INTERNAL or
         subject =~ SquishQuery::PARAMETER or
         'resource' == map.table)
        # internal predicate and subject is mappable to resource table

        i = clauses.size

        @clauses[i] = {
          :subject => [ { :node => subject, :field => 'id' } ],
          :object  => [ { :node => object, :field => map.field } ],
          :map => map,
          :transitive => transitive,
          :bind_mode => bind_mode
        }
        @clauses[i][:filter] = SqlExpression.new(filter) if filter

        [subject, object].each do |node|
          if @nodes[node]
            @nodes[node][:bind_mode] =
              stronger_bind_mode(@nodes[node][:bind_mode], bind_mode)
          else
            @nodes[node] = { :positions => [], :bind_mode => bind_mode, :colors => {} }
          end

          # set of node colors, one for each bind_mode
          @nodes[node][:colors][ color[node] ] = bind_mode
        end

        # reverse mapping of the node occurences
        @nodes[subject][:positions].push( { :clause => i, :role => :subject } )
        @nodes[object][:positions].push( { :clause => i, :role => :object } )

        if superp = map.subproperty_of
          # link subproperty qualifier into the pattern
          pattern.push(
            [RdfPropertyMap.qualifier_property(superp), subject, predicate])
          color[predicate] = color[object]

          # no need to ground both subproperty and superproperty
          @nodes[object][:ground] = true
        end

      else
        # assume reification for unmapped predicates:
        #
        #            | (rdf::predicate ?_stmt_#{i} p)
        # (p s o) -> | (rdf::subject ?_stmt_#{i} s)
        #            | (rdf::object ?_stmt_#{i} o)
        #
        rdf = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
        stmt = "?_stmt_#{i}"
        pattern.push([rdf + 'predicate', stmt, predicate],
                     [rdf + 'subject', stmt, subject],
                     [rdf + 'object', stmt, object])
        color[stmt] = color[predicate] = color[object]
      end
    end
  end

  # Select strongest of the two bind modes, in the following order of
  # preference:
  #
  #  :must_bind -> :must_not_bind -> :may_bind
  # 
  def stronger_bind_mode(mode1, mode2)
    if mode1 != mode2 and (:must_bind == mode2 or :may_bind == mode1)
      mode2
    else
      mode1
    end
  end

  # If a node can be mapped to more than one [table, field] pair, see if it can
  # be refined based on other occurences of this node in other query clauses.
  #
  def refine_ambiguous_properties
    @nodes.each_value do |n|
      map = n[:positions]

      map.each_with_index do |p, i|
        big = @clauses[ p[:clause] ][ p[:role] ]
        next if big.size <= 1   # no refining needed

        debug { n + ': ' + big.inspect }

        (i + 1).upto(map.size - 1) do |j|
          small_p = map[j]
          small = @clauses[ small_p[:clause] ][ small_p[:role] ]

          refined = big & small
          if refined.size > 0 and refined.size < big.size

            # refine the node...
            @clauses[ p[:clause] ][ p[:role] ] = big = refined

            # ...and its pair
            @clauses[ p[:clause] ][ opposite_role(p[:role]) ].collect! {|pair|
              refined.assoc(pair[0]) ? pair : nil
            }.compact!   
          end
        end
      end
    end

    # drop remaining ambiguous mappings
    # todo: split query for ambiguous mappings
    @clauses.each do |clause|
      next if clause.nil?   # means it was reified
      clause[:subject] = clause[:subject].first
      clause[:object] = clause[:object].first
    end
  end

  def opposite_role(role)
    :subject == role ? :object : :subject
  end

  # Return current value of alias counter, remember which table it was assigned
  # to, and increment the counter.
  #
  def next_alias(table, node, bind_mode = @nodes[node][:bind_mode])
    @ac ||= 'a'
    @aliases ||= {}

    a = @ac.dup
    @aliases[a] = {
      :table => table,
      :node => node,
      :bind_mode => bind_mode,
      :filter => []
    }

    @ac.next!
    return a
  end

  def define_relation_aliases
    @nodes.each do |node, n|

      positions = n[:positions]

      # go through all clauses with this node in subject position
      positions.each_with_index do |p, i|
        next if :subject != p[:role] or @clauses[ p[:clause] ][:alias]

        clause = @clauses[ p[:clause] ]
        map = clause[:map]
        table = clause[:transitive] ? map.transitive_closure : map.table

        # see if we've already mapped this node to the same table before
        0.upto(i - 1) do |j|
          similar_clause = @clauses[ positions[j][:clause] ]

          if similar_clause[:alias] and
            similar_clause[:map].table == table and
            similar_clause[:map].field != map.field
            # same node, same table, different field -> same alias

            clause[:alias] = similar_clause[:alias]
            break
          end
        end

        if clause[:alias].nil?
          clause[:alias] =
            if clause[:transitive]
              # transitive clause bind mode overrides a stronger node bind mode
              #
              # fixme: generic case for multiple aliases per node
              next_alias(table, node, clause[:bind_mode])
            else
              next_alias(table, node)
            end
        end
      end
    end   # optimize: unnecessary aliases are generated
  end

  def update_alias_filters
    @clauses.each do |c|
      if c[:filter]
        @aliases[ c[:alias] ][:filter].push(c[:filter])
      end
    end
  end

  # Stage 2: Relation Aliases and Join Conditions (storage-impl.txt).
  #
  # Result is map of aliases in @aliases and list of join conditions in @jc.
  #
  def transform
    define_relation_aliases
    update_alias_filters

    # [ [ binding1, binding2 ], ... ]
    @jc = []
    @bindings = {}

    @nodes.each do |node, n|
      positions = n[:positions]

      # node binding
      first = positions.first
      clause = @clauses[ first[:clause] ]
      a = clause[:alias]
      binding = SqlNodeBinding.new(a, clause[ first[:role] ][:field])
      @bindings[node] = [ binding ]

      # join conditions
      1.upto(positions.size - 1) do |i|
        p = positions[i]
        clause2 = @clauses[ p[:clause] ]
        binding2 = SqlNodeBinding.new(clause2[:alias], clause2[ p[:role] ][:field])

        unless @bindings[node].include?(binding2)
          @bindings[node].push(binding2)
          @jc.push([binding, binding2, node])
          n[:ground] = true
        end
      end

      # ground non-blank nodes
      if node !~ SquishQuery::BN

        if node =~ SquishQuery::INTERNAL   # internal resource id
          @aliases[a][:filter].push SqlExpression.new(binding, '=', $1)

        elsif node =~ SquishQuery::PARAMETER or node =~ SquishQuery::LITERAL
          @aliases[a][:filter].push SqlExpression.new(binding, '=', node)

        elsif node =~ URI::URI_REF  # external resource uriref

          r = nil
          positions.each do |p|
            next unless :subject == p[:role]

            c = @clauses[ p[:clause] ]
            if 'resource' == c[:map].table
              r = c[:alias]   # reuse existing mapping to resource table
              break
            end
          end

          if r.nil?
            r = next_alias('resource', node)
            r_binding = SqlNodeBinding.new(r, 'id')
            @bindings[node].unshift(r_binding)
            @jc.push([ binding, r_binding, node ])
          end

          @aliases[r][:filter].push SqlExpression.new(
            SqlNodeBinding.new(r, 'uriref'), '=', "'t'", 'AND',
            SqlNodeBinding.new(r, 'label'), '=', %{'#{node}'})

        else
          raise RuntimeError,
            "Invalid node '#{node}' should never occur at this point"
        end

        n[:ground] = true
      end
    end

    debug do
      @aliases.each {|alias_name, a| debug %{#{alias_name}: #{a.inspect}} }
      @jc.each {|jc| debug jc.inspect }
    end
  end

  # Produce SQL FROM and WHERE clauses from results of transform().
  #
  def generate_tables_and_conditions
    main_path, seen = jc_subgraph_path(:must_bind)
    debug { main_path.inspect }

    main_path and not main_path.empty? or raise RuntimeError,
      'Failed to find table aliases for main query'

    @where = ground_dangling_blank_nodes(main_path)

    joins = ''
    subquery_count = 'a'

    [ :must_not_bind, :may_bind ].each do |bind_mode|
      loop do
        sub_path, new = jc_subgraph_path(bind_mode, seen)
        break if sub_path.nil? or sub_path.empty?

        debug { sub_path.inspect }

        sub_query, sub_join = sub_path.partition {|a,| main_path.assoc(a).nil? }
        # fixme: make sure that sub_join is not empty

        if 1 == sub_query.size
          # simplified case: join single table directly without a subquery
          join_alias, = sub_query.first
          a = @aliases[join_alias]
          join_target = a[:table]
          join_conditions = jc_path_to_join_conditions(sub_join) + a[:filter]

        else
          # left join subquery to the main query
          join_alias = '_subquery_' << subquery_count
          subquery_count.next!

          sub_join = subquery_jc_path(sub_join, join_alias)
          rebind = rebind_subquery(sub_path, join_alias)
          select_nodes = subquery_select_nodes(rebind, main_path, sub_join)

          join_conditions = jc_path_to_join_conditions(sub_join, rebind,
                                                       select_nodes)

          select_nodes = select_nodes.keys.collect {|b|
            b.to_s << ' AS ' << rebind[b].field
          }.join(', ')

          tables, conditions = jc_path_to_tables_and_conditions(sub_path)

          join_target = "(\nSELECT #{select_nodes}\nFROM #{tables}"
          join_target << "\nWHERE " << conditions unless conditions.empty?
          join_target << "\n)"
          join_target.gsub!(/\n(?!\)\z)/, "\n    ")
        end

        joins << ("\nLEFT JOIN " + join_target + ' AS ' + join_alias + ' ON ' +
                  join_conditions.uniq.join(' AND '))

        if :must_not_bind == bind_mode
          left_join_is_null(main_path, sub_join)
        end
      end
    end

    @from, main_where = jc_path_to_tables_and_conditions(main_path)

    @from << joins

    @where.push('(' + main_where + ')') unless main_where.empty?
    @where.push('(' + @global_filter + ')') unless @global_filter.empty?
    @where = @where.join("\nAND ")
  end

  # Produce a subgraph path through join conditions linking all aliases with
  # given _bind_mode_ that form a same-color connected component of the join
  # conditions graph and weren't processed yet:
  #
  #  path = [ [start, []], [ next, [ jc, ... ] ], ... ]
  #
  # Update _seen_ hash for all aliases included in the produced path.
  #
  def jc_subgraph_path(bind_mode, seen = {})
    start = find_alias(bind_mode, seen)
    return nil if start.nil?

    new = {}
    new[start] = true
    path = [ [start, []] ]
    colors = @nodes[ @aliases[start][:node] ][:colors].keys

    loop do   # while we can find more connecting joins of the same color
      join_alias = nil

      @jc.each do |jc|
        # use cases:
        #  - seen is empty (composing the must-bind join)
        #  - seen is not empty (composing a subquery)

        next if (colors & @nodes[ jc[2] ][:colors].keys).empty?

        0.upto(1) do |i|
          a_seen = jc[i].alias
          a_next = jc[1-i].alias

          if not new[a_next] and (
            ((new[a_seen] or seen[a_seen]) and
             (@aliases[a_next][:bind_mode] == bind_mode)
             # connect an untouched node of matching bind mode
            ) or (
              new[a_seen] and seen[a_next] and
              # connect subquery to the rest of the query...
              @aliases[a_seen][:bind_mode] == bind_mode
              # ...but only go one step deep
            ))

            join_alias = a_next
            break
          end
        end

        break if join_alias
      end

      break if join_alias.nil?

      # join it to all seen aliases
      join_on = @jc.find_all do |jc|
        a1, a2 = jc[0, 2].collect {|b| b.alias }
        (new[a1] and a2 == join_alias) or (new[a2] and a1 == join_alias)
      end

      new[join_alias] = true
      path.push([join_alias, join_on])
    end

    seen.merge!(new)
    [ path, new ]
  end

  def find_alias(bind_mode, seen = {})
    @aliases.each do |alias_name, a|
      next if seen[alias_name] or a[:bind_mode] != bind_mode
      return alias_name
    end

    nil
  end

  # Ground all must-bind blank nodes that weren't ground elsewhere to an
  # existential quantifier.
  #
  def ground_dangling_blank_nodes(main_path)
    conditions = []
    ground_nodes = @global_filter.scan(SquishQuery::BN_SCAN)

    @nodes.each do |node, n|
      next if (n[:ground] or ground_nodes.include?(node))

      expression =
        case n[:bind_mode]
        when :must_bind
          'IS NOT NULL'
        when :must_not_bind
          'IS NULL'
        else
          next
        end

      @bindings[node].each do |binding|
        if main_path.assoc(binding.alias)
          conditions.push SqlExpression.new(binding, expression)
          break
        end
      end
    end

    conditions
  end

  # Join a subquery to the main query: for each alias shared between the two,
  # link 'id' field of the corresponding table within and outside the subquery.
  # If no node is bound to the 'id' field, create a virtual node bound to it,
  # so that it can be rebound by rebind_subquery().
  #
  def subquery_jc_path(sub_join, join_alias)
    sub_join.empty? and raise ProgrammingError,
      "Unexpected empty subquery, check your RDF storage configuration"
    # fixme: reify instead of raising an exception

    sub_join.transpose[0].uniq.collect do |a|
      binding = SqlNodeBinding.new(a, 'id')

      exists = false
      @nodes.each do |node, n|
        if @bindings[node].include?(binding)
          exists = true
          break
        end
      end

      unless exists
        node = '?' + join_alias + '_' + a
        @nodes[node] = { :ground => true }
        @bindings[node] = [ binding ]
      end

      [ a, [[ binding, binding ]] ]
    end
  end

  # Generate a hash that maps all bindings that's been wrapped inside the
  # _sub_query_ (a jc path, see jc_subquery_path()) to rebound bindings based
  # on the _join_alias_ so that they may still be used in the main query.
  #
  def rebind_subquery(sub_path, join_alias)
    rebind = {}
    field_count = 'a'

    wrapped = {}
    sub_path.each {|a,| wrapped[a] = true }

    @nodes.each do |node, n|
      @bindings[node].each do |b|
        if wrapped[b.alias] and rebind[b].nil?
          field = '_field_' << field_count
          field_count.next!
          rebind[b] = SqlNodeBinding.new(join_alias, field)
        end
      end
    end

    rebind
  end

  # Go through global filter, filters in the main query, and join conditions
  # attaching the subquery to the main query, rebind the bindings for nodes
  # wrapped inside the subquery, and return a hash with keys for all bindings
  # that should be selected from the subquery.
  #
  def subquery_select_nodes(rebind, main_path, sub_join)
    select_nodes = {}

    # update the global filter
    @nodes.each do |node, n|
      if r = rebind[ @bindings[node].first ]
        @global_filter.gsub!(/#{Regexp.escape(node)}\b/) do
          select_nodes[ @bindings[node].first ] = true
        r.to_s
        end
      end
    end

    # update filters in the main query
    main_path.each do |a,|
      next if sub_join.assoc(a)

      @aliases[a][:filter].each do |f|
      f.rebind!(rebind) do |b|
        select_nodes[b] = true
      end
      end
    end

    # update the subquery join path
    sub_join.each do |a, jcs|
      jcs.each do |jc|
        select_nodes[ jc[0] ] = true
        jc[1] = rebind[ jc[1] ]
      end
    end

    # fixme: update main SELECT list
    select_nodes
  end

  # Transform jc path (see jc_subgraph_path()) into a list of join conditions.
  # If _rebind_ and _select_nodes_ hashes are defined, conditions will be
  # rebound accordingly, and _select_nodes_ will be updated to include bindings
  # used in the conditions.
  #
  def jc_path_to_join_conditions(jc_path, rebind = nil, select_nodes = nil)
    conditions = []

    jc_path.each do |a, jcs|
      jcs.each do |b1, b2, n|
        conditions.push SqlExpression.new(b1, '=', b2)
      end
    end

    conditions.empty? and raise RuntimeError,
      "Failed to join subquery to the main query"

    conditions
  end

  # Generate FROM and WHERE clauses from a jc path (see jc_subgraph_path()).
  #
  def jc_path_to_tables_and_conditions(path)
    first, = path[0]
    a = @aliases[first]

    tables = a[:table] + ' AS ' + first
    conditions = a[:filter]

    path[1, path.size - 1].each do |join_alias, join_on|
      a = @aliases[join_alias]

      tables <<
        %{\nINNER JOIN #{a[:table]} AS #{join_alias} ON } <<
        (
          join_on.collect {|b1, b2| SqlExpression.new(b1, '=', b2) } +
          a[:filter]
        ).uniq.join(' AND ')
    end

    [ tables, conditions.uniq.join("\nAND ") ]
  end

  # Find and declare as NULL key fields of a must-not-bind subquery.
  #
  def left_join_is_null(main_path, sub_join)
    sub_join.each do |a, jcs|
      jcs.each do |jc|
        0.upto(1) do |i|
          if main_path.assoc(jc[i].alias).nil?
            @where.push SqlExpression.new(jc[i], 'IS NULL')
            break
          end
        end
      end
    end
  end
end

end
