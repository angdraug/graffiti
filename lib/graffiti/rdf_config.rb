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

require 'graffiti/rdf_property_map'

module Graffiti

# Configuration of relational RDF storage (see examples)
#
class RdfConfig
  def initialize(config)
    @ns = config['ns']

    @map = {}

    config['map'].each_pair do |p, m|
      table, field = m.to_a.first
      p = ns_expand(p)
      @map[p] = RdfPropertyMap.new(p, table, field)
    end

    if config['subproperties'].kind_of? Hash
      config['subproperties'].each_pair do |p, subproperties|
        p = ns_expand(p)
        map = @map[p] or raise RuntimeError,
          "Incorrect RDF storage configuration: superproperty #{p} must be mapped"
        map.superproperty = true

        qualifier = RdfPropertyMap.qualifier_property(p)
        @map[qualifier] = RdfPropertyMap.new(
          qualifier, map.table, RdfPropertyMap.qualifier_field(map.field))

        subproperties.each do |subp|
          subp = ns_expand(subp)
          @map[subp] = RdfPropertyMap.new(subp, map.table, map.field)
          @map[subp].subproperty_of = p
        end
      end
    end

    if config['transitive_closure'].kind_of? Hash
      config['transitive_closure'].each_pair do |p, table|
        @map[ ns_expand(p) ].transitive_closure = table

        if config['subproperties'].kind_of?(Hash) and config['subproperties'][p]
          config['subproperties'][p].each do |subp|
            @map[ ns_expand(subp) ].transitive_closure = table
          end
        end
      end
    end
  end

  # hash of namespaces
  attr_reader :ns

  # map internal property names with expanded namespaces to RdfPropertyMap
  # objects
  #
  attr_reader :map

  def ns_expand(p)
    p and p.sub(/\A(\S+?)::/) { @ns[$1] }
  end
end

end
