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

module Graffiti

# Map of an internal RDF property into relational storage
#
class RdfPropertyMap

  # special qualifier map
  #
  # ' ' is added to the property name to make sure it can't clash with any
  # valid property uriref
  #
  def RdfPropertyMap.qualifier_property(property, type = 'subproperty')
    property + ' ' + type
  end

  # special qualifier field
  #
  def RdfPropertyMap.qualifier_field(field, type = 'subproperty')
    field + '_' + type
  end

  def initialize(property, table, field)
    # fixme: support ambiguous mappings
    @property = property
    @table = table
    @field = field
  end

  # expanded uriref of the mapped property
  #
  attr_reader :property

  # name of the table into which the property is mapped (property domain is an
  # internal resource class mapped into this table)
  #
  attr_reader :table

  # name of the field into which the property is mapped
  #
  # if property range is not a literal, the field is a reference to the
  # resource table
  #
  attr_reader :field

  # expanded uriref of the property which this property is a subproperty of
  #
  # if set, this property maps into the same table and field as its
  # superproperty, and is qualified by an additional field named
  # <field>_subproperty which refers to a uriref resource holding uriref of
  # this subproperty
  #
  attr_accessor :subproperty_of

  attr_writer :superproperty

  # set to +true+ if this property has subproperties
  #
  def superproperty?
    @superproperty or false
  end

  # name of transitive closure table for a transitive property
  #
  # the format of a transitive closure table is:
  #
  #  - 'resource' field refers to the subject resource id
  #  - '<field>' property field and '<field>_subproperty' qualifier field (in
  #    case of subproperty) have the same name as in the main table
  #  - 'distance' field holds the distance from subject to object in the RDF
  #    graph
  #
  # the transitive closure table is automatically updated by a trigger on every
  # update of the main table
  #
  attr_accessor :transitive_closure
end

end
