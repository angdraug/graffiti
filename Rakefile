# Graffiti RDF Store
# (originally written for Samizdat project)
#
#   Copyright (c) 2002-2012, 2016  Dmitry Borodaenko <angdraug@debian.org>
#
#   This program is free software.
#   You can distribute/modify this program under the terms of
#   the GNU General Public License version 3 or later.
#
# see doc/rdf-storage.txt for introduction and Graffiti Squish definition;
# see doc/storage-impl.txt for explanation of implemented algorithms
#
# vim: et sw=2 sts=2 ts=8 tw=0

require "rake"

task :default => :test

task :test do
  sh %{#{FileUtils::RUBY} -I. -Ilib test/ts_graffiti.rb}
end
