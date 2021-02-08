Gem::Specification.new do |spec|
  spec.name        = 'graffiti'
  spec.version     = '2.3.2'
  spec.author      = 'Dmitry Borodaenko'
  spec.email       = 'angdraug@debian.org'
  spec.homepage    = 'https://github.com/angdraug/graffiti'
  spec.summary     = 'Relational RDF store for Ruby'
  spec.description = <<-EOF
Graffiti is an RDF store based on dynamic translation of RDF queries into SQL.
Graffiti allows one to map any relational database schema into RDF semantics
and vice versa, to store any RDF data in a relational database.

Graffiti uses Sequel to connect to database backend and provides a DBI-like
interface to run RDF queries in Squish query language from Ruby applications.
    EOF
  spec.files       = %w(COPYING ChangeLog.mtn README.rdoc TODO
                        setup.rb Rakefile graffiti.gemspec) +
                     Dir['{lib,test}/**/*.rb'] +
                     Dir['doc/**/*.{txt,tex,yaml,sql,svg}']
  spec.test_files  = Dir['test/ts_*.rb']
  spec.license     = 'GPL-3.0+'
  spec.add_dependency 'syncache'
  spec.add_dependency 'sequel'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'test-unit'
  spec.add_development_dependency 'sqlite3'
end
