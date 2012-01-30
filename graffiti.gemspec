Gem::Specification.new do |spec|
  spec.name        = 'graffiti'
  spec.version     = '2.1'
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
  spec.files       = `git ls-files`.split "\n"
  spec.test_files  = Dir['test/ts_*.rb']
  spec.license     = 'GPL3+'
  spec.add_dependency('syncache')
  spec.add_dependency('sequel')
end
