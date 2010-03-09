
begin
  require 'bones'
rescue LoadError
  abort '### Please install the "bones" gem ###'
end

ensure_in_path 'lib'
require 'mysqldiff'

task :default => 'test:run'
task 'gem:release' => 'test:run'

Bones {
  name  'mysqldiff'
  authors  'Stephen Walker'
  email  'swalker@walkertek.com'
  url  'http://www.walkertek.com'
  version  Mysqldiff::VERSION
}

# EOF
