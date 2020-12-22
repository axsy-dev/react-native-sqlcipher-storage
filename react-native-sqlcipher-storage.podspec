require 'json'

package = JSON.parse(File.read(File.join(__dir__, 'package.json')))

Pod::Spec.new do |s|
  s.name     = "react-native-sqlcipher-storage"
  s.version  = package['version']
  s.summary  = package['description']
  s.license  = package['license']
  s.homepage = "https://github.com/axsy-dev/react-native-sqlcipher-storage"
  s.authors   = "axsy-dev, andpor"
  s.source   = { :git => "https://github.com/axsy-dev/react-native-sqlcipher-storage.git" }

  s.ios.deployment_target = '8.0'
  s.osx.deployment_target = '10.10'

  s.preserve_paths = 'README.md', 'LICENSE', 'package.json', 'sqlite.js'
  s.source_files   = "src/{common,ios}/*.{h,m}"

  s.dependency 'React'
  s.library = 'sqlite3'
end