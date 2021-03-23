require 'json'

package = JSON.parse(File.read(File.join(__dir__, 'package.json')))


Pod::Spec.new do |s|
  s.name         = package['name']
  s.version      = package['version']
  s.summary      = package['description']
  s.license      = package['license']

  s.authors      = package['author']
  s.homepage     = package['homepage']
  s.platform     = :ios, "9.0"

  s.source = { :git => "https://github.com/axsy-dev/react-native-sqlcipher-storage.git"}
  s.source_files  = "ios/*.{h,m}"

  s.xcconfig = {'GCC_PREPROCESSOR_DEFINITIONS' => '$(inherited) SQLITE_HAS_CODEC=1' }
  s.dependency 'SQLCipher', '~>4.0'
  s.dependency 'React'
end
