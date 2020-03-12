#!/usr/local/bin/ruby

# basically a rake task for taking file data and giving to ingester
# usage: $ ruby ingest.rb ./sample_files/manual.psv

require 'pathname'
require './lib/ingester.rb'
require 'bundler'

Bundler.require(:default)

user_path = ARGV[0] || "(none given)"

unless Pathname.new(user_path).exist?
  p "ERR: File not found: #{user_path}"
  return
end

Ingester.new(user_path).ingest

puts 'done!'
