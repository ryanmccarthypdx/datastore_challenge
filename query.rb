#!/usr/local/bin/ruby

# usage: $ ruby query.rb -s TITLE,REV,DATE -o DATE,TITLE

require 'pathname'
require 'bundler'
require './lib/query.rb'

Bundler.require(:default)

Query.new(ARGV).perform.each do |r|
  puts r.join(',')
end
