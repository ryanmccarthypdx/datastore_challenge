require 'pstore'
require 'pathname'

class PstoreConnection
  attr_reader :store

  def initialize(file_path, init_hash = {})
    if ENV['environment'] == 'test' && !file_path.include?('/test/') # never mix up test data and 'prod' data
      file_path.gsub!('data/', 'data/test/')
    end
    unless Pathname.new(file_path).exist?
      FileUtils.touch(file_path)
      @store = PStore.new(file_path)
      seed_db(init_hash)
    else
      @store = PStore.new(file_path)
    end
    store.ultra_safe = true
  end

  def set(key, value)
    store.transaction do
      store[key] = value
    end
  end

  def set_multiple_in_single_transaction(hash)
    store.transaction do
      hash.each_pair do |k,v|
        store[k] = v
      end
    end
  end

  def get(key)
    store.transaction(true) do
      store[key]
    end
  end

  def keys
    store.transaction(true) do
      store.roots
    end
  end

  def increment(key)
    store.transaction do
      incremented_value = store[key] + 1
      store[key] = incremented_value
      incremented_value
    end
  end

  def delete(key)
    store.transaction do |store|
      store.delete(key)
    end
  end

  def seed_db(init_hash)
    init_hash.each_pair do |k,v|
      set(k, v)
    end
  end
end
