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
      seed_table(init_hash)
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

  def get_multiple_in_single_transaction(array)
    store.transaction(true) do
      array.map{|key| store[key]}.compact
    end
  end

  def keys
    store.transaction(true) do
      store.roots
    end
  end

  def get_all_in_single_transaction
    store.transaction(true) do
      store.roots.map{|key| store[key]}
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

  def seed_table(init_hash)
    init_hash.each_pair do |k,v|
      set(k, v)
    end
  end

  def shovel(key, value) # upserts and takes single or array
    store.transaction do |store|
      store[key] = [store[key], value].flatten.compact
    end
  end

  def delete_single_value_from_array(key:, value:)
    store.transaction do |store|
      store[key].delete(value)
      store.delete(key) unless store[key].any?
    end
  end
end
