require_relative '../config/psv_headers'
require_relative 'index'
require_relative 'data_store'

class QueryError < StandardError; end

class Query
  attr_accessor :selects, :orders, :filters
  def initialize(argv = [])
    raise QueryError, "Wrong number of args received: #{argv.join(' ')}" unless argv.count.even?
    while argv.any?
      flag, val = argv.shift(2)
      case flag
      when "-s"
        set_selects(val)
      when "-o"
        set_orders(val)
      when "-f"
        add_to_filters(val)
      else
        raise QueryError, "Unknown flag: #{flag}"
      end
    end
  end

  def perform
    results = filters.any? ? fetch_with_filters : DataStore.get_all
    return [] unless results.any?
    apply_orders_in_place!(results) if orders.any?
    apply_selects_in_place!(results) if selects.any?
    results
  end

  def set_selects(val)
    @selects = val.split(',')
    @selects.each do |v|
      raise QueryError, "#{v} is not a valid column!" unless PSV_HEADERS.include?(v)
    end
  end

  def set_orders(val)
    @orders = val.split(',')
    @orders.each do |v|
      raise QueryError, "#{v} is not a valid column!" unless PSV_HEADERS.include?(v)
    end
  end

  def add_to_filters(val)
    k, v = val.split('=')
    raise QueryError, "#{k} is not a valid column!" unless PSV_HEADERS.include?(k)
    (@filters ||= {})[k] = v
  end

  def fetch_with_filters(filter_hash = @filters)
    index_results = []
    filter_hash.each_pair do |column_name, index_key|
      index_results << Index.fetch_ids(column_name: column_name, index_key: index_key)
    end
    filtered_ids = index_results.inject(:&)
    if filtered_ids.any?
      data_store_id_map = StateMap.new.map_data_stores_by_ids(filtered_ids)
      DataStore.get_bulk(data_store_id_map)
    else
      []
    end
  end

  def apply_orders_in_place!(results, order_array = @orders)
    results.sort_by! do |result|
      order_array.map do |column_name|
        result[PSV_HEADERS.index(column_name)]
      end
    end
  end

  def apply_selects_in_place!(results, select_array = @selects)
    results.map! do |r|
      select_array.map do |column_name|
        r[PSV_HEADERS.index(column_name)]
      end
    end
  end
end
