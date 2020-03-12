require_relative 'pstore_connection'
require_relative 'index'

class DataStore
  attr_accessor :connection
  def initialize(path)
    @connection = PstoreConnection.new(path)
  end

  def create_new_record_from_row(id:, row:)
    formatted_row = shape_row_for_data_store(row)
    connection.set(id, formatted_row)
    Index.index_row(id: id, row: formatted_row)
  end

  def delete(id)
    data_to_deindex = connection.get(id)
    Index.deindex_id(data: data_to_deindex, id: id)
    connection.delete(id)
  end

  def self.get_bulk(data_store_id_map)
    results = []
    # TODO add concurrency here:
    data_store_id_map.each_pair do |path, ids|
      results << DataStore.new(path).connection.get_multiple_in_single_transaction(ids)
    end
    results.flatten(1)
  end

  def self.get_all(all_paths)
    results = []
    # TODO add concurrency here:
    all_paths.each do |path|
      results.concat(DataStore.new(path).connection.get_all_in_single_transaction)
    end
    results
  end

  private

  def shape_row_for_data_store(row)
    row.to_h.values
  end
end
