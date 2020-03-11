require_relative 'pstore_connection'
require_relative 'state_map'
require_relative 'index'

module DataStore
  class << self
    def connection(path)
      PstoreConnection.new(path)
    end

    def create_new_record_from_row(id:, row:)
      formatted_row = shape_row_for_data_store(row)
      connection(StateMap.data_store_for_new_record(id)).set(id, formatted_row)
      Index.index_row(id: id, row: formatted_row)
    end

    def delete(id)
      delete_connection = connection(StateMap.find_data_store_by_id(id))
      data_to_deindex = delete_connection.get(id)
      Index.deindex_id(data: data_to_deindex, id: id)
      delete_connection.delete(id)
    end

    def get_bulk(filtered_ids)
      data_store_id_map = StateMap.map_data_stores_by_ids(filtered_ids)
      results = []
      # TODO add concurrency here:
      data_store_id_map.each_pair do |path, ids|
        results << connection(path).get_multiple_in_single_transaction(ids)
      end
      results.flatten(1)
    end

    def get_all
      results = []
      # TODO add concurrency here:
      StateMap.all_data_store_paths.each do |path|
        results.concat(connection(path).get_all_in_single_transaction)
      end
      results
    end

    private

    def shape_row_for_data_store(row)
      row.to_h.values
    end
  end
end
