require_relative 'pstore_connection'
require_relative 'state_map'
require_relative 'indexer'

# knows about shape of data store rows
module DataStore
  class << self
    def connection(path)
      PstoreConnection.new(path)
    end

    def create_new_record_from_row(id:, row:)
      formatted_row = shape_row_for_data_store(row)
      connection(StateMap.data_store_for_new_record(id)).set(id, formatted_row)
      Indexer.index_row(id: id, row: formatted_row)
    end

    def delete(id)
      delete_connection = connection(StateMap.find_data_store_by_id(id))
      data_to_deindex = delete_connection.get(id)
      Indexer.deindex(data: data_to_deindex, id: id)
      delete_connection.delete(id)
    end

    private

    def shape_row_for_data_store(row)
      row.to_h.values
    end
  end
end
