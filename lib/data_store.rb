require_relative 'pstore_connection'
require_relative 'state_map'

# knows about shape of data store rows
module DataStore
  class << self
    def connection(path)
      PstoreConnection.new(path)
    end

    def create_new_record_from_row(id:, row:)
      connection(StateMap.data_store_for_new_record(id)).set(id, shape_row_for_data_store(row))
    end

    def delete(id)
      connection(StateMap.find_data_store_by_id(id)).delete(id)
    end

    private

    def shape_row_for_data_store(row)
      row.to_h.values
    end
  end
end
