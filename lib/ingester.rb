require_relative 'data_store'
require_relative 'state_map'
require_relative 'uniq_store'

module Ingester
  class << self
    def upsert_row(row)
      id_to_upsert = StateMap.increment_current_id
      DataStore.create_new_record_from_row(id: id_to_upsert, row: row)

      if id_to_delete = UniqStore.find_from_row(row)
        DataStore.delete(id_to_delete)
      end

      UniqStore.upsert(row: row, new_id: id_to_upsert)
    end
  end
end