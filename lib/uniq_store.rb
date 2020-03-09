require_relative 'pstore_connection'
require_relative 'state_map'

# knows about uniq constraint shape
module UniqStore
  UNIQ_FIELDS = ["DATE", "STB", "TITLE"]
  class << self
    def connection(path)
      PstoreConnection.new(path)
    end

    def find_from_row(row)
      compound_key = compound_key_from_row(row)
      connection(StateMap.find_uniq_store_from_compound_key(compound_key))
        .get(compound_key_from_row(row))
    end

    def upsert(row:, new_id:)
      compound_key = compound_key_from_row(row)
      connection(StateMap.find_uniq_store_from_compound_key(compound_key))
        .set(compound_key, new_id)
    end

    private

    def compound_key_from_row(row)
      row.to_h.fetch_values(*UNIQ_FIELDS).join('.')
    end
  end
end
